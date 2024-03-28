/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util.vectorio;

import static java.util.Objects.requireNonNull;
import static org.apache.parquet.Exceptions.throwIfInstance;
import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.hadoop.util.vectorio.BindingUtils.loadInvocation;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.util.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vector IO bridge.
 * <p>
 * This loads the {@code PositionedReadable} method:
 * <pre>
 *   void readVectored(List[?extends FileRange] ranges,
 *     IntFunction[ByteBuffer] allocate) throws IOException
 * </pre>
 * It is made slightly easier because of type erasure; the signature of the
 * function is actually {@code Void.class method(List.class, IntFunction.class)}.
 * <p>
 * There are some counters to aid in testing; the {@link #toString()} method
 * will print them and the loaded method, for use in tests and debug logs.
 */
public final class VectorIOBridge {

  private static final Logger LOG = LoggerFactory.getLogger(VectorIOBridge.class);

  /**
   * readVectored Method to look for.
   */
  private static final String READ_VECTORED = "readVectored";

  /**
   * hasCapability Method name.
   * {@code boolean hasCapability(String capability);}
   */
  private static final String HAS_CAPABILITY = "hasCapability";

  /**
   * hasCapability() probe for vectored IO api implementation.
   */
  static final String VECTOREDIO_CAPABILITY = "in:readvectored";

  /**
   * The singleton instance of the bridge.
   */
  private static final VectorIOBridge INSTANCE = new VectorIOBridge();

  /**
   * readVectored() method.
   */
  private final DynMethods.UnboundMethod readVectored;

  /**
   * {@code boolean StreamCapabilities.hasCapability(String)}.
   */
  private final DynMethods.UnboundMethod hasCapabilityMethod;

  /**
   * How many vector read calls made.
   */
  private final AtomicLong vectorReads = new AtomicLong();

  /**
   * How many blocks were read.
   */
  private final AtomicLong blocksRead = new AtomicLong();

  /**
   * How many bytes were read.
   */
  private final AtomicLong bytesRead = new AtomicLong();

  /**
   * Constructor. package private for testing.
   */
  private VectorIOBridge() {

    readVectored =
        loadInvocation(PositionedReadable.class, Void.TYPE, READ_VECTORED, List.class, IntFunction.class);
    LOG.debug("Vector IO availability: {}", available());

    // if readVectored is present, so is hasCapabilities().
    hasCapabilityMethod = loadInvocation(FSDataInputStream.class, boolean.class, HAS_CAPABILITY, String.class);
  }

  /**
   * Is the vectored IO API available for the given stream
   * and allocator in this Hadoop runtime?
   *
   * @param stream input stream to query.
   * @param allocator allocator to be used.
   *
   * @return true if the stream declares the capability is available.
   */
  public boolean readVectoredAvailable(final FSDataInputStream stream, final ByteBufferAllocator allocator) {
    return available() && !allocator.isDirect();
  }

  /**
   * Is the bridge available?
   *
   * @return true if readVectored() is available.
   */
  public boolean available() {
    return !readVectored.isNoop() && FileRangeBridge.bridgeAvailable();
  }

  /**
   * Check that the bridge is available.
   *
   * @throws UnsupportedOperationException if it is not.
   */
  private void checkAvailable() {
    if (!available()) {
      throw new UnsupportedOperationException("Hadoop VectorIO not found");
    }
  }

  /**
   * Read fully a list of file ranges asynchronously from this file.
   * The default iterates through the ranges to read each synchronously, but
   * the intent is that FSDataInputStream subclasses can make more efficient
   * readers.
   * The {@link ParquetFileRange} parameters all have their
   * data read futures set to the range reads of the associated
   * operations; callers must await these to complete.
   * <p>
   * As a result of the call, each range will have FileRange.setData(CompletableFuture)
   * called with a future that when complete will have a ByteBuffer with the
   * data from the file's range.
   * <p>
   *   The position returned by getPos() after readVectored() is undefined.
   * </p>
   * <p>
   *   If a file is changed while the readVectored() operation is in progress, the output is
   *   undefined. Some ranges may have old data, some may have new and some may have both.
   * </p>
   * <p>
   *   While a readVectored() operation is in progress, normal read api calls may block.
   * </p>
   * @param stream stream from where the data has to be read.
   * @param ranges parquet file ranges.
   * @param allocator buffer allocator.
   * @throws UnsupportedOperationException if the API is not available.
   * @throws EOFException if a range is past the end of the file.
   * @throws IOException other IO problem initiating the read operations.
   */
  public static void readVectoredRanges(
      final FSDataInputStream stream, final List<ParquetFileRange> ranges, final ByteBufferAllocator allocator)
      throws IOException {

    final VectorIOBridge bridge = availableInstance();
    if (!bridge.readVectoredAvailable(stream, allocator)) {
      throw new UnsupportedOperationException("Vectored IO not available on stream " + stream);
    }

    // Sort the ranges by offset and then validate for overlaps.
    // This ensures consistent behavior with all filesystems
    // across all implementations of Hadoop (specifically those without HADOOP-19098)
    final List<ParquetFileRange> sorted = validateAndSortRanges(ranges);

    final FileRangeBridge rangeBridge = FileRangeBridge.instance();
    // Setting the parquet range as a reference.
    List<FileRangeBridge.WrappedFileRange> fileRanges =
        sorted.stream().map(rangeBridge::toFileRange).collect(Collectors.toList());
    bridge.readWrappedRanges(stream, fileRanges, allocator::allocate);

    // copy back the completable futures from the scheduled
    // vector reads to the ParquetFileRange entries passed in.
    fileRanges.forEach(fileRange -> {
      // toFileRange() sets up this back reference
      ParquetFileRange parquetFileRange = (ParquetFileRange) fileRange.getReference();
      parquetFileRange.setDataReadFuture(fileRange.getData());
    });
  }

  /**
   * Read data in a list of wrapped file ranges, extracting the inner
   * instances and then executing.
   * Returns when the data reads are active; possibly executed
   * in a blocking sequence of reads, possibly scheduled on different
   * threads.
   *
   * @param stream stream from where the data has to be read.
   * @param ranges wrapped file ranges.
   * @param allocate allocate function to allocate memory to hold data.
   * @throws EOFException if a range is past the end of the file.
   * @throws IOException other IO problem initiating the read operations.
   */
  private void readWrappedRanges(
      final PositionedReadable stream,
      final List<FileRangeBridge.WrappedFileRange> ranges,
      final IntFunction<ByteBuffer> allocate)
      throws IOException {

    // update the counters.
    vectorReads.incrementAndGet();
    blocksRead.addAndGet(ranges.size());
    // extract the instances the wrapped ranges refer to; update the
    // bytes read counter.
    List<Object> instances = ranges.stream()
        .map(r -> {
          bytesRead.addAndGet(r.getLength());
          return r.getFileRange();
        })
        .collect(Collectors.toList());
    LOG.debug("readVectored with {} ranges on stream {}", ranges.size(), stream);
    try {
      readVectored.invokeChecked(stream, instances, allocate);
    } catch (Exception e) {
      throwIfInstance(e, IOException.class);
      throwIfInstance(e, RuntimeException.class);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "VectorIOBridge{"
        + "available=" + available()
        + ", readVectored=" + readVectored
        + ", vectorReads=" + vectorReads.get()
        + ", blocksRead=" + blocksRead.get()
        + ", bytesRead=" + bytesRead.get()
        + '}';
  }

  /**
   * Does a stream implement StreamCapability and, if so, is the capability
   * available.
   * The call will return false if
   * <ol>
   *   <li>The method is not found</li>
   *   <li>The method is found but throws an exception when invoked</li>
   *   <li>The method is found, invoked and returns false</li>
   * </ol>
   * Put differently: it will only return true if the method
   * probe returned true.
   * @param stream input stream to query.
   * @param capability the capability to look for.
   * @return true if the stream declares the capability is available.
   */
  public boolean hasCapability(final FSDataInputStream stream, final String capability) {

    if (hasCapabilityMethod.isNoop()) {
      return false;
    } else {
      try {
        return hasCapabilityMethod.invoke(stream, capability);
      } catch (RuntimeException e) {
        return false;
      }
    }
  }

  /**
   * How many vector read calls have been made?
   * @return the count of vector reads.
   */
  public long getVectorReads() {
    return vectorReads.get();
  }

  /**
   * How many blocks were read?
   * @return the count of blocks read.
   */
  public long getBlocksRead() {
    return blocksRead.get();
  }

  /**
   * How many bytes of data have been read.
   * @return the count of bytes read.
   */
  public long getBytesRead() {
    return bytesRead.get();
  }

  /**
   * Reset all counters; for testing.
   * Non-atomic.
   */
  void resetCounters() {
    vectorReads.set(0);
    blocksRead.set(0);
    bytesRead.set(0);
  }
  /**
   * Sort the input ranges by offset; no validation is done.
   * @param input input ranges.
   * @return a new list of the ranges, sorted by offset.
   */
  private static List<ParquetFileRange> sortRanges(List<ParquetFileRange> input) {
    final List<ParquetFileRange> l = new ArrayList<>(input);
    l.sort(Comparator.comparingLong(ParquetFileRange::getOffset));
    return l;
  }
  /**
   * Validate a single range.
   * @param range range to validate.
   * @return the range.
   * @throws IllegalArgumentException the range length is negative or other invalid condition
   * is met other than the those which raise EOFException or NullPointerException.
   * @throws EOFException the range offset is negative
   * @throws NullPointerException if the range is null.
   */
  private static ParquetFileRange validateRangeRequest(ParquetFileRange range) throws EOFException {

    requireNonNull(range, "range is null");

    checkArgument(range.getLength() >= 0, "length is negative in %s", range);
    if (range.getOffset() < 0) {
      throw new EOFException("position is negative in range " + range);
    }
    return range;
  }

  /**
   * Validate a list of ranges (including overlapping checks) and
   * return the sorted list.
   * <p>
   * Two ranges overlap when the start offset
   * of second is less than the end offset of first.
   * End offset is calculated as start offset + length.
   * @param input input list
   * @return a new sorted list.
   * @throws IllegalArgumentException if there are overlapping ranges or
   * a range element is invalid
   */
  private static List<ParquetFileRange> validateAndSortRanges(final List<ParquetFileRange> input)
      throws EOFException {

    requireNonNull(input, "Null input list");
    checkArgument(!input.isEmpty(), "Empty input list");
    final List<ParquetFileRange> sortedRanges;

    if (input.size() == 1) {
      validateRangeRequest(input.get(0));
      sortedRanges = input;
    } else {
      sortedRanges = sortRanges(input);
      ParquetFileRange prev = null;
      for (final ParquetFileRange current : sortedRanges) {
        validateRangeRequest(current);
        if (prev != null) {
          checkArgument(
              current.getOffset() >= prev.getOffset() + prev.getLength(),
              "Overlapping ranges %s and %s",
              prev,
              current);
        }
        prev = current;
      }
    }

    return sortedRanges;
  }

  /**
   * Get the singleton instance.
   *
   * @return instance.
   */
  public static VectorIOBridge instance() {
    return INSTANCE;
  }

  /**
   * Is the bridge available for use?
   *
   * @return true if the vector IO API is available.
   */
  public static boolean bridgeAvailable() {
    return instance().available();
  }

  /**
   * Get the instance <i>verifying that the API is available</i>.
   * @return an available instance, always
   * @throws UnsupportedOperationException if it is not.
   */
  public static VectorIOBridge availableInstance() {
    final VectorIOBridge bridge = instance();
    bridge.checkAvailable();
    return bridge;
  }
}
