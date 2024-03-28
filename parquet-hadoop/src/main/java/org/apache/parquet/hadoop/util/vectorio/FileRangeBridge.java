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
import static org.apache.parquet.hadoop.util.vectorio.BindingUtils.implemented;
import static org.apache.parquet.hadoop.util.vectorio.BindingUtils.loadInvocation;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.util.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to bridge to the FileRange class through reflection.
 * A singleton is created when the class is loaded, so there is no need
 * to repeat the reflection process on every API call.
 * <p>
 * The Hadoop FileRange class is an interface with getters and setters;
 * to instantiate the static method {@code createFileRange()} is used.
 */
final class FileRangeBridge {

  private static final Logger LOG = LoggerFactory.getLogger(FileRangeBridge.class);

  /** Class to bridge to: {@value}. */
  public static final String CLASSNAME = "org.apache.hadoop.fs.FileRange";

  /**
   * The singleton instance of the bridge.
   */
  private static final FileRangeBridge INSTANCE = new FileRangeBridge();

  /**
   * Is the bridge available?
   */
  private final boolean available;

  /**
   * The Hadoop FileRange interface class.
   */
  private final Class<?> fileRangeInterface;

  private final DynMethods.UnboundMethod _getData;
  private final DynMethods.UnboundMethod _setData;
  private final DynMethods.UnboundMethod _getLength;
  private final DynMethods.UnboundMethod _getOffset;
  private final DynMethods.UnboundMethod _getReference;
  private final DynMethods.UnboundMethod createFileRange;

  /**
   * Constructor.
   * This loads the real FileRange and its methods, if present.
   */
  FileRangeBridge() {

    // try to load the class
    Class<?> loadedClass;
    try {
      loadedClass = this.getClass().getClassLoader().loadClass(CLASSNAME);
    } catch (ReflectiveOperationException e) {
      // failure.
      LOG.debug("No {}", CLASSNAME, e);

      // set the loaded class to null so that the loadInvocation calls generate
      // no-op invocations.
      loadedClass = null;
    }
    fileRangeInterface = loadedClass;

    // as loadInvocation returns a no-op if the class is null, this sequence
    // will either construct in the list of operations or leave them with no-ops

    _getOffset = loadInvocation(loadedClass, long.class, "getOffset");
    _getLength = loadInvocation(loadedClass, int.class, "getLength");
    _getData = loadInvocation(loadedClass, null, "getData");
    _setData = loadInvocation(loadedClass, void.class, "setData", CompletableFuture.class);
    _getReference = loadInvocation(loadedClass, Object.class, "getReference");
    // static interface method to create an instance.
    createFileRange = loadInvocation(
        fileRangeInterface, Object.class, "createFileRange", long.class, int.class, Object.class);

    // the bridge is available only if the class is present and all methods are found
    // the checks for the method are extra paranoia, but harmless
    available = loadedClass != null
        && implemented(createFileRange, _getOffset, _getLength, _getData, _setData, _getReference);

    LOG.debug("FileRangeBridge availability: {}", available);
  }

  /**
   * Is the bridge available?
   *
   * @return true iff the bridge is present.
   */
  public boolean available() {
    return available;
  }

  /**
   * Check that the bridge is available.
   *
   * @throws UnsupportedOperationException if it is not.
   */
  private void checkAvailable() {
    if (!available()) {
      throw new UnsupportedOperationException("Interface " + CLASSNAME + " not found");
    }
  }

  /**
   * Get the file range class.
   *
   * @return the file range implementation class, if present.
   */
  public Class<?> getFileRangeInterface() {
    return fileRangeInterface;
  }

  /**
   * Instantiate.
   *
   * @param offset offset in file
   * @param length length of data to read.
   * @param reference nullable reference to store in the range.
   *
   * @return a VectorFileRange wrapping a FileRange
   *
   * @throws RuntimeException if the range cannot be instantiated
   * @throws IllegalStateException if the API is not available.
   */
  public WrappedFileRange createFileRange(final long offset, final int length, final Object reference) {

    checkAvailable();
    return new WrappedFileRange(createFileRange.invoke(null, offset, length, reference));
  }

  /**
   * Build a WrappedFileRange from a ParquetFileRange;
   * the reference field of the wrapped object refers
   * back to the object passed in.
   *
   * @param in input.
   *
   * @return a wrapper around a FileRange instance
   */
  public FileRangeBridge.WrappedFileRange toFileRange(final ParquetFileRange in) {
    // create a new wrapped file range, fill in and then
    // get the instance
    return createFileRange(in.getOffset(), in.getLength(), in);
  }

  @Override
  public String toString() {
    return "FileRangeBridge{"
        + "available=" + available
        + ", fileRangeInterface=" + fileRangeInterface
        + ", _getOffset=" + _getOffset
        + ", _getLength=" + _getLength
        + ", _getData=" + _getData
        + ", _setData=" + _setData
        + ", _getReference=" + _getReference
        + ", createFileRange=" + createFileRange
        + '}';
  }

  /**
   * Get the singleton instance.
   *
   * @return instance.
   */
  public static FileRangeBridge instance() {
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
   * Wraps a Vector {@code FileRange} instance through reflection.
   */
  class WrappedFileRange {

    /**
     * The wrapped {@code FileRange} instance.
     */
    private final Object fileRange;

    /**
     * Instantiate.
     *
     * @param fileRange non null {@code FileRange} instance.
     */
    WrappedFileRange(final Object fileRange) {
      this.fileRange = requireNonNull(fileRange);
    }

    /**
     * Get the range offset.
     *
     * @return the offset.
     */
    public long getOffset() {
      return _getOffset.invoke(fileRange);
    }

    /**
     * Get the range length.
     *
     * @return the length.
     */
    public int getLength() {
      return _getLength.invoke(fileRange);
    }

    /**
     * Get the future of the data; only valid
     * after a read has commenced.
     * @return the future to wait on.
     */
    public CompletableFuture<ByteBuffer> getData() {
      return _getData.invoke(fileRange);
    }

    /**
     * Set the data field.
     *
     * @param data future to set.
     */
    public void setData(final CompletableFuture<ByteBuffer> data) {
      _setData.invoke(fileRange, data);
    }

    /**
     * Get the reference field.
     *
     * @return the reference.
     */
    public Object getReference() {
      return _getReference.invoke(fileRange);
    }

    /**
     * Get the wrapped fileRange.
     *
     * @return the fileRange.
     */
    public Object getFileRange() {
      return fileRange;
    }

    @Override
    public String toString() {
      return "WrappedFileRange{fileRange=" + fileRange + '}';
    }
  }
}
