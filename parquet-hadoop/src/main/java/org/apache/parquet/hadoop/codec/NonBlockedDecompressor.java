/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.Preconditions;

public abstract class NonBlockedDecompressor implements Decompressor {

  private static final int INITIAL_INPUT_BUFFER_SIZE = 4096;

  // Buffer for uncompressed output. This buffer grows as necessary.
  private ByteBuffer outputBuffer = ByteBuffer.allocateDirect(0);

  // Buffer for compressed input. This buffer grows as necessary.
  private ByteBuffer inputBuffer = ByteBuffer.allocateDirect(0);

  private boolean finished;

  /**
   * Fills specified buffer with uncompressed data. Returns actual number
   * of bytes of uncompressed data. A return value of 0 indicates that
   * {@link #needsInput()} should be called in order to determine if more
   * input data is required.
   *
   * @param buffer Buffer for the compressed data
   * @param off    Start offset of the data
   * @param len    Size of the buffer
   * @return The actual number of bytes of uncompressed data.
   * @throws IOException if reading or decompression fails
   */
  @Override
  public synchronized int decompress(byte[] buffer, int off, int len) throws IOException {
    // SnappyUtil was dedicated to SnappyCodec in the past. Now it is used for both
    // NonBlockedDecompressor and NonBlockedCompressor without renaming due to its
    // dependency by some external downstream projects.
    SnappyUtil.validateBuffer(buffer, off, len);
    if (inputBuffer.position() == 0 && !outputBuffer.hasRemaining()) {
      return 0;
    }

    if (!outputBuffer.hasRemaining()) {
      inputBuffer.rewind();
      Preconditions.checkArgument(inputBuffer.position() == 0, "Invalid position of 0.");
      Preconditions.checkArgument(outputBuffer.position() == 0, "Invalid position of 0.");
      // There is compressed input, decompress it now.
      int decompressedSize = maxUncompressedLength(inputBuffer, len);
      if (decompressedSize > outputBuffer.capacity()) {
        ByteBuffer oldBuffer = outputBuffer;
        outputBuffer = ByteBuffer.allocateDirect(decompressedSize);
        CleanUtil.cleanDirectBuffer(oldBuffer);
      }

      // Reset the previous outputBuffer (i.e. set position to 0)
      outputBuffer.clear();
      int size = uncompress(inputBuffer, outputBuffer);
      outputBuffer.limit(size);
      // We've decompressed the entire input, reset the input now
      inputBuffer.clear();
      inputBuffer.limit(0);
      finished = true;
    }

    // Return compressed output up to 'len'
    int numBytes = Math.min(len, outputBuffer.remaining());
    outputBuffer.get(buffer, off, numBytes);
    return numBytes;
  }

  /**
   * Sets input data for decompression.
   * This should be called if and only if {@link #needsInput()} returns
   * <code>true</code> indicating that more input data is required.
   * (Both native and non-native versions of various Decompressors require
   * that the data passed in via <code>b[]</code> remain unmodified until
   * the caller is explicitly notified--via {@link #needsInput()}--that the
   * buffer may be safely modified.  With this requirement, an extra
   * buffer-copy can be avoided.)
   *
   * @param buffer Input data
   * @param off    Start offset
   * @param len    Length
   */
  @Override
  public synchronized void setInput(byte[] buffer, int off, int len) {
    // SnappyUtil was dedicated to SnappyCodec in the past. Now it is used for both
    // NonBlockedDecompressor and NonBlockedCompressor without renaming due to its
    // dependency by some external downstream projects.
    SnappyUtil.validateBuffer(buffer, off, len);

    if (inputBuffer.capacity() - inputBuffer.position() < len) {
      final int newBufferSize;
      if (inputBuffer.capacity() == 0) {
        newBufferSize = Math.max(INITIAL_INPUT_BUFFER_SIZE, len);
      } else {
        newBufferSize = Math.max(inputBuffer.position() + len, inputBuffer.capacity() * 2);
      }
      final ByteBuffer newBuffer = ByteBuffer.allocateDirect(newBufferSize);
      newBuffer.limit(inputBuffer.position() + len);
      inputBuffer.rewind();
      newBuffer.put(inputBuffer);
      final ByteBuffer oldBuffer = inputBuffer;
      inputBuffer = newBuffer;
      CleanUtil.cleanDirectBuffer(oldBuffer);
    } else {
      inputBuffer.limit(inputBuffer.position() + len);
    }
    inputBuffer.put(buffer, off, len);
  }

  @Override
  public void end() {
    CleanUtil.cleanDirectBuffer(inputBuffer);
    CleanUtil.cleanDirectBuffer(outputBuffer);
  }

  @Override
  public synchronized boolean finished() {
    return finished && !outputBuffer.hasRemaining();
  }

  @Override
  public int getRemaining() {
    return 0;
  }

  @Override
  public synchronized boolean needsInput() {
    return !inputBuffer.hasRemaining() && !outputBuffer.hasRemaining();
  }

  @Override
  public synchronized void reset() {
    finished = false;
    inputBuffer.rewind();
    outputBuffer.rewind();
    inputBuffer.limit(0);
    outputBuffer.limit(0);
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // No-op
  }

  /**
   * Get the maximum uncompressed byte size of the given compressed input. This operation takes O(1) time.
   *
   * @param compressed            input data [pos() ... limit())
   * @param maxUncompressedLength maximum length of the uncompressed data
   * @return uncompressed byte length of the given input
   */
  protected abstract int maxUncompressedLength(ByteBuffer compressed, int maxUncompressedLength) throws IOException;

  /**
   * Uncompress the content in the input buffer. The result is dumped to the
   * specified output buffer.
   *
   * @param compressed   buffer[pos() ... limit()) containing the input data
   * @param uncompressed output of the the uncompressed data. It uses buffer[pos()..]
   * @return uncompressed data size
   */
  protected abstract int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) throws IOException;
}
