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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.parquet.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is a wrapper around the underlying compressor. It always consumes
 * the entire input in setInput and compresses it as one compressed block.
 */
abstract public class NonBlockedCompressor implements Compressor {

  // Buffer for compressed output. This buffer grows as necessary.
  private ByteBuffer outputBuffer = ByteBuffer.allocateDirect(0);

  // Buffer for uncompressed input. This buffer grows as necessary.
  private ByteBuffer inputBuffer = ByteBuffer.allocateDirect(0);

  private long bytesRead = 0L;
  private long bytesWritten = 0L;
  private boolean finishCalled = false;

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * @param buffer Buffer for the compressed data
   * @param off    Start offset of the data
   * @param len    Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public synchronized int compress(byte[] buffer, int off, int len) throws IOException {
    // SnappyUtil was dedicated to SnappyCodec in the past. Now it is used for both
    // NonBlockedDecompressor and NonBlockedCompressor without renaming due to its
    // dependency by some external downstream projects.
    SnappyUtil.validateBuffer(buffer, off, len);

    if (needsInput()) {
      // No buffered output bytes and no input to consume, need more input
      return 0;
    }

    if (!outputBuffer.hasRemaining()) {
      // There is uncompressed input, compress it now
      int maxOutputSize = maxCompressedLength(inputBuffer.position());
      if (maxOutputSize > outputBuffer.capacity()) {
        ByteBuffer oldBuffer = outputBuffer;
        outputBuffer = ByteBuffer.allocateDirect(maxOutputSize);
        CleanUtil.cleanDirectBuffer(oldBuffer);
      }
      // Reset the previous outputBuffer
      outputBuffer.clear();
      inputBuffer.limit(inputBuffer.position());
      inputBuffer.position(0);

      int size = compress(inputBuffer, outputBuffer);
      outputBuffer.limit(size);
      inputBuffer.limit(0);
      inputBuffer.rewind();
    }

    // Return compressed output up to 'len'
    int numBytes = Math.min(len, outputBuffer.remaining());
    outputBuffer.get(buffer, off, numBytes);
    bytesWritten += numBytes;
    return numBytes;
  }

  @Override
  public synchronized void setInput(byte[] buffer, int off, int len) {
    // SnappyUtil was dedicated to SnappyCodec in the past. Now it is used for both
    // NonBlockedDecompressor and NonBlockedCompressor without renaming due to its
    // dependency by some external downstream projects.
    SnappyUtil.validateBuffer(buffer, off, len);

    Preconditions.checkArgument(!outputBuffer.hasRemaining(),
      "Output buffer should be empty. Caller must call compress()");

    if (inputBuffer.capacity() - inputBuffer.position() < len) {
      ByteBuffer tmp = ByteBuffer.allocateDirect(inputBuffer.position() + len);
      inputBuffer.rewind();
      tmp.put(inputBuffer);
      ByteBuffer oldBuffer = inputBuffer;
      inputBuffer = tmp;
      CleanUtil.cleanDirectBuffer(oldBuffer);
    } else {
      inputBuffer.limit(inputBuffer.position() + len);
    }

    // Append the current bytes to the input buffer
    inputBuffer.put(buffer, off, len);
    bytesRead += len;
  }

  @Override
  public void end() {
    CleanUtil.cleanDirectBuffer(inputBuffer);
    CleanUtil.cleanDirectBuffer(outputBuffer);
  }

  @Override
  public synchronized void finish() {
    finishCalled = true;
  }

  @Override
  public synchronized boolean finished() {
    return finishCalled && inputBuffer.position() == 0 && !outputBuffer.hasRemaining();
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  // We want to compress all the input in one go so we always need input until it is
  // all consumed.
  public synchronized boolean needsInput() {
    return !finishCalled;
  }

  @Override
  public void reinit(Configuration c) {
    reset();
  }

  @Override
  public synchronized void reset() {
    finishCalled = false;
    bytesRead = bytesWritten = 0;
    inputBuffer.rewind();
    outputBuffer.rewind();
    inputBuffer.limit(0);
    outputBuffer.limit(0);
  }

  @Override
  public void setDictionary(byte[] dictionary, int off, int len) {
    // No-op		
  }

  /**
   * Get the maximum byte size needed for compressing data of the given byte
   * size.
   *
   * @param byteSize byte size of the data to compress
   * @return maximum byte size of the compressed data
   */
  abstract protected int maxCompressedLength(int byteSize);

  /**
   * Compress the content in the given input buffer. After the compression,
   * you can retrieve the compressed data from the output buffer [pos() ...
   * limit()) (compressed data size = limit() - pos() = remaining())
   *
   * @param uncompressed buffer[pos() ... limit()) containing the input data
   * @param compressed   output of the compressed data. Uses range [pos()..].
   * @return byte size of the compressed data.
   */
  abstract protected int compress(ByteBuffer uncompressed, ByteBuffer compressed) throws IOException;

}
