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

package org.apache.parquet.bytes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.parquet.io.SeekableInputStream;

public class LazyByteBufferInputStream extends InputStream {
  private final SeekableInputStream underlying;
  private final int bufferSize;
  private final long totalReadableBytes;

  private long streamOffset;
  private int bytesReadInBuffer;
  private int totalBytesRead;
  private ByteBuffer delegate;
  private ByteBufferInputStream buffer;

  public static LazyByteBufferInputStream wrap(
      SeekableInputStream underlying,
      ByteBufferAllocator allocator,
      long startingOffset,
      long readableBytes,
      int bufferSize)
      throws IOException {
    return new LazyByteBufferInputStream(underlying, allocator, startingOffset, readableBytes, bufferSize);
  }

  LazyByteBufferInputStream(
      SeekableInputStream underlying,
      ByteBufferAllocator allocator,
      long startingOffset,
      long readableBytes,
      int bufferSize) {
    this.underlying = underlying;
    this.totalReadableBytes = readableBytes;
    this.bytesReadInBuffer = 0;
    this.bufferSize = (int) Math.min(readableBytes, bufferSize); // @Todo clean up casting...
    this.streamOffset = startingOffset;
    this.delegate = allocator.allocate(this.bufferSize);
    try {
      underlying.seek(startingOffset);
      underlying.read(delegate);
      delegate.flip();
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize LazyByteBufferInputStream", e);
    }
    this.buffer = ByteBufferInputStream.wrap(delegate);
  }

  public ByteBuffer getDelegate() {
    return delegate;
  }

  public void refillBuffers() {
    try {
      final byte[] unreadBytes =
          Arrays.copyOfRange(delegate.array(), delegate.capacity() - buffer.available(), delegate.capacity());
      this.streamOffset += bytesReadInBuffer;
      delegate.put(unreadBytes);
      underlying.seek(streamOffset + unreadBytes.length);
      underlying.read(delegate); // Todo check that we're not reading past total chunk size
      delegate.flip();
    } catch (IOException e) {
      throw new RuntimeException(e); // @Todo better exception handling
    }

    this.bytesReadInBuffer = 0;
    this.buffer = ByteBufferInputStream.wrap(delegate);
  }

  private void checkRefill() {
    checkRefill(1);
  }

  private void checkRefill(long bytesNeeded) {
    if (buffer.available() < bytesNeeded) {
      refillBuffers();
    }
  }

  public List<ByteBuffer> sliceBuffers(int len) throws IOException {
    if (len == 0) {
      return Collections.emptyList();
    }

    checkRefill(len);

    final ByteBuffer slice =
        ByteBuffer.wrap(Arrays.copyOfRange(delegate.array(), bytesReadInBuffer, bytesReadInBuffer + len));
    slice.position(0);

    buffer.skip(len);
    bytesReadInBuffer += len;
    totalBytesRead += len;
    return Collections.singletonList(slice);
  }

  public long position() {
    return totalBytesRead;
  }

  private int wrapRead(int bytesRead) {
    this.totalBytesRead += bytesRead;
    this.bytesReadInBuffer += bytesRead;
    return bytesRead;
  }

  @Override
  public int read() throws IOException {
    checkRefill();
    wrapRead(1);
    return buffer.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    checkRefill(b.length);
    return wrapRead(buffer.read(b));
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkRefill(len);
    return wrapRead(buffer.read(b, off, len));
  }

  @Override
  public byte[] readAllBytes() throws IOException {
    throw new UnsupportedOperationException("readAllBytes");
  }

  @Override
  public byte[] readNBytes(int len) throws IOException {
    throw new UnsupportedOperationException("readNBytes");
  }

  @Override
  public int readNBytes(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException("readNBytes");
  }

  @Override
  public long skip(long n) {
    checkRefill(n);
    totalBytesRead += n;
    bytesReadInBuffer += n;
    return buffer.skip(n);
  }

  @Override
  public int available() {
    return (int) (totalReadableBytes - totalBytesRead);
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    buffer.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    buffer.reset();
  }

  @Override
  public boolean markSupported() {
    return buffer.markSupported();
  }
}
