/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.parquet.bytes.ByteBufferAllocator;

/**
 * {@code SeekableInputStream} is an interface with the methods needed by
 * Parquet to read data from a file or Hadoop data stream.
 */
public abstract class SeekableInputStream extends InputStream {

  /**
   * Return the current position in the InputStream.
   *
   * @return current position in bytes from the start of the stream
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract long getPos() throws IOException;

  /**
   * Seek to a new position in the InputStream.
   *
   * @param newPos the new position to seek to
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract void seek(long newPos) throws IOException;

  /**
   * Read a byte array of data, from position 0 to the end of the array.
   * <p>
   * This method is equivalent to {@code read(bytes, 0, bytes.length)}.
   * <p>
   * This method will block until len bytes are available to copy into the
   * array, or will throw {@link EOFException} if the stream ends before the
   * array is full.
   *
   * @param bytes a byte array to fill with data from the stream
   * @throws IOException  If the underlying stream throws IOException
   * @throws EOFException If the stream has fewer bytes left than are needed to
   *                      fill the array, {@code bytes.length}
   */
  public abstract void readFully(byte[] bytes) throws IOException;

  /**
   * Read {@code len} bytes of data into an array, at position {@code start}.
   * <p>
   * This method will block until len bytes are available to copy into the
   * array, or will throw {@link EOFException} if the stream ends before the
   * array is full.
   *
   * @param bytes a byte array to fill with data from the stream
   * @param start the starting position in the byte array for data
   * @param len   the length of bytes to read into the byte array
   * @throws IOException  If the underlying stream throws IOException
   * @throws EOFException If the stream has fewer than {@code len} bytes left
   */
  public abstract void readFully(byte[] bytes, int start, int len) throws IOException;

  /**
   * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
   * <p>
   * This method will copy available bytes into the buffer, reading at most
   * {@code buf.remaining()} bytes. The number of bytes actually copied is
   * returned by the method, or -1 is returned to signal that the end of the
   * underlying stream has been reached.
   *
   * @param buf a byte buffer to fill with data from the stream
   * @return the number of bytes read or -1 if the stream ended
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract int read(ByteBuffer buf) throws IOException;

  /**
   * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
   * <p>
   * This method will block until {@code buf.remaining()} bytes are available
   * to copy into the buffer, or will throw {@link EOFException} if the stream
   * ends before the buffer is full.
   *
   * @param buf a byte buffer to fill with data from the stream
   * @throws IOException  If the underlying stream throws IOException
   * @throws EOFException If the stream has fewer bytes left than are needed to
   *                      fill the buffer, {@code buf.remaining()}
   */
  public abstract void readFully(ByteBuffer buf) throws IOException;

  /**
   * Read a set of file ranges in a vectored manner.
   *
   * @param ranges the list of file ranges to read
   * @param allocator the allocator to use for allocating ByteBuffers
   * @throws UnsupportedOperationException if not available in this class/runtime (default)
   */
  public void readVectored(List<ParquetFileRange> ranges, final ByteBufferAllocator allocator) throws IOException {

    throw new UnsupportedOperationException("Vectored IO is not supported for " + this);
  }

  /**
   * Is the {@link #readVectored(List, ByteBufferAllocator)} method available?
   * @param allocator the allocator to use for allocating ByteBuffers
   * @return True if the operation is considered available for this allocator in the hadoop runtime.
   */
  public boolean readVectoredAvailable(final ByteBufferAllocator allocator) {
    return false;
  }
}
