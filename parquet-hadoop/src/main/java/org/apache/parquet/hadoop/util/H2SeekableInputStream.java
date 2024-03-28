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

package org.apache.parquet.hadoop.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.hadoop.util.vectorio.VectorIOBridge;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.ParquetFileRange;

/**
 * SeekableInputStream implementation for FSDataInputStream that implements
 * ByteBufferReadable in Hadoop 2.
 * It implements {@link #readVectored(List, ByteBufferAllocator)}) by
 * handing off to VectorIOBridge which uses reflection to offer the API if it is found.
 * The return value of {@link #readVectoredAvailable(ByteBufferAllocator)}
 * reflects the availability of the API.
 */
class H2SeekableInputStream extends DelegatingSeekableInputStream {

  // Visible for testing
  interface Reader {
    int read(ByteBuffer buf) throws IOException;
  }

  private final FSDataInputStream stream;
  private final Reader reader;

  public H2SeekableInputStream(FSDataInputStream stream) {
    super(stream);
    this.stream = stream;
    this.reader = new H2Reader();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public long getPos() throws IOException {
    return stream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    stream.seek(newPos);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    stream.readFully(bytes, start, len);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return stream.read(buf);
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    readFully(reader, buf);
  }

  private class H2Reader implements Reader {
    @Override
    public int read(ByteBuffer buf) throws IOException {
      return stream.read(buf);
    }
  }

  @Override
  public boolean readVectoredAvailable(final ByteBufferAllocator allocator) {
    return VectorIOBridge.instance().readVectoredAvailable(stream, allocator);
  }

  @Override
  public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
    VectorIOBridge.readVectoredRanges(stream, ranges, allocator);
  }

  public static void readFully(Reader reader, ByteBuffer buf) throws IOException {
    // unfortunately the Hadoop 2 APIs do not have a 'readFully' equivalent for the byteBuffer read
    // calls. The read(ByteBuffer) call might read fewer than byteBuffer.hasRemaining() bytes. Thus we
    // have to loop to ensure we read them.
    while (buf.hasRemaining()) {
      int readCount = reader.read(buf);
      if (readCount == -1) {
        // this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer
        // that has more remaining than the amount of data in the stream.
        throw new EOFException("Reached the end of stream. Still have: " + buf.remaining() + " bytes left");
      }
    }
  }
}
