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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * SeekableInputStream implementation that implements read(ByteBuffer) for
 * Hadoop 1 FSDataInputStream.
 */
class H1SeekableInputStream extends DelegatingSeekableInputStream {

  private final FSDataInputStream stream;

  public H1SeekableInputStream(FSDataInputStream stream) {
    super(stream);
    this.stream = stream;
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
  public void readFully(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      stream.readFully(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
    } else {
      super.readFully(buf);
    }
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    stream.readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    stream.readFully(bytes, start, len);
  }

}
