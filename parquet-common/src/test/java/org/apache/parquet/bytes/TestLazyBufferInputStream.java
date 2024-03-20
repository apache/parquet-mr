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

package org.apache.parquet.bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.Assert;
import org.junit.Test;

public class TestLazyBufferInputStream {
  static final ByteBuffer DATA = ByteBuffer.wrap(new byte[] {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30, 31, 32, 33, 34
  });

  protected LazyByteBufferInputStream newStream() {
    ByteBufferInputStream stream = ByteBufferInputStream.wrap(DATA);
    SeekableInputStream inputStream = new DelegatingSeekableInputStream(stream) {
      @Override
      public long getPos() throws IOException {
        return stream.position();
      }

      @Override
      public void seek(long newPos) throws IOException {
        stream.skip(newPos - stream.position());
      }
    };

    return new LazyByteBufferInputStream(inputStream, new HeapByteBufferAllocator(), 2, 20, 6);
  }

  @Test
  public void testReadBytes() throws Exception {
    LazyByteBufferInputStream stream = newStream();
    Assert.assertEquals(20, stream.available());

    Assert.assertEquals(stream.read(), DATA.array()[2]);
    Assert.assertEquals(stream.read(), DATA.array()[3]);
    Assert.assertEquals(stream.read(), DATA.array()[4]);
    Assert.assertEquals(stream.read(), DATA.array()[5]);
    Assert.assertEquals(stream.read(), DATA.array()[6]);

    Assert.assertEquals(15, stream.available());

    Assert.assertEquals(stream.read(), DATA.array()[7]);
    Assert.assertEquals(stream.read(), DATA.array()[8]);
    Assert.assertEquals(stream.read(), DATA.array()[9]);
    Assert.assertEquals(stream.read(), DATA.array()[10]);
    Assert.assertEquals(stream.read(), DATA.array()[11]);

    Assert.assertEquals(10, stream.available());
    stream.close();
  }

  @Test
  public void testReadByteBuffers() throws Exception {
    LazyByteBufferInputStream stream = newStream();
    byte[] buf1 = new byte[4];
    Assert.assertEquals(stream.read(buf1), 4);
    System.out.println(
        "actual bytes = " + Arrays.toString(buf1) + ", expected =" + Arrays.toString(new byte[] {2, 3, 4, 5}));
    Assert.assertArrayEquals(new byte[] {2, 3, 4, 5}, buf1);
    Assert.assertEquals(stream.available(), 16);
    Assert.assertEquals(stream.position(), 4);

    byte[] buf2 = new byte[4];
    Assert.assertEquals(stream.read(buf2), 4);
    Assert.assertArrayEquals(new byte[] {6, 7, 8, 9}, buf2);
    Assert.assertEquals(stream.available(), 12);
    Assert.assertEquals(stream.position(), 8);

    byte[] buf3 = new byte[6];
    Assert.assertEquals(stream.read(buf3), 6);
    Assert.assertArrayEquals(new byte[] {10, 11, 12, 13, 14, 15}, buf3);
    Assert.assertEquals(stream.available(), 6);
    Assert.assertEquals(stream.position(), 14);

    byte[] buf4 = new byte[6];
    Assert.assertEquals(stream.read(buf4), 6);
    Assert.assertArrayEquals(new byte[] {16, 17, 18, 19, 20, 21}, buf4);
    Assert.assertEquals(stream.available(), 0);
    Assert.assertEquals(stream.position(), 20);

    Assert.assertEquals(0, stream.available());
    stream.close();
  }

  @Test
  public void testSliceBuffers() throws Exception {
    final LazyByteBufferInputStream stream = newStream();

    // Initialize with a few reads to test when position != 0
    stream.read();
    stream.read();

    Assert.assertEquals(18, stream.available());
    Assert.assertEquals(2, stream.position());

    ByteBuffer slice1 = stream.sliceBuffers(4).get(0);
    Assert.assertEquals(0, slice1.position());
    Assert.assertEquals(4, slice1.remaining());

    byte[] buf = new byte[4];
    buf[0] = slice1.get();
    buf[1] = slice1.get();
    buf[2] = slice1.get();
    buf[3] = slice1.get();
    Assert.assertArrayEquals(new byte[] {4, 5, 6, 7}, buf);
    Assert.assertEquals(14, stream.available());
    Assert.assertEquals(6, stream.position());

    ByteBuffer slice2 = stream.sliceBuffers(6).get(0);
    Assert.assertEquals(0, slice2.position());
    Assert.assertEquals(6, slice2.remaining());

    byte[] buf2 = new byte[6];
    buf2[0] = slice2.get();
    buf2[1] = slice2.get();
    buf2[2] = slice2.get();
    buf2[3] = slice2.get();
    buf2[4] = slice2.get();
    buf2[5] = slice2.get();
    Assert.assertArrayEquals(new byte[] {8, 9, 10, 11, 12, 13}, buf2);
    Assert.assertEquals(8, stream.available());
    Assert.assertEquals(12, stream.position());

    stream.close();
  }
}
