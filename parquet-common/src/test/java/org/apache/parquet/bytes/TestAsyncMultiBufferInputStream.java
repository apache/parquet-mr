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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAsyncMultiBufferInputStream extends TestMultiBufferInputStream {
  private static final List<ByteBuffer> DATA = Arrays.asList(
      ByteBuffer.allocate(9),
      ByteBuffer.allocate(4),
      ByteBuffer.allocate(0),
      ByteBuffer.allocate(12),
      ByteBuffer.allocate(1),
      ByteBuffer.allocate(7),
      ByteBuffer.allocate(2));

  private static final List<ByteBuffer> WRITEDATA = Arrays.asList(
      ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8}),
      ByteBuffer.wrap(new byte[] {9, 10, 11, 12}),
      ByteBuffer.wrap(new byte[] {}),
      ByteBuffer.wrap(new byte[] {13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}),
      ByteBuffer.wrap(new byte[] {25}),
      ByteBuffer.wrap(new byte[] {26, 27, 28, 29, 30, 31, 32}),
      ByteBuffer.wrap(new byte[] {33, 34}));

  private static Path tempPath;
  private static String filename;
  private static SeekableInputStream seekableInputStream;
  private static ExecutorService threadPool = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors() * 2, r -> new Thread(r, "test-parquet-async-io"));

  @BeforeClass
  public static void init() throws IOException {
    tempPath = Files.createTempDirectory("asyncstream");
    filename = tempPath.toAbsolutePath() + "/async_mbinput";
    OutputStream outputStream = new FileOutputStream(filename);
    for (ByteBuffer writedatum : WRITEDATA) {
      outputStream.write(writedatum.array());
    }
  }

  @AfterClass
  public static void close() throws IOException {
    threadPool.shutdownNow();
    Files.deleteIfExists(Paths.get(filename));
    Files.deleteIfExists(tempPath);
  }

  @Override
  protected ByteBufferInputStream newStream() {
    try {
      seekableInputStream = new LocalFSInputStream(new FileInputStream(filename));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to initialize test input stream.", e);
    }
    AsyncMultiBufferInputStream stream = new AsyncMultiBufferInputStream(threadPool, seekableInputStream, DATA);
    stream.nextBuffer();
    return stream;
  }

  @Override
  protected void checkOriginalData() {
    for (ByteBuffer buffer : DATA) {
      Assert.assertEquals("Position should not change", 0, buffer.position());
      Assert.assertEquals("Limit should not change", buffer.array().length, buffer.limit());
    }
  }

  @Test
  public void testSliceData() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    List<ByteBuffer> buffers = new ArrayList<>();
    // slice the stream into 3 8-byte buffers and 1 2-byte buffer
    while (stream.available() > 0) {
      int bytesToSlice = Math.min(stream.available(), 8);
      buffers.add(stream.slice(bytesToSlice));
    }

    Assert.assertEquals("Position should be at end", length, stream.position());
    Assert.assertEquals("Should produce 5 buffers", 5, buffers.size());

    int i = 0;

    // one is a view of the first buffer because it is smaller
    ByteBuffer one = buffers.get(0);
    Assert.assertSame(
        "Should be a duplicate of the first array",
        one.array(),
        DATA.get(0).array());
    Assert.assertEquals(8, one.remaining());
    Assert.assertEquals(0, one.position());
    Assert.assertEquals(8, one.limit());
    Assert.assertEquals(9, one.capacity());
    for (; i < 8; i += 1) {
      Assert.assertEquals("Should produce correct values", i, one.get());
    }

    // two should be a copy of the next 8 bytes
    ByteBuffer two = buffers.get(1);
    Assert.assertEquals(8, two.remaining());
    Assert.assertEquals(0, two.position());
    Assert.assertEquals(8, two.limit());
    Assert.assertEquals(8, two.capacity());
    for (; i < 16; i += 1) {
      Assert.assertEquals("Should produce correct values", i, two.get());
    }

    // three is a copy of part of the 4th buffer
    ByteBuffer three = buffers.get(2);
    Assert.assertSame(
        "Should be a duplicate of the fourth array",
        three.array(),
        DATA.get(3).array());
    Assert.assertEquals(8, three.remaining());
    Assert.assertEquals(3, three.position());
    Assert.assertEquals(11, three.limit());
    Assert.assertEquals(12, three.capacity());
    for (; i < 24; i += 1) {
      Assert.assertEquals("Should produce correct values", i, three.get());
    }

    // four should be a copy of the next 8 bytes
    ByteBuffer four = buffers.get(3);
    Assert.assertEquals(8, four.remaining());
    Assert.assertEquals(0, four.position());
    Assert.assertEquals(8, four.limit());
    Assert.assertEquals(8, four.capacity());
    for (; i < 32; i += 1) {
      Assert.assertEquals("Should produce correct values", i, four.get());
    }

    // five should be a copy of the next 8 bytes
    ByteBuffer five = buffers.get(4);
    Assert.assertEquals(3, five.remaining());
    Assert.assertEquals(0, five.position());
    Assert.assertEquals(3, five.limit());
    Assert.assertEquals(3, five.capacity());
    for (; i < 35; i += 1) {
      Assert.assertEquals("Should produce correct values", i, five.get());
    }
  }

  public static class LocalFSInputStream extends DelegatingSeekableInputStream {

    long pos = 0;
    private final InputStream stream;

    public LocalFSInputStream(InputStream stream) {
      super(stream);
      this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      long skipBytes;
      if (newPos < pos) {
        stream.reset();
        skipBytes = newPos;
      } else {
        skipBytes = newPos - pos;
      }
      while (skipBytes > 0) {
        long n = stream.skip(skipBytes);
        if (n == 0) break;
        skipBytes -= n;
      }
      pos = newPos - skipBytes;
    }

    @Override
    public int read() throws IOException {
      pos++;
      return super.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      pos += len;
      return super.read(b, off, len);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      pos += bytes.length;
      super.readFully(bytes);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      pos += len - start;
      super.readFully(bytes, start, len);
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      int n = super.read(buf);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      pos += buf.remaining();
      super.readFully(buf);
    }
  }
}
