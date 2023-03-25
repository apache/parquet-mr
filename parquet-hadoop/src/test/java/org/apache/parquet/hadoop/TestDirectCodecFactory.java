/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.junit.Assert;
import org.junit.Test;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.BROTLI;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZO;

public class TestDirectCodecFactory {

  private enum Decompression {
    ON_HEAP, OFF_HEAP, OFF_HEAP_BYTES_INPUT
  }

  private final int pageSize = 64 * 1024;

  private void test(int size, CompressionCodecName codec, boolean useOnHeapCompression, Decompression decomp) {
    ByteBuffer rawBuf = null;
    ByteBuffer outBuf = null;
    ByteBufferAllocator allocator = null;
    try {
      allocator = new DirectByteBufferAllocator();
      final CodecFactory codecFactory = CodecFactory.createDirectCodecFactory(new Configuration(), allocator, pageSize);
      rawBuf = allocator.allocate(size);
      final byte[] rawArr = new byte[size];
      outBuf = allocator.allocate(size * 2);
      final Random r = new Random();
      final byte[] random = new byte[1024];
      int pos = 0;
      while (pos < size) {
        r.nextBytes(random);
        rawBuf.put(random);
        System.arraycopy(random, 0, rawArr, pos, random.length);
        pos += random.length;
      }
      rawBuf.flip();

      final DirectCodecFactory.BytesCompressor c = codecFactory.getCompressor(codec);
      final CodecFactory.BytesDecompressor d = codecFactory.getDecompressor(codec);

      final BytesInput compressed;
      if (useOnHeapCompression) {
        compressed = c.compress(BytesInput.from(rawArr));
      } else {
        compressed = c.compress(BytesInput.from(rawBuf));
      }

      switch (decomp) {
        case OFF_HEAP: {
          final ByteBuffer buf = compressed.toByteBuffer();
          final ByteBuffer b = allocator.allocate(buf.capacity());
          try {
            b.put(buf);
            b.flip();
            d.decompress(b, (int) compressed.size(), outBuf, size);
            for (int i = 0; i < size; i++) {
              Assert.assertTrue("Data didn't match at " + i, outBuf.get(i) == rawBuf.get(i));
            }
          } finally {
            allocator.release(b);
          }
          break;
        }

        case OFF_HEAP_BYTES_INPUT: {
          final ByteBuffer buf = compressed.toByteBuffer();
          final ByteBuffer b = allocator.allocate(buf.limit());
          try {
            b.put(buf);
            b.flip();
            final BytesInput input = d.decompress(BytesInput.from(b), size);
            Assert.assertArrayEquals(
                String.format("While testing codec %s", codec),
                input.toByteArray(), rawArr);
          } finally {
            allocator.release(b);
          }
          break;
        }
        case ON_HEAP: {
          final byte[] buf = compressed.toByteArray();
          final BytesInput input = d.decompress(BytesInput.from(buf), size);
          Assert.assertArrayEquals(input.toByteArray(), rawArr);
          break;
        }
      }
    } catch (Exception e) {
      final String msg = String.format(
          "Failure while testing Codec: %s, OnHeapCompressionInput: %s, Decompression Mode: %s, Data Size: %d",
          codec.name(),
          useOnHeapCompression, decomp.name(), size);
      System.out.println(msg);
      throw new RuntimeException(msg, e);
    } finally {
      if (rawBuf != null) {
        allocator.release(rawBuf);
      }
      if (outBuf != null) {
        allocator.release(rawBuf);
      }
    }
  }

  @Test
  public void createDirectFactoryWithHeapAllocatorFails() {
    String errorMsg = "Test failed, creation of a direct codec factory should have failed when passed a non-direct allocator.";
    try {
      CodecFactory.createDirectCodecFactory(new Configuration(), new HeapByteBufferAllocator(), 0);
      throw new RuntimeException(errorMsg);
    } catch (IllegalStateException ex) {
      // indicates successful completion of the test
      Assert.assertTrue("Missing expected error message.",
          ex.getMessage()
          .contains("A DirectCodecFactory requires a direct buffer allocator be provided.")
      );
    } catch (Exception ex) {
      throw new RuntimeException(errorMsg + " Failed with the wrong error.");
    }
  }

  @Test
  public void compressionCodecs() {
    final int[] sizes = { 4 * 1024, 1 * 1024 * 1024 };
    final boolean[] comp = { true, false };
    Set<CompressionCodecName> codecsToSkip = new HashSet<>();
    codecsToSkip.add(LZO); // not distributed because it is GPL
    codecsToSkip.add(LZ4); // not distributed in the default version of Hadoop
    final String arch = System.getProperty("os.arch");
    if ("aarch64".equals(arch)) {
      // PARQUET-1975 brotli-codec does not have natives for ARM64 architectures
      codecsToSkip.add(BROTLI);
    }

    for (final int size : sizes) {
      for (final boolean useOnHeapComp : comp) {
        for (final Decompression decomp : Decompression.values()) {
          for (final CompressionCodecName codec : CompressionCodecName.values()) {
            if (codecsToSkip.contains(codec)) {
              continue;
            }
            test(size, codec, useOnHeapComp, decomp);
          }
        }
      }
    }
  }
}

