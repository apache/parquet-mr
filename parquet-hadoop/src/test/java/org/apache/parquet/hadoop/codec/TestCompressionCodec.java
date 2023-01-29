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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestCompressionCodec {

  @Test
  public void testLz4RawBlock() throws IOException {
    testBlock(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void testSnappyBlock() throws IOException {
    testBlock(CompressionCodecName.SNAPPY);
  }

  private void testBlock(CompressionCodecName codecName) throws IOException {
    // Reuse the codec objects between test cases
    CompressionCodec codec = getCodec(codecName, 4 * 1024);
    Compressor compressor = codec.createCompressor();
    Decompressor decompressor = codec.createDecompressor();

    testBlockCompression(compressor, decompressor, "");
    testBlockCompression(compressor, decompressor, "FooBar");
    testBlockCompression(compressor, decompressor, "FooBar1FooBar2");
    testBlockCompression(compressor, decompressor, "FooBar");
    testBlockCompression(compressor, decompressor, "ablahblahblahabcdef");
    testBlockCompression(compressor, decompressor, "");
    testBlockCompression(compressor, decompressor, "FooBar");
  }

  // Test compression in the block fashion
  private void testBlockCompression(Compressor compressor, Decompressor decompressor,
                                    String data) throws IOException {
    compressor.reset();
    decompressor.reset();

    int uncompressedSize = data.length();
    byte[] uncompressedData = data.getBytes();

    assert (compressor.needsInput());
    compressor.setInput(uncompressedData, 0, uncompressedSize);
    assert (compressor.needsInput());
    compressor.finish();
    assert (!compressor.needsInput());
    assert (!compressor.finished() || uncompressedSize == 0);
    byte[] compressedData = new byte[1000];

    int compressedSize = compressor.compress(compressedData, 0, 1000);
    assert (compressor.finished());

    assert (!decompressor.finished());
    assert (decompressor.needsInput());
    decompressor.setInput(compressedData, 0, compressedSize);
    assert (!decompressor.finished());
    byte[] decompressedData = new byte[uncompressedSize];
    int decompressedSize = decompressor.decompress(decompressedData, 0, uncompressedSize);
    assert (decompressor.finished());

    assertEquals(uncompressedSize, decompressedSize);
    assertArrayEquals(uncompressedData, decompressedData);
  }

  @Test
  public void testLz4RawCodec() throws IOException {
    testCodec(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void testSnappyCodec() throws IOException {
    testCodec(CompressionCodecName.SNAPPY);
  }

  // Test compression in the streaming fashion
  private void testCodec(CompressionCodecName codecName) throws IOException {
    int[] bufferSizes = {128, 1024, 4 * 1024, 16 * 1024, 128 * 1024, 1024 * 1024};
    int[] dataSizes = {0, 1, 10, 128, 1024, 2048, 1024 * 1024};

    for (int i = 0; i < bufferSizes.length; i++) {
      CompressionCodec codec = getCodec(codecName, bufferSizes[i]);
      for (int j = 0; j < dataSizes.length; j++) {
        // do not repeat
        testCodec(codec, dataSizes[j], dataSizes[j]);
        // repeat by every 128 bytes
        testCodec(codec, dataSizes[j], 128);
      }
    }
  }

  private void testCodec(CompressionCodec codec, int dataSize, int repeatSize) throws IOException {
    byte[] data = new byte[dataSize];
    if (repeatSize >= dataSize) {
      (new Random()).nextBytes(data);
    } else {
      byte[] repeat = new byte[repeatSize];
      (new Random()).nextBytes(repeat);
      for (int offset = 0; offset < dataSize; offset += repeatSize) {
        System.arraycopy(repeat, 0, data, offset, Math.min(repeatSize, dataSize - offset));
      }
    }
    BytesInput compressedData = compress(codec, BytesInput.from(data));
    byte[] decompressedData = decompress(codec, compressedData, data.length);
    Assert.assertArrayEquals(data, decompressedData);
  }

  private BytesInput compress(CompressionCodec codec, BytesInput bytes) throws IOException {
    ByteArrayOutputStream compressedOutBuffer = new ByteArrayOutputStream((int) bytes.size());
    CompressionOutputStream cos = codec.createOutputStream(compressedOutBuffer);
    bytes.writeAllTo(cos);
    cos.close();
    return BytesInput.from(compressedOutBuffer);
  }

  private byte[] decompress(CompressionCodec codec, BytesInput bytes, int uncompressedSize) throws IOException {
    InputStream is = codec.createInputStream(bytes.toInputStream());
    byte[] decompressed = BytesInput.from(is, uncompressedSize).toByteArray();
    is.close();
    return decompressed;
  }

  private CompressionCodec getCodec(CompressionCodecName codecName, int bufferSize) {
    switch (codecName) {
      case LZ4_RAW: {
        Configuration conf = new Configuration();
        conf.setInt(Lz4RawCodec.BUFFER_SIZE_CONFIG, bufferSize);
        Lz4RawCodec codec = new Lz4RawCodec();
        codec.setConf(conf);
        return codec;
      }
      case SNAPPY: {
        Configuration conf = new Configuration();
        conf.setInt("io.file.buffer.size", bufferSize);
        SnappyCodec codec = new SnappyCodec();
        codec.setConf(conf);
        return codec;
      }
      default:
        // Not implemented yet
        return null;
    }
  }

}
