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
package org.apache.parquet.hadoop;

import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCodecFactory {

  @Test
  public void testCreationOfPooledAndUnpooledDecompressors() {
    CodecFactory factory = null;
    try {
      factory = new CodecFactory(new Configuration(), 0);
      CodecFactory.BytesInputDecompressor gd1 = factory.getDecompressor(CompressionCodecName.GZIP);
      CodecFactory.BytesInputDecompressor gd2 = factory.getDecompressor(CompressionCodecName.GZIP);
      Assert.assertNotEquals(gd1, gd2);
      CodecFactory.BytesInputDecompressor sd1 = factory.getDecompressor(CompressionCodecName.SNAPPY);
      CodecFactory.BytesInputDecompressor sd2 = factory.getDecompressor(CompressionCodecName.SNAPPY);
      Assert.assertEquals(sd1, sd2);
    } finally {
      if (factory != null) {
        factory.release();
      }
    }
  }

  @Test
  public void testReleasingDecompressors() {
    final CodecFactory spyFactory = Mockito.spy(new CodecFactory(new Configuration(), 1));
    CodecFactory.BytesDecompressor unpooledDecompressor = new CodecFactory.BytesDecompressor() {
      @Override
      public BytesInput decompress(BytesInput bytes, int uncompressedSize) {
        return null;
      }

      @Override
      public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) {
      }

      @Override
      public void release() {
      }

      @Override
      public boolean isPooled() {
        return false;
      }
    };
    CodecFactory.BytesDecompressor spyUnpooled = spy(unpooledDecompressor);
    when(spyFactory.createDecompressor(CompressionCodecName.GZIP)).thenReturn(spyUnpooled);

    CodecFactory.BytesDecompressor pooledDecompressor = new CodecFactory.BytesDecompressor() {
      @Override
      public BytesInput decompress(BytesInput bytes, int uncompressedSize) {
        return null;
      }

      @Override
      public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) {
      }

      @Override
      public void release() {
      }

      @Override
      public boolean isPooled() {
        return true;
      }
    };
    CodecFactory.BytesDecompressor spyPooled = spy(pooledDecompressor);
    when(spyFactory.createDecompressor(CompressionCodecName.SNAPPY)).thenReturn(spyPooled);

    Assert.assertEquals(spyUnpooled, spyFactory.getDecompressor(CompressionCodecName.GZIP));
    Assert.assertEquals(spyPooled, spyFactory.getDecompressor(CompressionCodecName.SNAPPY));
    spyFactory.release();
    verify(spyFactory).release();
    verify(spyUnpooled).release();
    verify(spyPooled).release();
  }
}
