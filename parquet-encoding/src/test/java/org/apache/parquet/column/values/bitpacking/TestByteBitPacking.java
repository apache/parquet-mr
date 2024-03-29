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
package org.apache.parquet.column.values.bitpacking;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingWriter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestByteBitPacking {
  private static final Logger LOG = LoggerFactory.getLogger(TestByteBitPacking.class);

  @Test
  public void testPackUnPack() {
    LOG.debug("");
    LOG.debug("testPackUnPack");
    for (int i = 1; i < 32; i++) {
      LOG.debug("Width: {}", i);
      int[] unpacked = new int[32];
      int[] values = generateValues(i);
      packUnpack(Packer.BIG_ENDIAN.newBytePacker(i), values, unpacked);
      LOG.debug("Output: {}", TestBitPacking.toString(unpacked));
      Assert.assertArrayEquals("width " + i, values, unpacked);
    }
  }

  @Test
  public void testPackUnPackLong() {
    LOG.debug("");
    LOG.debug("testPackUnPackLong");
    for (int i = 1; i < 64; i++) {
      LOG.debug("Width: {}", i);
      long[] unpacked32 = new long[32];
      long[] unpacked8 = new long[32];
      long[] values = generateValuesLong(i);
      packUnpack32(Packer.BIG_ENDIAN.newBytePackerForLong(i), values, unpacked32);
      LOG.debug("Output 32: {}", TestBitPacking.toString(unpacked32));
      Assert.assertArrayEquals("width " + i, values, unpacked32);
      packUnpack8(Packer.BIG_ENDIAN.newBytePackerForLong(i), values, unpacked8);
      LOG.debug("Output 8: {}", TestBitPacking.toString(unpacked8));
      Assert.assertArrayEquals("width " + i, values, unpacked8);
    }
  }

  private void packUnpack(BytePacker packer, int[] values, int[] unpacked) {
    byte[] packed = new byte[packer.getBitWidth() * 4];
    packer.pack32Values(values, 0, packed, 0);
    LOG.debug("packed: {}", TestBitPacking.toString(packed));
    packer.unpack32Values(ByteBuffer.wrap(packed), 0, unpacked, 0);
  }

  private void packUnpack32(BytePackerForLong packer, long[] values, long[] unpacked) {
    byte[] packed = new byte[packer.getBitWidth() * 4];
    packer.pack32Values(values, 0, packed, 0);
    LOG.debug("packed: {}", TestBitPacking.toString(packed));
    packer.unpack32Values(packed, 0, unpacked, 0);
  }

  private void packUnpack8(BytePackerForLong packer, long[] values, long[] unpacked) {
    byte[] packed = new byte[packer.getBitWidth() * 4];
    for (int i = 0; i < 4; i++) {
      packer.pack8Values(values, 8 * i, packed, packer.getBitWidth() * i);
    }
    LOG.debug("packed: {}", TestBitPacking.toString(packed));
    for (int i = 0; i < 4; i++) {
      packer.unpack8Values(packed, packer.getBitWidth() * i, unpacked, 8 * i);
    }
  }

  private int[] generateValues(int bitWidth) {
    int[] values = new int[32];
    for (int j = 0; j < values.length; j++) {
      values[j] = (int) (Math.random() * 100000) % (int) Math.pow(2, bitWidth);
    }
    LOG.debug("Input:  {}", TestBitPacking.toString(values));
    return values;
  }

  private long[] generateValuesLong(int bitWidth) {
    long[] values = new long[32];
    Random random = new Random(0);
    for (int j = 0; j < values.length; j++) {
      values[j] = random.nextLong() & ((1l << bitWidth) - 1l);
    }
    LOG.debug("Input:  {}", TestBitPacking.toString(values));
    return values;
  }

  @Test
  public void testPackUnPackAgainstHandWritten() throws IOException {
    LOG.debug("");
    LOG.debug("testPackUnPackAgainstHandWritten");
    for (int i = 1; i < 8; i++) {
      LOG.debug("Width: {}", i);
      byte[] packed = new byte[i * 4];
      int[] unpacked = new int[32];
      int[] values = generateValues(i);

      // pack generated
      final BytePacker packer = Packer.BIG_ENDIAN.newBytePacker(i);
      packer.pack32Values(values, 0, packed, 0);

      LOG.debug("Generated: {}", TestBitPacking.toString(packed));

      // pack manual
      final ByteArrayOutputStream manualOut = new ByteArrayOutputStream();
      final BitPackingWriter writer = BitPacking.getBitPackingWriter(i, manualOut);
      for (int j = 0; j < values.length; j++) {
        writer.write(values[j]);
      }
      final byte[] packedManualAsBytes = manualOut.toByteArray();
      LOG.debug("Manual: {}", TestBitPacking.toString(packedManualAsBytes));

      // unpack manual
      final BitPackingReader reader = BitPacking.createBitPackingReader(i, new ByteArrayInputStream(packed), 32);
      for (int j = 0; j < unpacked.length; j++) {
        unpacked[j] = reader.read();
      }

      LOG.debug("Output: {}", TestBitPacking.toString(unpacked));
      Assert.assertArrayEquals("width " + i, values, unpacked);
    }
  }

  @Test
  public void testPackUnPackAgainstLemire() throws IOException {
    for (Packer pack : Packer.values()) {
      LOG.debug("");
      LOG.debug("testPackUnPackAgainstLemire {}", pack.name());
      for (int i = 1; i < 32; i++) {
        LOG.debug("Width: {}", i);
        int[] packed = new int[i];
        int[] unpacked = new int[32];
        int[] values = generateValues(i);

        // pack lemire
        final IntPacker packer = pack.newIntPacker(i);
        packer.pack32Values(values, 0, packed, 0);
        // convert to bytes
        final ByteArrayOutputStream lemireOut = new ByteArrayOutputStream();
        for (int v : packed) {
          switch (pack) {
            case LITTLE_ENDIAN:
              lemireOut.write((v >>> 0) & 0xFF);
              lemireOut.write((v >>> 8) & 0xFF);
              lemireOut.write((v >>> 16) & 0xFF);
              lemireOut.write((v >>> 24) & 0xFF);
              break;
            case BIG_ENDIAN:
              lemireOut.write((v >>> 24) & 0xFF);
              lemireOut.write((v >>> 16) & 0xFF);
              lemireOut.write((v >>> 8) & 0xFF);
              lemireOut.write((v >>> 0) & 0xFF);
              break;
          }
        }
        final byte[] packedByLemireAsBytes = lemireOut.toByteArray();
        LOG.debug("Lemire out: {}", TestBitPacking.toString(packedByLemireAsBytes));

        // pack manual
        final BytePacker bytePacker = pack.newBytePacker(i);
        byte[] packedGenerated = new byte[i * 4];
        bytePacker.pack32Values(values, 0, packedGenerated, 0);
        LOG.debug("Gener. out: {}", TestBitPacking.toString(packedGenerated));
        Assert.assertEquals(
            pack.name() + " width " + i,
            TestBitPacking.toString(packedByLemireAsBytes),
            TestBitPacking.toString(packedGenerated));

        bytePacker.unpack32Values(ByteBuffer.wrap(packedByLemireAsBytes), 0, unpacked, 0);
        LOG.debug("Output: {}", TestBitPacking.toString(unpacked));

        Assert.assertArrayEquals("width " + i, values, unpacked);
      }
    }
  }
}
