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
package org.apache.parquet.column.values.bytestreamsplit;

import static org.junit.Assert.assertEquals;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class ByteStreamSplitValuesWriterTest {

  public static class FloatTest {

    static float convertType(byte[] bytes) {
      int v = 0;
      for (int i = 0; i < bytes.length; ++i) {
        v |= ((bytes[i] & 0xFF) << (i * 8));
      }
      return Float.intBitsToFloat(v);
    }

    private ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter getWriter(int capacity) {
      return new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator());
    }

    @Test
    public void testSingleElement() throws Exception {
      final float value = 0.56274414062f;
      ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer = null;
      try {
        writer = getWriter(1);
        writer.writeFloat(value);

        // Check that the buffer size is exactly the size of one float in bytes.
        assertEquals(4, writer.getBufferedSize());

        // Get bytes and check that this doesn't modify the internal state.
        BytesInput bytesInput = writer.getBytes();
        assertEquals(4, writer.getBufferedSize());
        assertEquals(4, bytesInput.size());

        // Check that the bytes are as expected.
        final float newValue = convertType(bytesInput.toByteArray());
        assertEquals(value, newValue, 0.0f);

        // Check that reseting the writer clears the buffered data.
        writer.reset();
        assertEquals(0, writer.getBufferedSize());
        assertEquals(0, writer.getBytes().size());
      } finally {
        if (writer != null) {
          writer.reset();
          writer.close();
        }
      }
    }

    @Test
    public void testSmallBuffer() throws Exception {
      ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer = null;
      try {
        writer = getWriter(3);
        writer.writeFloat(202.625f);
        writer.writeFloat(1024.921875f);
        writer.writeFloat(2024.5f);

        assertEquals(12, writer.getBufferedSize());
        byte[] rawBytes = writer.getBytes().toByteArray();
        assertEquals(12, rawBytes.length);

        final byte[] expectedBytes = {
          (byte) 0x00, (byte) 0x80, (byte) 0x00,
          (byte) 0xa0, (byte) 0x1d, (byte) 0x10,
          (byte) 0x4a, (byte) 0x80, (byte) 0xfd,
          (byte) 0x43, (byte) 0x44, (byte) 0x44
        };
        for (int i = 0; i < 12; ++i) {
          assertEquals(expectedBytes[i], rawBytes[i]);
        }
      } finally {
        if (writer != null) {
          writer.reset();
          writer.close();
        }
      }
    }
  }

  public static class DoubleTest {

    static double convertType(byte[] bytes) {
      long v = 0;
      for (int i = 0; i < bytes.length; ++i) {
        v |= (((long) (bytes[i] & 0xFF)) << (i * 8));
      }
      return Double.longBitsToDouble(v);
    }

    private ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter getWriter(int capacity) {
      return new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator());
    }

    @Test
    public void testSingleElement() throws Exception {
      final double value = 23.6718811111112;
      ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter writer = null;
      try {
        writer = getWriter(1);
        writer.writeDouble(value);

        // Check that the buffer size is exactly the size of one double in bytes.
        assertEquals(8, writer.getBufferedSize());

        // Get bytes and check that this doesn't modify the internal state.
        BytesInput bytesInput = writer.getBytes();
        assertEquals(8, writer.getBufferedSize());
        assertEquals(8, bytesInput.size());

        // Check that the bytes are as expected.
        final double newValue = convertType(bytesInput.toByteArray());
        assertEquals(value, newValue, 0.0);

        // Check that reseting the writer clears the buffered data.
        writer.reset();
        assertEquals(0, writer.getBufferedSize());
        assertEquals(0, writer.getBytes().size());
      } finally {
        if (writer != null) {
          writer.reset();
          writer.close();
        }
      }
    }

    @Test
    public void testSmallBuffer() throws Exception {
      ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter writer = null;
      try {
        writer = getWriter(3);
        writer.writeDouble(13.6288992);
        writer.writeDouble(671.99901111);
        writer.writeDouble(3500199909.3019013);

        assertEquals(24, writer.getBufferedSize());
        byte[] rawBytes = writer.getBytes().toByteArray();
        assertEquals(24, rawBytes.length);

        final byte[] expectedBytes = {
          (byte) 0x0c, (byte) 0x53, (byte) 0x2D,
          (byte) 0xF6, (byte) 0x6E, (byte) 0xA9,
          (byte) 0x70, (byte) 0x89, (byte) 0xA9,
          (byte) 0x13, (byte) 0xF9, (byte) 0xFC,
          (byte) 0xFF, (byte) 0xFD, (byte) 0x19,
          (byte) 0x41, (byte) 0xFF, (byte) 0x14,
          (byte) 0x2b, (byte) 0x84, (byte) 0xEA,
          (byte) 0x40, (byte) 0x40, (byte) 0x41
        };

        for (int i = 0; i < 24; ++i) {
          assertEquals(expectedBytes[i], rawBytes[i]);
        }
      } finally {
        if (writer != null) {
          writer.reset();
          writer.close();
        }
      }
    }
  }

  public static class IntegerTest {
    private ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter getWriter(int capacity) {
      return new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator());
    }

    @Test
    public void testSmallBuffer() throws Exception {
      ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter writer = getWriter(3);
      writer.writeInteger(0x11223344);
      writer.writeInteger(0xFFEEDDCC);
      writer.writeInteger(0x66778899);

      assertEquals(12, writer.getBufferedSize());
      byte[] rawBytes = writer.getBytes().toByteArray();
      assertEquals(12, rawBytes.length);

      final byte[] expectedBytes = {
        (byte) 0x44, (byte) 0xCC, (byte) 0x99,
        (byte) 0x33, (byte) 0xDD, (byte) 0x88,
        (byte) 0x22, (byte) 0xEE, (byte) 0x77,
        (byte) 0x11, (byte) 0xFF, (byte) 0x66
      };
      for (int i = 0; i < expectedBytes.length; ++i) {
        assertEquals(expectedBytes[i], rawBytes[i]);
      }
      writer.reset();
      writer.close();
    }
  }

  public static class LongTest {
    private ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter getWriter(int capacity) {
      return new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
          capacity, capacity, new DirectByteBufferAllocator());
    }

    @Test
    public void testSmallBuffer() throws Exception {
      ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter writer = getWriter(2);
      writer.writeLong(0x1122334455667700L);
      writer.writeLong(0xFFEEDDCCBBAA9988L);

      assertEquals(16, writer.getBufferedSize());
      byte[] rawBytes = writer.getBytes().toByteArray();
      assertEquals(16, rawBytes.length);

      final byte[] expectedBytes = {
        (byte) 0x00, (byte) 0x88,
        (byte) 0x77, (byte) 0x99,
        (byte) 0x66, (byte) 0xAA,
        (byte) 0x55, (byte) 0xBB,
        (byte) 0x44, (byte) 0xCC,
        (byte) 0x33, (byte) 0xDD,
        (byte) 0x22, (byte) 0xEE,
        (byte) 0x11, (byte) 0xFF,
      };
      for (int i = 0; i < expectedBytes.length; ++i) {
        assertEquals(expectedBytes[i], rawBytes[i]);
      }
      writer.reset();
      writer.close();
    }
  }

  public static class FixedLenByteArrayTest {
    private ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter getWriter(
        int length, int capacity) {
      return new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
          length, capacity, capacity, new DirectByteBufferAllocator());
    }

    @Test
    public void testSmallBuffer() throws Exception {
      ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter writer = getWriter(3, 2);
      writer.writeBytes(Binary.fromString("abc"));
      writer.writeBytes(Binary.fromString("ghi"));

      assertEquals(6, writer.getBufferedSize());
      byte[] rawBytes = writer.getBytes().toByteArray();
      assertEquals(6, rawBytes.length);

      final byte[] expectedBytes = {
        'a', 'g', 'b', 'h', 'c', 'i',
      };
      for (int i = 0; i < expectedBytes.length; ++i) {
        assertEquals(expectedBytes[i], rawBytes[i]);
      }
      writer.reset();
      writer.close();
    }
  }
}
