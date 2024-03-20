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
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class ByteStreamSplitValuesReaderTest {
  private static <Reader extends ValuesReader> Reader makeReader(byte[] input, int length, Class<Reader> cls)
      throws Exception {
    ByteBuffer buffer = ByteBuffer.wrap(input);
    ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffer);
    Reader reader = cls.newInstance();
    reader.initFromPage(length, stream);
    return reader;
  }

  public static class FloatTest {

    private void testReader(byte[] input, float[] values) throws Exception {
      ByteStreamSplitValuesReaderForFloat reader =
          makeReader(input, values.length, ByteStreamSplitValuesReaderForFloat.class);
      for (float expectedValue : values) {
        float f = reader.readFloat();
        assertEquals(expectedValue, f, 0.0f);
      }
      // All data exhausted
      assertThrows(ParquetDecodingException.class, () -> reader.readFloat());
    }

    @Test
    public void testSingleElement() throws Exception {
      byte[] byteData = {(byte) 0x00, (byte) 0x00, (byte) 0x10, (byte) 0x40};
      testReader(byteData, new float[] {2.25f});
    }

    @Test
    public void testSmallBuffer() throws Exception {
      byte[] byteData = {
        (byte) 0x40,
        (byte) 0x00,
        (byte) 0x80,
        (byte) 0x40,
        (byte) 0x05,
        (byte) 0x84,
        (byte) 0xc5,
        (byte) 0xbd,
        (byte) 0x32,
        (byte) 0xc2,
        (byte) 0x41,
        (byte) 0x42
      };
      float[] expectedValues = {-98.62548828125f, 23.62744140625f, 44.62939453125f};
      testReader(byteData, expectedValues);
    }

    @Test
    public void testRandomInput() throws Exception {
      Random rand = new Random(1337);
      final int numElements = 256;
      byte[] byteData = new byte[numElements * 4];
      float[] values = new float[numElements];
      for (int i = 0; i < numElements; ++i) {
        float f = rand.nextFloat() * 1024.0f;
        values[i] = f;
        int fAsInt = Float.floatToIntBits(f);
        byteData[i] = (byte) (fAsInt & 0xFF);
        byteData[numElements + i] = (byte) ((fAsInt >> 8) & 0xFF);
        byteData[numElements * 2 + i] = (byte) ((fAsInt >> 16) & 0xFF);
        byteData[numElements * 3 + i] = (byte) ((fAsInt >> 24) & 0xFF);
      }
      testReader(byteData, values);
    }

    @Test
    public void testExtraReads() throws Exception {
      byte[] byteData = {(byte) 0x00, (byte) 0x00, (byte) 0x10, (byte) 0x40};
      ByteStreamSplitValuesReaderForFloat reader =
          makeReader(byteData, 1, ByteStreamSplitValuesReaderForFloat.class);
      float f = reader.readFloat();
      assertEquals(2.25f, f, 0.0f);
      assertThrows(ParquetDecodingException.class, () -> reader.readFloat());
    }

    @Test
    public void testSkip() throws Exception {
      byte[] byteData = new byte[16];
      for (int i = 0; i < 16; ++i) {
        byteData[i] = (byte) 0xFF;
      }
      byteData[3] = (byte) 0x00;
      byteData[7] = (byte) 0x00;
      byteData[11] = (byte) 0x10;
      byteData[15] = (byte) 0x40;

      ByteStreamSplitValuesReaderForFloat reader =
          makeReader(byteData, 4, ByteStreamSplitValuesReaderForFloat.class);
      reader.skip(3);
      float f = reader.readFloat();
      assertEquals(2.25f, f, 0.0f);
      // Data exhausted
      assertThrows(ParquetDecodingException.class, () -> reader.readFloat());
    }

    @Test
    public void testSkipOverflow() throws Exception {
      byte[] byteData = new byte[128];
      ByteStreamSplitValuesReaderForFloat reader =
          makeReader(byteData, 32, ByteStreamSplitValuesReaderForFloat.class);
      assertThrows(ParquetDecodingException.class, () -> reader.skip(33));
    }

    @Test
    public void testSkipUnderflow() throws Exception {
      byte[] byteData = new byte[128];
      ByteStreamSplitValuesReaderForFloat reader =
          makeReader(byteData, 32, ByteStreamSplitValuesReaderForFloat.class);
      assertThrows(ParquetDecodingException.class, () -> reader.skip(-1));
    }
  }

  public static class DoubleTest {

    private void testReader(byte[] input, double[] values) throws Exception {
      ByteStreamSplitValuesReaderForDouble reader =
          makeReader(input, values.length, ByteStreamSplitValuesReaderForDouble.class);
      for (double expectedValue : values) {
        double d = reader.readDouble();
        assertEquals(expectedValue, d, 0.0);
      }
      // All data exhausted
      assertThrows(ParquetDecodingException.class, () -> reader.readDouble());
    }

    @Test
    public void testSingleElement() throws Exception {
      byte[] byteData = {
        (byte) 0xFE, (byte) 0xFF, (byte) 0xFF, (byte) 0x0D, (byte) 0xA8, (byte) 0x77, (byte) 0xD2, (byte) 0x40
      };
      testReader(byteData, new double[] {18910.62585449218});
    }

    @Test
    public void testSmallBuffer() throws Exception {
      byte[] byteData = {
        (byte) 0xE7,
        (byte) 0x72,
        (byte) 0xBE,
        (byte) 0x09,
        (byte) 0xA1,
        (byte) 0xC1,
        (byte) 0x0A,
        (byte) 0x0A,
        (byte) 0x17,
        (byte) 0xD7,
        (byte) 0x21,
        (byte) 0x26,
        (byte) 0x01,
        (byte) 0xC7,
        (byte) 0x53,
        (byte) 0x0A,
        (byte) 0x46,
        (byte) 0x05,
        (byte) 0x70,
        (byte) 0xF3,
        (byte) 0xE4,
        (byte) 0x40,
        (byte) 0xC0,
        (byte) 0x3F
      };
      double[] expectedValues = {256.625449218, -78956.4455667788, 0.62565};
      testReader(byteData, expectedValues);
    }

    @Test
    public void testRandomInput() throws Exception {
      Random rand = new Random(6557);
      final int numElements = 256;
      byte[] byteData = new byte[numElements * 8];
      double[] values = new double[numElements];
      for (int i = 0; i < numElements; ++i) {
        double f = rand.nextDouble() * 8192.0;
        values[i] = f;
        long fAsLong = Double.doubleToLongBits(f);
        for (int j = 0; j < 8; ++j) {
          byteData[numElements * j + i] = (byte) ((fAsLong >> (8 * j)) & 0xFF);
        }
      }
      testReader(byteData, values);
    }
  }

  public static class IntegerTest {
    private void testReader(byte[] input, int[] values) throws Exception {
      ByteStreamSplitValuesReaderForInteger reader =
          makeReader(input, values.length, ByteStreamSplitValuesReaderForInteger.class);
      for (int expectedValue : values) {
        int actual = reader.readInteger();
        assertEquals(expectedValue, actual);
      }
      // All data exhausted
      assertThrows(ParquetDecodingException.class, () -> reader.readInteger());
    }

    @Test
    public void testSingleElement() throws Exception {
      byte[] byteData = {(byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78};
      testReader(byteData, new int[] {0x78563412});
    }

    @Test
    public void testSmallBuffer() throws Exception {
      byte[] byteData = {
        (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78,
        (byte) 0x9A, (byte) 0xBC, (byte) 0xDE, (byte) 0xF0,
      };
      testReader(byteData, new int[] {0xDE9A5612, 0xF0BC7834});
    }
  }

  public static class LongTest {
    private void testReader(byte[] input, long[] values) throws Exception {
      ByteStreamSplitValuesReaderForLong reader =
          makeReader(input, values.length, ByteStreamSplitValuesReaderForLong.class);
      for (long expectedValue : values) {
        long actual = reader.readLong();
        assertEquals(expectedValue, actual);
      }
      // All data exhausted
      assertThrows(ParquetDecodingException.class, () -> reader.readLong());
    }

    @Test
    public void testSingleElement() throws Exception {
      byte[] byteData = {
        (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78,
        (byte) 0x9A, (byte) 0xBC, (byte) 0xDE, (byte) 0xF0,
      };
      testReader(byteData, new long[] {0xF0DEBC9A78563412L});
    }

    @Test
    public void testSmallBuffer() throws Exception {
      byte[] byteData = {
        (byte) 0x00, (byte) 0x11, (byte) 0x22, (byte) 0x33,
        (byte) 0x44, (byte) 0x55, (byte) 0x66, (byte) 0x77,
        (byte) 0x88, (byte) 0x99, (byte) 0xAA, (byte) 0xBB,
        (byte) 0xCC, (byte) 0xDD, (byte) 0xEE, (byte) 0xFF,
      };
      testReader(byteData, new long[] {0xEECCAA8866442200L, 0xFFDDBB9977553311L});
    }
  }

  public static class FixedLenByteArrayTest {
    private static ByteStreamSplitValuesReaderForFLBA makeReader(byte[] input, int valuesCount) throws Exception {
      ByteBuffer buffer = ByteBuffer.wrap(input);
      ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffer);
      ByteStreamSplitValuesReaderForFLBA reader =
          new ByteStreamSplitValuesReaderForFLBA(input.length / valuesCount);
      reader.initFromPage(valuesCount, stream);
      return reader;
    }

    private void testReader(byte[] input, byte[][] values) throws Exception {
      ByteStreamSplitValuesReaderForFLBA reader = makeReader(input, values.length);
      Binary previousExpected = null, previousActual = null;
      for (byte[] expectedValue : values) {
        Binary expected = Binary.fromReusedByteArray(expectedValue);
        Binary actual = reader.readBytes();
        assertEquals(expected, actual);
        if (previousExpected != null) {
          // The latest readBytes() call shouldn't have clobbered the result of the previous call.
          assertEquals(previousExpected, previousActual);
        }
        previousExpected = expected;
        previousActual = actual;
      }
      // All data exhausted
      assertThrows(ParquetDecodingException.class, () -> reader.readBytes());
    }

    @Test
    public void testSingleElement() throws Exception {
      byte[] byteData = {
        (byte) 0x12, (byte) 0x34, (byte) 0x56,
      };
      byte[][] values = {
        {
          (byte) 0x12, (byte) 0x34, (byte) 0x56,
        }
      };
      testReader(byteData, values);
    }

    @Test
    public void testSmallBuffer() throws Exception {
      byte[] byteData = {
        (byte) 0x12, (byte) 0x34, (byte) 0x56,
        (byte) 0x78, (byte) 0x9A, (byte) 0xBC,
      };
      byte[][] values = {
        {
          (byte) 0x12, (byte) 0x56, (byte) 0x9A,
        },
        {
          (byte) 0x34, (byte) 0x78, (byte) 0xBC,
        },
      };
      testReader(byteData, values);
    }
  }
}
