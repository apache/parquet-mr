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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;

public class TestParquetReadRouter {
  private static final Logger LOG = LoggerFactory.getLogger(TestParquetReadRouter.class);

  private static final int minBitWidth = 1;
  private static final int maxBitWidth = 32;
  private static final int outputValues = 1024;
  private final int[] output = new int[outputValues];
  private final int[] outputBatch = new int[outputValues];
  private final int[] outputBatchVector = new int[outputValues];

  @Test
  public void testRead() throws IOException {
    for (int bitWidth=minBitWidth; bitWidth <= maxBitWidth; bitWidth++) {
      byte[] input = new byte[outputValues * bitWidth / 8];
      for (int i = 0; i < input.length; i++) {
        input[i] = (byte) i;
      }
      ByteBufferInputStream inputStream = ByteBufferInputStream.wrap(ByteBuffer.wrap(input));

      ParquetReadRouter.read(bitWidth, inputStream, 0, output);
      ParquetReadRouter.readBatch(bitWidth, inputStream, 0, outputBatch);
      assertArrayEquals(output, outputBatch);
      Assume.assumeTrue(ParquetReadRouter.getSupportVectorFromCPUFlags() == VectorSupport.VECTOR_512);
      ParquetReadRouter.readBatchUsing512Vector(bitWidth, inputStream, 0, outputBatchVector);
      assertArrayEquals(output, outputBatchVector);
    }
  }
}
