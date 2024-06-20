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
package org.apache.parquet.benchmarks;
import static org.apache.parquet.benchmarks.VectorizedConversionBenchmark.EXAMPLE_RL;
import static org.apache.parquet.benchmarks.VectorizedConversionBenchmark.OFFSET;
import static org.apache.parquet.benchmarks.VectorizedConversionBenchmark.RL;

import java.util.Arrays;

import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COArrayInc;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COArrayInc2;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COBaseLine;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COBinaryInc;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COBinaryInc1;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COBinaryInc2;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COBinaryInc3;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COBinaryIncU;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.COTernaryInc;
import org.apache.parquet.benchmarks.VectorizedConversionBenchmark.ComputeOffset;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorizedConversion {

  static ComputeOffset[] cs = {
      new COBaseLine(),
      new COArrayInc(),
      new COArrayInc2(),
      new COBinaryInc(),
      new COBinaryInc1(),
      new COBinaryInc2(),
      new COBinaryInc3(),
      new COBinaryIncU(),
      new COTernaryInc()
      };

  /**
   * Actually verify that the implementation produce the right result
   */
  @Test
  public void testComputeOffsets() {
    char[] val = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'};
    int[] expectedCounts = { 2, 4, 10 };
    int[][] expectedOffsets = { { 0, 2, 4 }, { 0, 3, 7, 8, 10 } };
    for (ComputeOffset c : cs) {
      int[][] offset = { { 0, 0, 0 }, { 0, 0, 0, 0, 0 } };
      c.computeOffsets(EXAMPLE_RL, offset);
      print(expectedCounts, offset);
      Assert.assertArrayEquals(c.getClass().getSimpleName() + ": " + Arrays.deepToString(offset), expectedOffsets, offset);
    }
  }

  /**
   * roll your own benchmark.
   * see jmh based benchmark instead
   */
  @Test
  public void testComputeOffsetsGrown() {
    for (int k = 0; k < 3; k++) {
      System.out.println(k);
      for (ComputeOffset c : cs) {
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < 5000; i++) {
          c.computeOffsets(RL, OFFSET);
        }
        long t1 = System.currentTimeMillis();
        System.out.println(c.getClass().getSimpleName() + ": " + k + " " + (t1 - t0) + " ms");
      }
    }
  }


  private void print(int[] expectedCounts, int[][] offset) {
    System.out.println();
    for (int i = 0; i < expectedCounts.length; i++) {
      System.out.print(expectedCounts[i] + ", ");
    }
    System.out.println();
    System.out.println("--- offsets");
    for (int i = 0; i < offset.length; i++) {
      System.out.println(i + ":");
      for (int j = 0; j < offset[i].length; j++) {
        System.out.print(offset[i][j] + ", ");
      }
      System.out.println();
    }
  }
}
