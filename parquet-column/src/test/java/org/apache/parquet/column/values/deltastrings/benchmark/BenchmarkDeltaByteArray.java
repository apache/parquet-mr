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
package org.apache.parquet.column.values.deltastrings.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import java.io.IOException;
import java.util.Arrays;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.junit.Rule;
import org.junit.Test;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-encoding-writing-random")
public class BenchmarkDeltaByteArray {

  @Rule
  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

  static String[] values = Utils.getRandomStringSamples(1000000, 32);
  static String[] sortedVals;

  static {
    sortedVals = Arrays.copyOf(values, values.length);
    Arrays.sort(sortedVals);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void benchmarkRandomStringsWithPlainValuesWriter() throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    BinaryPlainValuesReader reader = new BinaryPlainValuesReader();

    Utils.writeData(writer, values);
    ByteBufferInputStream data = writer.getBytes().toInputStream();
    Utils.readData(reader, data, values.length);
    System.out.println("size " + data.position());
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void benchmarkRandomStringsWithDeltaLengthByteArrayValuesWriter() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    Utils.writeData(writer, values);
    ByteBufferInputStream data = writer.getBytes().toInputStream();
    Utils.readData(reader, data, values.length);
    System.out.println("size " + data.position());
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void benchmarkSortedStringsWithPlainValuesWriter() throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    BinaryPlainValuesReader reader = new BinaryPlainValuesReader();

    Utils.writeData(writer, sortedVals);
    ByteBufferInputStream data = writer.getBytes().toInputStream();
    Utils.readData(reader, data, values.length);
    System.out.println("size " + data.position());
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void benchmarkSortedStringsWithDeltaLengthByteArrayValuesWriter() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    Utils.writeData(writer, sortedVals);
    ByteBufferInputStream data = writer.getBytes().toInputStream();
    Utils.readData(reader, data, values.length);
    System.out.println("size " + data.position());
  }
}
