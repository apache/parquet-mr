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

import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.annotations.*;

import static org.apache.parquet.column.ParquetProperties.WriterVersion;
import static org.openjdk.jmh.annotations.Scope.Benchmark;
import static org.apache.parquet.benchmarks.BenchmarkConstants.*;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;

import java.io.IOException;

import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

@State(Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
public class WriteBenchmarks {
  private DataGenerator dataGenerator;

  @Param("v2")
  public String parquetVersion;

  @Param("false")
  public boolean randomData;

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    dataGenerator = new DataGenerator(new Configuration(), WriterVersion.fromString(parquetVersion));

    if (randomData) {
      dataGenerator.initializeRandomSampleData(RANDOM_SAMPLE_ROWS);
    }
  }

  @Setup(Level.Invocation)
  public void cleanup() {
    //clean existing test data at the beginning of each iteration
    dataGenerator.cleanup();
  }

  @Benchmark
  public void write1MRowsDefaultBlockAndPageSizeUncompressed()
          throws IOException
  {
    dataGenerator.generateData(file_1M,
                               BLOCK_SIZE_DEFAULT,
                               PAGE_SIZE_DEFAULT,
                               UNCOMPRESSED,
                               ONE_MILLION);
  }

  @Benchmark
  public void write1MRowsBS256MPS4MUncompressed()
          throws IOException
  {
    dataGenerator.generateData(file_1M_BS256M_PS4M,
                               BLOCK_SIZE_256M,
                               PAGE_SIZE_4M,
                               UNCOMPRESSED,
                               ONE_MILLION);
  }

  @Benchmark
  public void write1MRowsBS256MPS8MUncompressed()
          throws IOException
  {
    dataGenerator.generateData(file_1M_BS256M_PS8M,
                               BLOCK_SIZE_256M,
                               PAGE_SIZE_8M,
                               UNCOMPRESSED,
                               ONE_MILLION);
  }

  @Benchmark
  public void write1MRowsBS512MPS4MUncompressed()
          throws IOException
  {
    dataGenerator.generateData(file_1M_BS512M_PS4M,
                               BLOCK_SIZE_512M,
                               PAGE_SIZE_4M,
                               UNCOMPRESSED,
                               ONE_MILLION);
  }

  @Benchmark
  public void write1MRowsBS512MPS8MUncompressed()
          throws IOException
  {
    dataGenerator.generateData(file_1M_BS512M_PS8M,
                               BLOCK_SIZE_512M,
                               PAGE_SIZE_8M,
                               UNCOMPRESSED,
                               ONE_MILLION);
  }

  //TODO how to handle lzo jar?
//  @Benchmark
//  public void write1MRowsDefaultBlockAndPageSizeLZO()
//          throws IOException
//  {
//    dataGenerator.generateData(parquetFile_1M_LZO,
//            BLOCK_SIZE_DEFAULT,
//            PAGE_SIZE_DEFAULT,
//            LZO,
//            ONE_MILLION);
//  }

  @Benchmark
  public void write1MRowsDefaultBlockAndPageSizeSNAPPY()
          throws IOException
  {
    dataGenerator.generateData(file_1M_SNAPPY,
                               BLOCK_SIZE_DEFAULT,
                               PAGE_SIZE_DEFAULT,
                               SNAPPY,
                               ONE_MILLION);
  }

  @Benchmark
  public void write1MRowsDefaultBlockAndPageSizeGZIP()
          throws IOException
  {
    dataGenerator.generateData(file_1M_GZIP,
                               BLOCK_SIZE_DEFAULT,
                               PAGE_SIZE_DEFAULT,
                               GZIP,
                               ONE_MILLION);
  }
}
