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

import static org.apache.parquet.benchmarks.BenchmarkConstants.ONE_MILLION;
import static org.apache.parquet.benchmarks.BenchmarkFiles.configuration;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_BS256M_PS4M;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_BS256M_PS8M;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_BS512M_PS4M;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_BS512M_PS8M;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_GZIP;
import static org.apache.parquet.benchmarks.BenchmarkFiles.file_1M_SNAPPY;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class ReadBenchmarks {

  private void read(Path parquetFile, int nRows, Blackhole blackhole) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), parquetFile)
        .withConf(configuration)
        .build();
    for (int i = 0; i < nRows; i++) {
      Group group = reader.read();
      blackhole.consume(group.getBinary("binary_field", 0));
      blackhole.consume(group.getInteger("int32_field", 0));
      blackhole.consume(group.getLong("int64_field", 0));
      blackhole.consume(group.getBoolean("boolean_field", 0));
      blackhole.consume(group.getFloat("float_field", 0));
      blackhole.consume(group.getDouble("double_field", 0));
      blackhole.consume(group.getBinary("flba_field", 0));
      blackhole.consume(group.getInt96("int96_field", 0));
    }
    reader.close();
  }

  /**
   * This needs to be done exactly once.  To avoid needlessly regenerating the files for reading, they aren't cleaned
   * as part of the benchmark.  If the files exist, a message will be printed and they will not be regenerated.
   */
  @Setup(Level.Trial)
  public void generateFilesForRead() {
    new DataGenerator().generateAll();
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsDefaultBlockAndPageSizeUncompressed(Blackhole blackhole) throws IOException {
    read(file_1M, ONE_MILLION, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsBS256MPS4MUncompressed(Blackhole blackhole) throws IOException {
    read(file_1M_BS256M_PS4M, ONE_MILLION, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsBS256MPS8MUncompressed(Blackhole blackhole) throws IOException {
    read(file_1M_BS256M_PS8M, ONE_MILLION, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsBS512MPS4MUncompressed(Blackhole blackhole) throws IOException {
    read(file_1M_BS512M_PS4M, ONE_MILLION, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsBS512MPS8MUncompressed(Blackhole blackhole) throws IOException {
    read(file_1M_BS512M_PS8M, ONE_MILLION, blackhole);
  }

  // TODO how to handle lzo jar?
  //  @Benchmark
  //  public void read1MRowsDefaultBlockAndPageSizeLZO(Blackhole blackhole)
  //          throws IOException
  //  {
  //    read(parquetFile_1M_LZO, ONE_MILLION, blackhole);
  //  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsDefaultBlockAndPageSizeSNAPPY(Blackhole blackhole) throws IOException {
    read(file_1M_SNAPPY, ONE_MILLION, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void read1MRowsDefaultBlockAndPageSizeGZIP(Blackhole blackhole) throws IOException {
    read(file_1M_GZIP, ONE_MILLION, blackhole);
  }
}
