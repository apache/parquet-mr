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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Mode.SingleShotTime;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmarks related to the different filtering options (e.g. w or w/o column index).
 * <p>
 * To execute this benchmark a jar file shall be created of this module. Then the jar file can be executed using the JMH
 * framework.<br>
 * The following one-liner (shall be executed in the parquet-benchmarks submodule) generates result statistics in the
 * file {@code jmh-result.json}. This json might be visualized by using the tool at
 * <a href="https://jmh.morethan.io">https://jmh.morethan.io</a>.
 *
 * <pre>
 * mvn clean package &amp;&amp; java -jar target/parquet-benchmarks.jar org.apache.parquet.benchmarks.FilteringBenchmarks -rf json
 * </pre>
 */
@BenchmarkMode(SingleShotTime)
@Fork(1)
@Warmup(iterations = 10, batchSize = 1)
@Measurement(iterations = 50, batchSize = 1)
@OutputTimeUnit(MILLISECONDS)
public class FilteringBenchmarks {
  private static final int RECORD_COUNT = 2_000_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(FilteringBenchmarks.class);

  /*
   * For logging human readable file size
   */
  private static class FileSize {
    private static final String[] SUFFIXES = {"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
    private final Path file;

    FileSize(Path file) {
      this.file = file;
    }

    @Override
    public String toString() {
      try {
        FileSystem fs = file.getFileSystem(new Configuration());
        long bytes = fs.getFileStatus(file).getLen();
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        if (exp == 0) {
          return Long.toString(bytes);
        }
        String suffix = SUFFIXES[exp - 1];
        return String.format("%d [%.2f%s]", bytes, bytes / Math.pow(1024, exp), suffix);
      } catch (IOException e) {
        return "N/A";
      }
    }
  }

  /*
   * For generating binary values
   */
  private static class StringGenerator {
    private static final int MAX_LENGTH = 100;
    private static final int MIN_LENGTH = 50;
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ";
    private final Random random = new Random(44);

    String nextString() {
      char[] str = new char[MIN_LENGTH + random.nextInt(MAX_LENGTH - MIN_LENGTH)];
      for (int i = 0, n = str.length; i < n; ++i) {
        str[i] = ALPHABET.charAt(random.nextInt(ALPHABET.length()));
      }
      return new String(str);
    }
  }

  interface ReadConfigurator {
    static ReadConfigurator DEFAULT = new ReadConfigurator() {
      @Override
      public <T> Builder<T> configureBuilder(Builder<T> builder) {
        return builder;
      }

      @Override
      public String toString() {
        return "DEFAULT";
      }
    };

    <T> ParquetReader.Builder<T> configureBuilder(ParquetReader.Builder<T> builder);
  }

  interface WriteConfigurator {
    static WriteConfigurator DEFAULT = new WriteConfigurator() {
      @Override
      public <T> org.apache.parquet.hadoop.ParquetWriter.Builder<T, ?> configureBuilder(
          org.apache.parquet.hadoop.ParquetWriter.Builder<T, ?> builder) {
        return builder;
      }

      @Override
      public String toString() {
        return "DEFAULT";
      }
    };

    <T> ParquetWriter.Builder<T, ?> configureBuilder(ParquetWriter.Builder<T, ?> builder);
  }

  public static enum ColumnIndexUsage implements ReadConfigurator {
    WITHOUT_COLUMN_INDEX {
      @Override
      public <T> Builder<T> configureBuilder(Builder<T> builder) {
        return builder.useColumnIndexFilter(false);
      }
    },
    WITH_COLUMN_INDEX {
      @Override
      public <T> Builder<T> configureBuilder(Builder<T> builder) {
        return builder.useColumnIndexFilter(true);
      }
    };
  }

  public static enum ColumnCharacteristic {
    SORTED {
      @Override
      void arrangeData(long[] data) {
        Arrays.parallelSort(data);
      }
    },
    CLUSTERED_9 {
      @Override
      void arrangeData(long[] data) {
        arrangeToBuckets(data, 9);
      }
    },
    CLUSTERED_5 {
      @Override
      void arrangeData(long[] data) {
        arrangeToBuckets(data, 5);
      }
    },
    CLUSTERED_3 {
      @Override
      void arrangeData(long[] data) {
        arrangeToBuckets(data, 3);
      }
    },
    RANDOM {
      @Override
      void arrangeData(long[] data) {
        // Nothing to do
      }
    };

    abstract void arrangeData(long[] data);

    public static void arrangeToBuckets(long[] data, int bucketCnt) {
      long bucketSize = (long) (Long.MAX_VALUE / (bucketCnt / 2.0));
      long bucketBorders[] = new long[bucketCnt - 1];
      for (int i = 0, n = bucketBorders.length; i < n; ++i) {
        bucketBorders[i] = Long.MIN_VALUE + (i + 1) * bucketSize;
      }
      LongList[] buckets = new LongList[bucketCnt];
      for (int i = 0; i < bucketCnt; ++i) {
        buckets[i] = new LongArrayList(data.length / bucketCnt);
      }
      for (int i = 0, n = data.length; i < n; ++i) {
        long value = data[i];
        int bucket = Arrays.binarySearch(bucketBorders, value);
        if (bucket < 0) {
          bucket = -(bucket + 1);
        }
        buckets[bucket].add(value);
      }
      int offset = 0;
      int mid = bucketCnt / 2;
      for (int i = 0; i < bucketCnt; ++i) {
        int bucketIndex;
        if (i % 2 == 0) {
          bucketIndex = mid + i / 2;
        } else {
          bucketIndex = mid - i / 2 - 1;
        }
        LongList bucket = buckets[bucketIndex];
        bucket.getElements(0, data, offset, bucket.size());
        offset += bucket.size();
      }
    }
  }

  public enum PageRowLimit implements WriteConfigurator {
    PAGE_ROW_COUNT_1K {
      @Override
      public <T> ParquetWriter.Builder<T, ?> configureBuilder(ParquetWriter.Builder<T, ?> builder) {
        return builder.withPageSize(
                Integer.MAX_VALUE) // Ensure that only the row count limit takes into account
            .withPageRowCountLimit(1_000);
      }
    },
    PAGE_ROW_COUNT_10K {
      @Override
      public <T> ParquetWriter.Builder<T, ?> configureBuilder(ParquetWriter.Builder<T, ?> builder) {
        return builder.withPageSize(
                Integer.MAX_VALUE) // Ensure that only the row count limit takes into account
            .withPageRowCountLimit(10_000);
      }
    },
    PAGE_ROW_COUNT_50K {
      @Override
      public <T> ParquetWriter.Builder<T, ?> configureBuilder(ParquetWriter.Builder<T, ?> builder) {
        return builder.withPageSize(
                Integer.MAX_VALUE) // Ensure that only the row count limit takes into account
            .withPageRowCountLimit(50_000);
      }
    },
    PAGE_ROW_COUNT_100K {
      @Override
      public <T> ParquetWriter.Builder<T, ?> configureBuilder(ParquetWriter.Builder<T, ?> builder) {
        return builder.withPageSize(
                Integer.MAX_VALUE) // Ensure that only the row count limit takes into account
            .withPageRowCountLimit(100_000);
      }
    };
  }

  @State(Benchmark)
  public abstract static class BaseContext {
    private static final MessageType SCHEMA = Types.buildMessage()
        .required(INT64)
        .named("int64_col")
        .required(BINARY)
        .as(stringType())
        .named("dummy1_col")
        .required(BINARY)
        .as(stringType())
        .named("dummy2_col")
        .required(BINARY)
        .as(stringType())
        .named("dummy3_col")
        .required(BINARY)
        .as(stringType())
        .named("dummy4_col")
        .required(BINARY)
        .as(stringType())
        .named("dummy5_col")
        .named("schema");
    public static final LongColumn COLUMN = FilterApi.longColumn("int64_col");
    private Path file;
    private Random random;
    private StringGenerator dummyGenerator;

    @Param
    private ColumnCharacteristic characteristic;

    @Setup
    public void writeFile() throws IOException {
      WriteConfigurator writeConfigurator = getWriteConfigurator();
      file = new Path(Files.createTempFile(
              "benchmark-filtering_" + characteristic + '_' + writeConfigurator + '_', ".parquet")
          .toAbsolutePath()
          .toString());
      long[] data = generateData();
      characteristic.arrangeData(data);
      try (ParquetWriter<Group> writer = writeConfigurator
          .configureBuilder(ExampleParquetWriter.builder(file)
              .config(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, SCHEMA.toString())
              .withRowGroupSize(Integer.MAX_VALUE) // Ensure to have one row-group per file only
              .withWriteMode(OVERWRITE))
          .build()) {
        for (long value : data) {
          Group group = new SimpleGroup(SCHEMA);
          group.add(0, value);
          group.add(1, Binary.fromString(dummyGenerator.nextString()));
          group.add(2, Binary.fromString(dummyGenerator.nextString()));
          group.add(3, Binary.fromString(dummyGenerator.nextString()));
          group.add(4, Binary.fromString(dummyGenerator.nextString()));
          group.add(5, Binary.fromString(dummyGenerator.nextString()));
          writer.write(group);
        }
      }
    }

    WriteConfigurator getWriteConfigurator() {
      return WriteConfigurator.DEFAULT;
    }

    ReadConfigurator getReadConfigurator() {
      return ReadConfigurator.DEFAULT;
    }

    private long[] generateData() {
      Random random = new Random(43);
      long[] data = new long[RECORD_COUNT];
      for (int i = 0, n = data.length; i < n; ++i) {
        data[i] = random.nextLong();
      }
      return data;
    }

    // Resetting the random so every measurement would use the same sequence
    @Setup
    public void resetRandom() {
      random = new Random(42);
      dummyGenerator = new StringGenerator();
    }

    @Setup(Invocation)
    public void gc() {
      System.gc();
    }

    @TearDown
    public void deleteFile() throws IOException {
      LOGGER.info("Deleting file {} (size: {})", file, new FileSize(file));
      file.getFileSystem(new Configuration()).delete(file, false);
    }

    public ParquetReader.Builder<Group> createReaderBuilder() throws IOException {
      ReadConfigurator readConfigurator = getReadConfigurator();
      return readConfigurator.configureBuilder(
          new ParquetReader.Builder<Group>(HadoopInputFile.fromPath(file, new Configuration())) {
            @Override
            protected ReadSupport<Group> getReadSupport() {
              return new GroupReadSupport();
            }
          }.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, SCHEMA.toString()));
    }

    public Random getRandom() {
      return random;
    }
  }

  @State(Benchmark)
  public static class WithOrWithoutColumnIndexContext extends BaseContext {
    @Param
    private ColumnIndexUsage columnIndexUsage;

    @Override
    ReadConfigurator getReadConfigurator() {
      return columnIndexUsage;
    }
  }

  @State(Benchmark)
  public static class PageSizeContext extends BaseContext {
    @Param
    private PageRowLimit pageRowLimit;

    @Override
    WriteConfigurator getWriteConfigurator() {
      return pageRowLimit;
    }

    @Override
    ReadConfigurator getReadConfigurator() {
      return ColumnIndexUsage.WITH_COLUMN_INDEX;
    }
  }

  @Benchmark
  public void benchmarkWithOrWithoutColumnIndex(Blackhole blackhole, WithOrWithoutColumnIndexContext context)
      throws Exception {
    benchmark(blackhole, context);
  }

  @Benchmark
  public void benchmarkPageSize(Blackhole blackhole, PageSizeContext context) throws Exception {
    benchmark(blackhole, context);
  }

  private void benchmark(Blackhole blackhole, BaseContext context) throws Exception {
    FilterPredicate filter =
        FilterApi.eq(BaseContext.COLUMN, context.getRandom().nextLong());
    try (ParquetReader<Group> reader = context.createReaderBuilder()
        .withFilter(FilterCompat.get(filter))
        .build()) {
      blackhole.consume(reader.read());
    }
  }
}
