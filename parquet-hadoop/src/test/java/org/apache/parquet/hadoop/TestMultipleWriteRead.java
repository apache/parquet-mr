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

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.io.api.Binary.fromString;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;

import com.google.common.io.Files;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests writing/reading multiple files in the same time (using multiple threads). Readers/writers do not support
 * concurrency but the API shall support using separate reader/writer instances to read/write parquet files in different
 * threads. (Of course, simultaneous writing to the same file is not supported.)
 */
public class TestMultipleWriteRead {
  private static final MessageType SCHEMA = Types.buildMessage()
      .required(INT32)
      .named("id")
      .required(BINARY)
      .as(stringType())
      .named("name")
      .requiredList()
      .requiredElement(INT64)
      .as(intType(64, false))
      .named("phone_numbers")
      .optional(BINARY)
      .as(stringType())
      .named("comment")
      .named("msg");
  private static final Comparator<Binary> BINARY_COMPARATOR =
      Types.required(BINARY).as(stringType()).named("dummy").comparator();

  private static class DataGenerator implements Supplier<Group> {
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz -";
    private static final int NAME_MIN_SIZE = 5;
    private static final int NAME_MAX_SIZE = 30;
    private static final int PHONE_NUMBERS_MAX_SIZE = 5;
    private static final long MIN_PHONE_NUMBER = 36_1_000_000;
    private static final long MAX_PHONE_NUMBER = 36_1_999_999;
    private static final double COMMENT_NULL_RATIO = 0.3;
    private static final int COMMENT_MAX_SIZE = 200;

    private final Random random;
    private final GroupFactory factory = new SimpleGroupFactory(SCHEMA);

    DataGenerator(long seed) {
      random = new Random(seed);
    }

    private String getString(int minSize, int maxSize) {
      int size = random.nextInt(maxSize - minSize) + minSize;
      StringBuilder builder = new StringBuilder(size);
      for (int i = 0; i < size; ++i) {
        builder.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
      }
      return builder.toString();
    }

    @Override
    public Group get() {
      Group group = factory.newGroup();
      group.add("id", random.nextInt());
      group.add("name", getString(NAME_MIN_SIZE, NAME_MAX_SIZE));
      Group phoneNumbers = group.addGroup("phone_numbers");
      for (int i = 0, n = random.nextInt(PHONE_NUMBERS_MAX_SIZE); i < n; ++i) {
        Group phoneNumber = phoneNumbers.addGroup(0);
        phoneNumber.add(0, random.nextLong() % (MAX_PHONE_NUMBER - MIN_PHONE_NUMBER) + MIN_PHONE_NUMBER);
      }
      if (random.nextDouble() >= COMMENT_NULL_RATIO) {
        group.add("comment", getString(0, COMMENT_MAX_SIZE));
      }
      return group;
    }
  }

  private static Path tmpDir;
  private Configuration conf;

  @BeforeClass
  public static void createTmpDir() {
    tmpDir = new Path(Files.createTempDir().getAbsolutePath().toString());
  }

  @AfterClass
  public static void deleteTmpDir() throws IOException {
    tmpDir.getFileSystem(new Configuration()).delete(tmpDir, true);
  }

  @Before
  public void initConfiguration() {
    this.conf = new Configuration();
  }

  private Path writeFile(Iterable<Group> data) throws IOException {
    Path file = new Path(tmpDir, "testMultipleReadWrite_" + UUID.randomUUID() + ".parquet");
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .config(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, SCHEMA.toString())
        .build()) {
      for (Group group : data) {
        writer.write(group);
      }
    }
    return file;
  }

  private void validateFile(Path file, List<Group> data) throws IOException {
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf)
        .build()) {
      for (Group group : data) {
        assertEquals(group.toString(), reader.read().toString());
      }
    }
  }

  private void validateFile(Path file, Filter filter, Stream<Group> data) throws IOException {
    ExecutorService parquetIOThreadPool = Executors.newFixedThreadPool(4);
    ExecutorService parquetProcessThreadPool = Executors.newFixedThreadPool(4);

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf)
        .withFilter(filter)
        .withIOThreadPool(parquetIOThreadPool)
        .withProcessThreadPool(parquetProcessThreadPool)
        .build()) {
      for (Iterator<Group> it = data.iterator(); it.hasNext(); ) {
        assertEquals(it.next().toString(), reader.read().toString());
      }
    } finally {
      parquetProcessThreadPool.shutdown();
      parquetIOThreadPool.shutdown();
    }
  }

  private void validateFileWithIdFilter(Path file, List<Group> data) throws IOException {
    validateFile(
        file,
        FilterCompat.get(eq(intColumn("id"), 0)),
        data.stream().filter(group -> group.getInteger("id", 0) == 0));
  }

  private void validateFileWithCommentFilter(Path file, List<Group> data) throws IOException {
    validateFile(
        file,
        FilterCompat.get(eq(binaryColumn("comment"), null)),
        data.stream().filter(group -> group.getFieldRepetitionCount("comment") == 0));
  }

  private void validateFileWithComplexFilter(Path file, List<Group> data) throws IOException {
    Binary binaryValueB = fromString("b");
    Filter filter = FilterCompat.get(and(
        gtEq(intColumn("id"), 0),
        and(lt(binaryColumn("name"), binaryValueB), notEq(binaryColumn("comment"), null))));
    Predicate<Group> predicate = group -> group.getInteger("id", 0) >= 0
        && BINARY_COMPARATOR.compare(group.getBinary("name", 0), binaryValueB) < 0
        && group.getFieldRepetitionCount("comment") > 0;
    validateFile(file, filter, data.stream().filter(predicate));
  }

  public void runReadWriteTest() throws Throwable {
    // 10 random datasets with row counts 10000 to 1000
    List<List<Group>> data = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      data.add(Stream.generate(new DataGenerator(i))
          .limit(10000 - i * 1000)
          .collect(Collectors.toList()));
    }

    // Writes (and reads back the data to validate) the random values using 6 threads
    List<Future<Path>> futureFiles = new ArrayList<>();
    ExecutorService exec = Executors.newFixedThreadPool(6);
    for (List<Group> d : data) {
      futureFiles.add(exec.submit(() -> {
        Path file = writeFile(d);
        validateFile(file, d);
        return file;
      }));
    }
    List<Path> files = new ArrayList<>();
    for (Future<Path> future : futureFiles) {
      try {
        files.add(future.get());
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }

    // Executes 3 filterings on each files using 6 threads
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      Path file = files.get(i);
      List<Group> d = data.get(i);
      futures.add(exec.submit(() -> {
        validateFileWithIdFilter(file, d);
        return null;
      }));
      futures.add(exec.submit(() -> {
        validateFileWithCommentFilter(file, d);
        return null;
      }));
      futures.add(exec.submit(() -> {
        validateFileWithComplexFilter(file, d);
        return null;
      }));
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  @Test
  public void testWriteRead() throws Throwable {
    runReadWriteTest();
  }

  @Test
  public void testWriteReadAsync() throws Throwable {
    conf.set("parquet.read.async.io.enabled", Boolean.toString(true));
    conf.set("parquet.read.parallel.columnreader.enabled", Boolean.toString(false));
    runReadWriteTest();
    conf.set("parquet.read.async.io.enabled", Boolean.toString(false));
    conf.set("parquet.read.parallel.columnreader.enabled", Boolean.toString(true));
    runReadWriteTest();
    conf.set("parquet.read.async.io.enabled", Boolean.toString(true));
    conf.set("parquet.read.parallel.columnreader.enabled", Boolean.toString(true));
    runReadWriteTest();
  }
}
