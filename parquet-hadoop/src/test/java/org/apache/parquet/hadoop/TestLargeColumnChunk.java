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

import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ResourceIntensiveTestRule;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

/**
 * This test is to test parquet-mr working with potential int overflows (when the sizes are greater than
 * Integer.MAX_VALUE).
 */
public class TestLargeColumnChunk {
  private static final MessageType SCHEMA = buildMessage().addFields(
      required(INT64).named("id"),
      required(BINARY).named("data"))
      .named("schema");
  private static final int DATA_SIZE = 256;
  // Ensure that the size of the column chunk would overflow an int
  private static final int ROW_COUNT = Integer.MAX_VALUE / DATA_SIZE + 1000;
  private static final long RANDOM_SEED = 42;
  private static final int ID_INDEX = SCHEMA.getFieldIndex("id");
  private static final int DATA_INDEX = SCHEMA.getFieldIndex("data");

  private static final long ID_OF_FILTERED_DATA = ROW_COUNT / 2;
  private static Binary VALUE_IN_DATA;
  private static Binary VALUE_NOT_IN_DATA;
  private static Path file;

  @ClassRule
  public static TestRule maySkip = ResourceIntensiveTestRule.get();

  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void createFile() throws IOException {
    file = new Path(folder.newFile().getAbsolutePath());

    GroupFactory factory = new SimpleGroupFactory(SCHEMA);
    Random random = new Random(RANDOM_SEED);
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    try (ParquetWriter<Group> writer = ExampleParquetWriter
        .builder(HadoopOutputFile.fromPath(file, conf))
        .withWriteMode(OVERWRITE)
        .withConf(conf)
        .withCompressionCodec(UNCOMPRESSED)
        .withRowGroupSize(4L * 1024 * 1024 * 1024) // 4G to ensure all data goes to one row group
        .withBloomFilterEnabled(true)
        .build()) {
      for (long id = 0; id < ROW_COUNT; ++id) {
        Group group = factory.newGroup();
        group.add(ID_INDEX, id);
        Binary data = nextBinary(random);
        group.add(DATA_INDEX, data);
        writer.write(group);
        if (id == ID_OF_FILTERED_DATA) {
          VALUE_IN_DATA = data;
        }
      }
    }
    VALUE_NOT_IN_DATA = nextBinary(random);
  }

  private static Binary nextBinary(Random random) {
    byte[] bytes = new byte[DATA_SIZE];
    random.nextBytes(bytes);
    return Binary.fromConstantByteArray(bytes);
  }

  @Test
  public void validateAllData() throws IOException {
    Random random = new Random(RANDOM_SEED);
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).build()) {
      for (long id = 0; id < ROW_COUNT; ++id) {
        Group group = reader.read();
        assertEquals(id, group.getLong(ID_INDEX, 0));
        assertEquals(nextBinary(random), group.getBinary(DATA_INDEX, 0));
      }
      assertNull("No more record should be read", reader.read());
    }
  }

  @Test
  public void validateFiltering() throws IOException {
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withFilter(FilterCompat.get(eq(binaryColumn("data"), VALUE_IN_DATA)))
        .build()) {
      Group group = reader.read();
      assertEquals(ID_OF_FILTERED_DATA, group.getLong(ID_INDEX, 0));
      assertEquals(VALUE_IN_DATA, group.getBinary(DATA_INDEX, 0));
      assertNull("No more record should be read", reader.read());
    }
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withFilter(FilterCompat.get(eq(binaryColumn("data"), VALUE_NOT_IN_DATA)))
        .build()) {
      assertNull("No record should be read", reader.read());
    }
  }
}
