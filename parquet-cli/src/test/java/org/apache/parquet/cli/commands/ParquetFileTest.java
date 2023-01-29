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
package org.apache.parquet.cli.commands;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.LogicalTypes;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.parquet.schema.Type.Repetition.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public abstract class ParquetFileTest extends FileTest {

  private Random rnd = new Random();

  @Before
  public void setUp() throws IOException {
    createTestParquetFile();
  }

  protected File parquetFile() {
    File tmpDir = getTempFolder();
    return new File(tmpDir, getClass().getSimpleName() + ".parquet");
  }

  protected File randomParquetFile() {
    File tmpDir = getTempFolder();
    return new File(tmpDir, getClass().getSimpleName() + rnd.nextLong() + ".parquet");
  }

  private static MessageType createSchema() {
    return Types.buildMessage()
      .required(PrimitiveTypeName.INT32).named(INT32_FIELD)
      .required(PrimitiveTypeName.INT64).named(INT64_FIELD)
      .required(PrimitiveTypeName.FLOAT).named(FLOAT_FIELD)
      .required(PrimitiveTypeName.DOUBLE).named(DOUBLE_FIELD)
      .required(PrimitiveTypeName.BINARY).named(BINARY_FIELD)
      .required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(12)
        .named(FIXED_LEN_BYTE_ARRAY_FIELD)
      .required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType())
        .named(DATE_FIELD)
      .named("schema");
  }

  private void createTestParquetFile() throws IOException {
    File file = parquetFile();
    Path fsPath = new Path(file.getPath());
    Configuration conf = new Configuration();

    MessageType schema = createSchema();
    SimpleGroupFactory fact = new SimpleGroupFactory(schema);
    GroupWriteSupport.setSchema(schema, conf);

    try (
      ParquetWriter<Group> writer = new ParquetWriter<>(
        fsPath,
        new GroupWriteSupport(),
        CompressionCodecName.UNCOMPRESSED,
        1024,
        1024,
        512,
        true,
        false,
        ParquetProperties.WriterVersion.PARQUET_2_0,
        conf)) {
      for (int i = 0; i < 10; i++) {
        final byte[] bytes = new byte[12];
        ThreadLocalRandom.current().nextBytes(bytes);

        writer.write(fact.newGroup()
         .append(INT32_FIELD, 32 + i)
         .append(INT64_FIELD, 64L + i)
         .append(FLOAT_FIELD, 1.0f + i)
         .append(DOUBLE_FIELD, 2.0d + i)
         .append(BINARY_FIELD, Binary.fromString(COLORS[i % COLORS.length]))
         .append(FIXED_LEN_BYTE_ARRAY_FIELD,
           Binary.fromConstantByteArray(bytes))
         .append(DATE_FIELD, i));
      }
    }
  }
}
