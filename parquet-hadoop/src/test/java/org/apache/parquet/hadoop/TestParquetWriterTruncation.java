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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.assertEquals;

public class TestParquetWriterTruncation {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testTruncateColumnIndex() throws IOException {
    MessageType schema = Types.buildMessage().
      required(BINARY).as(stringType()).named("name").named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
      .withPageRowCountLimit(10)
      .withConf(conf)
      .withDictionaryEncoding(false)
      .withColumnIndexTruncateLength(10)
      .build()) {

      writer.write(factory.newGroup().append("name", "1234567890abcdefghijklmnopqrstuvwxyz"));
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {

      ColumnChunkMetaData column = reader.getFooter().getBlocks().get(0).getColumns().get(0);
      ColumnIndex index = reader.readColumnIndex(column);
      assertEquals(Collections.singletonList("1234567890"), asStrings(index.getMinValues()));
      assertEquals(Collections.singletonList("1234567891"), asStrings(index.getMaxValues()));
    }
  }

  @Test
  public void testTruncateStatistics() throws IOException {
    MessageType schema = Types.buildMessage().
      required(BINARY).as(stringType()).named("name").named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    GroupFactory factory = new SimpleGroupFactory(schema);
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
      .withPageRowCountLimit(10)
      .withConf(conf)
      .withDictionaryEncoding(false)
      .withStatisticsTruncateLength(10)
      .build()) {

      writer.write(factory.newGroup().append("name", "1234567890abcdefghijklmnopqrstuvwxyz"));
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {

      ColumnChunkMetaData column = reader.getFooter().getBlocks().get(0).getColumns().get(0);
      Statistics<?> statistics = column.getStatistics();
      assertEquals("1234567890", new String(statistics.getMinBytes()));
      assertEquals("1234567891", new String(statistics.getMaxBytes()));
    }
  }

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }

  private static List<String> asStrings(List<ByteBuffer> buffers) {
    return buffers.stream()
      .map(buffer -> new String(buffer.array()))
      .collect(Collectors.toList());
  }

}
