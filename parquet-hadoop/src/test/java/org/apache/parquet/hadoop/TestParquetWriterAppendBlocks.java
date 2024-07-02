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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetWriterAppendBlocks {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public static final int FILE_SIZE = 10000;
  public static final Configuration CONF = new Configuration();
  public static final Map<String, String> EMPTY_METADATA = new HashMap<String, String>();
  public static final MessageType FILE_SCHEMA = Types.buildMessage()
      .required(INT32)
      .named("id")
      .required(BINARY)
      .as(UTF8)
      .named("string")
      .named("AppendTest");
  public static final SimpleGroupFactory GROUP_FACTORY = new SimpleGroupFactory(FILE_SCHEMA);

  private static final Path STATIC_FILE_1 = createPathFromCP("/test-append_1.parquet");
  private static final Path STATIC_FILE_2 = createPathFromCP("/test-append_2.parquet");

  private static Path createPathFromCP(String path) {
    try {
      return new Path(
          TestParquetWriterAppendBlocks.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public Path file1;
  public List<Group> file1content = new ArrayList<Group>();
  public Path file2;
  public List<Group> file2content = new ArrayList<Group>();

  @Before
  public void createSourceData() throws IOException {
    this.file1 = newTemp();
    this.file2 = newTemp();

    ParquetWriter<Group> writer1 =
        ExampleParquetWriter.builder(file1).withType(FILE_SCHEMA).build();
    ParquetWriter<Group> writer2 =
        ExampleParquetWriter.builder(file2).withType(FILE_SCHEMA).build();

    for (int i = 0; i < FILE_SIZE; i += 1) {
      Group group1 = GROUP_FACTORY.newGroup();
      group1.add("id", i);
      group1.add("string", UUID.randomUUID().toString());
      writer1.write(group1);
      file1content.add(group1);

      Group group2 = GROUP_FACTORY.newGroup();
      group2.add("id", FILE_SIZE + i);
      group2.add("string", UUID.randomUUID().toString());
      writer2.write(group2);
      file2content.add(group2);
    }

    writer1.close();
    writer2.close();
  }

  @Test
  public void testBasicBehavior() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, FILE_SCHEMA, combinedFile);
    writer.start();
    writer.appendFile(CONF, file1);
    writer.appendFile(CONF, file2);
    writer.end(EMPTY_METADATA);

    LinkedList<Group> expected = new LinkedList<Group>();
    expected.addAll(file1content);
    expected.addAll(file2content);

    ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), combinedFile).build();

    Group next;
    while ((next = reader.read()) != null) {
      Group expectedNext = expected.removeFirst();
      // check each value; equals is not supported for simple records
      Assert.assertEquals("Each id should match", expectedNext.getInteger("id", 0), next.getInteger("id", 0));
      Assert.assertEquals(
          "Each string should match", expectedNext.getString("string", 0), next.getString("string", 0));
    }

    Assert.assertEquals("All records should be present", 0, expected.size());
  }

  /**
   * This test is similar to {@link #testBasicBehavior()} only that it uses static files generated by a previous release
   * (1.11.1). This test is to validate the fix of PARQUET-2027.
   */
  @Test
  public void testBasicBehaviorWithStaticFiles() throws IOException {
    List<Group> expected = new ArrayList<>();
    readAll(STATIC_FILE_1, expected);
    readAll(STATIC_FILE_2, expected);

    Path combinedFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, FILE_SCHEMA, combinedFile);
    writer.start();
    writer.appendFile(CONF, STATIC_FILE_1);
    writer.appendFile(CONF, STATIC_FILE_2);
    writer.end(EMPTY_METADATA);

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), combinedFile).build()) {

      for (Group expectedNext : expected) {
        Group next = reader.read();
        // check each value; equals is not supported for simple records
        Assert.assertEquals("Each id should match", expectedNext.getInteger("id", 0), next.getInteger("id", 0));
        Assert.assertEquals(
            "Each string should match", expectedNext.getString("string", 0), next.getString("string", 0));
      }
      Assert.assertNull("No extra records should be present", reader.read());
    }
  }

  private void readAll(Path file, List<Group> values) throws IOException {
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      for (Group g = reader.read(); g != null; g = reader.read()) {
        values.add(g);
      }
    }
  }

  @Test
  public void testMergedMetadata() throws IOException {
    Path combinedFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, FILE_SCHEMA, combinedFile);
    writer.start();
    writer.appendFile(CONF, file1);
    writer.appendFile(CONF, file2);
    writer.end(EMPTY_METADATA);

    ParquetMetadata combinedFooter = ParquetFileReader.readFooter(CONF, combinedFile, NO_FILTER);
    ParquetMetadata f1Footer = ParquetFileReader.readFooter(CONF, file1, NO_FILTER);
    ParquetMetadata f2Footer = ParquetFileReader.readFooter(CONF, file2, NO_FILTER);

    LinkedList<BlockMetaData> expectedRowGroups = new LinkedList<BlockMetaData>();
    expectedRowGroups.addAll(f1Footer.getBlocks());
    expectedRowGroups.addAll(f2Footer.getBlocks());

    Assert.assertEquals(
        "Combined should have the right number of row groups",
        expectedRowGroups.size(),
        combinedFooter.getBlocks().size());

    long nextStart = 4;
    for (BlockMetaData rowGroup : combinedFooter.getBlocks()) {
      BlockMetaData expected = expectedRowGroups.removeFirst();
      Assert.assertEquals("Row count should match", expected.getRowCount(), rowGroup.getRowCount());
      Assert.assertEquals(
          "Compressed size should match", expected.getCompressedSize(), rowGroup.getCompressedSize());
      Assert.assertEquals("Total size should match", expected.getTotalByteSize(), rowGroup.getTotalByteSize());
      Assert.assertEquals(
          "Start pos should be at the last row group's end", nextStart, rowGroup.getStartingPos());
      assertColumnsEquivalent(expected.getColumns(), rowGroup.getColumns());
      nextStart = rowGroup.getStartingPos() + rowGroup.getTotalByteSize();
    }
  }

  public void assertColumnsEquivalent(List<ColumnChunkMetaData> expected, List<ColumnChunkMetaData> actual) {
    Assert.assertEquals("Should have the expected columns", expected.size(), actual.size());
    for (int i = 0; i < actual.size(); i += 1) {
      ColumnChunkMetaData current = actual.get(i);
      if (i != 0) {
        ColumnChunkMetaData previous = actual.get(i - 1);
        long expectedStart = previous.getStartingPos() + previous.getTotalSize();
        Assert.assertEquals("Should start after the previous column", expectedStart, current.getStartingPos());
      }

      assertColumnMetadataEquivalent(expected.get(i), current);
    }
  }

  public void assertColumnMetadataEquivalent(ColumnChunkMetaData expected, ColumnChunkMetaData actual) {
    Assert.assertEquals("Should be the expected column", expected.getPath(), expected.getPath());
    Assert.assertEquals("Primitive type should not change", expected.getType(), actual.getType());
    Assert.assertEquals("Compression codec should not change", expected.getCodec(), actual.getCodec());
    Assert.assertEquals("Data encodings should not change", expected.getEncodings(), actual.getEncodings());
    Assert.assertEquals("Statistics should not change", expected.getStatistics(), actual.getStatistics());
    Assert.assertEquals(
        "Uncompressed size should not change",
        expected.getTotalUncompressedSize(),
        actual.getTotalUncompressedSize());
    Assert.assertEquals("Compressed size should not change", expected.getTotalSize(), actual.getTotalSize());
    Assert.assertEquals("Number of values should not change", expected.getValueCount(), actual.getValueCount());
  }

  @Test
  public void testAllowDroppingColumns() throws IOException {
    MessageType droppedColumnSchema =
        Types.buildMessage().required(BINARY).as(UTF8).named("string").named("AppendTest");

    Path droppedColumnFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, droppedColumnSchema, droppedColumnFile);
    writer.start();
    writer.appendFile(CONF, file1);
    writer.appendFile(CONF, file2);
    writer.end(EMPTY_METADATA);

    LinkedList<Group> expected = new LinkedList<Group>();
    expected.addAll(file1content);
    expected.addAll(file2content);

    ParquetMetadata footer = ParquetFileReader.readFooter(CONF, droppedColumnFile, NO_FILTER);
    for (BlockMetaData rowGroup : footer.getBlocks()) {
      Assert.assertEquals(
          "Should have only the string column",
          1,
          rowGroup.getColumns().size());
    }

    ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), droppedColumnFile).build();

    Group next;
    while ((next = reader.read()) != null) {
      Group expectedNext = expected.removeFirst();
      Assert.assertEquals(
          "Each string should match", expectedNext.getString("string", 0), next.getString("string", 0));
    }

    Assert.assertEquals("All records should be present", 0, expected.size());
  }

  @Test
  public void testFailDroppingColumns() throws IOException {
    MessageType droppedColumnSchema =
        Types.buildMessage().required(BINARY).as(UTF8).named("string").named("AppendTest");

    final ParquetMetadata footer = ParquetFileReader.readFooter(CONF, file1, NO_FILTER);
    final FSDataInputStream incoming = file1.getFileSystem(CONF).open(file1);

    Path droppedColumnFile = newTemp();
    final ParquetFileWriter writer = new ParquetFileWriter(CONF, droppedColumnSchema, droppedColumnFile);
    writer.start();

    TestUtils.assertThrows(
        "Should complain that id column is dropped", IllegalArgumentException.class, (Callable<Void>) () -> {
          writer.appendRowGroups(incoming, footer.getBlocks(), false);
          return null;
        });
  }

  @Test
  public void testFailMissingColumn() throws IOException {
    MessageType fileSchema = Types.buildMessage()
        .required(INT32)
        .named("id")
        .required(BINARY)
        .as(UTF8)
        .named("string")
        .required(FLOAT)
        .named("value")
        .named("AppendTest");

    Path missingColumnFile = newTemp();
    final ParquetFileWriter writer = new ParquetFileWriter(CONF, fileSchema, missingColumnFile);
    writer.start();

    TestUtils.assertThrows(
        "Should complain that value column is missing", IllegalArgumentException.class, (Callable<Void>) () -> {
          writer.appendFile(CONF, file1);
          return null;
        });
  }

  @Test
  public void testCloseParquetFileReaderOpenByWriter() throws IOException {
    Path file1 = newTemp();
    Path file2 = newTemp();

    ParquetWriter<Group> writer1 =
        ExampleParquetWriter.builder(file1).withType(FILE_SCHEMA).build();
    ParquetWriter<Group> writer2 =
        ExampleParquetWriter.builder(file2).withType(FILE_SCHEMA).build();

    for (int i = 0; i < FILE_SIZE; i += 1) {
      Group group1 = GROUP_FACTORY.newGroup();
      group1.add("id", i);
      group1.add("string", UUID.randomUUID().toString());
      writer1.write(group1);

      Group group2 = GROUP_FACTORY.newGroup();
      group2.add("id", FILE_SIZE + i);
      group2.add("string", UUID.randomUUID().toString());
      writer2.write(group2);
    }

    writer1.close();
    writer2.close();

    MessageType droppedColumnSchema =
        Types.buildMessage().required(BINARY).as(UTF8).named("string").named("AppendTest");

    Path droppedColumnFile = newTemp();
    ParquetFileWriter writer = new ParquetFileWriter(CONF, droppedColumnSchema, droppedColumnFile);
    writer.start();
    writer.appendFile(CONF, file1);
    writer.appendFile(CONF, file2);
    writer.end(EMPTY_METADATA);
    FileSystem fs = FileSystem.get(new Configuration());
    Assert.assertTrue(fs.delete(file1));
    Assert.assertTrue(fs.delete(file2));
  }

  private Path newTemp() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.toString());
  }
}
