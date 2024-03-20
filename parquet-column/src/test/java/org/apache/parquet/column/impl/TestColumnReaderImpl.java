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
package org.apache.parquet.column.impl;

import static junit.framework.Assert.assertEquals;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

import java.util.List;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.mem.MemPageReader;
import org.apache.parquet.column.page.mem.MemPageWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

public class TestColumnReaderImpl {

  private int rows = 13001;

  private static final class ValidatingConverter extends PrimitiveConverter {
    int count;

    @Override
    public void addBinary(Binary value) {
      assertEquals("bar" + count % 10, value.toStringUsingUTF8());
      ++count;
    }
  }

  @Test
  public void test() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary foo; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 columnWriterV2 = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withDictionaryPageSize(1024)
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(2048)
            .build());
    for (int i = 0; i < rows; i++) {
      columnWriterV2.write(Binary.fromString("bar" + i % 10), 0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage();
      }
    }
    columnWriterV2.writePage();
    columnWriterV2.finalizeColumnChunk();
    List<DataPage> pages = pageWriter.getPages();
    int valueCount = 0;
    int rowCount = 0;
    for (DataPage dataPage : pages) {
      valueCount += dataPage.getValueCount();
      rowCount += ((DataPageV2) dataPage).getRowCount();
    }
    assertEquals(rows, rowCount);
    assertEquals(rows, valueCount);
    MemPageReader pageReader = new MemPageReader(rows, pages.iterator(), pageWriter.getDictionaryPage(), true);
    ValidatingConverter converter = new ValidatingConverter();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, pageReader, converter, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < rows; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      columnReader.writeCurrentValueToConverter();
      columnReader.consume();
    }
    assertEquals(rows, converter.count);
  }

  @Test
  public void testLazy() throws Exception {
    // Write test data
    MessageType schema = MessageTypeParser.parseMessageType("message test { required binary foo; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 columnWriterV2 = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withDictionaryPageSize(1024)
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(2048)
            .build());
    for (int i = 0; i < rows; i++) {
      columnWriterV2.write(Binary.fromString("bar" + i % 10), 0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage();
      }
    }
    columnWriterV2.writePage();
    columnWriterV2.finalizeColumnChunk();
    List<DataPage> pages = pageWriter.getPages();
    int valueCount = 0;
    int rowCount = 0;
    for (DataPage dataPage : pages) {
      valueCount += dataPage.getValueCount();
      rowCount += ((DataPageV2) dataPage).getRowCount();
    }
    assertEquals(rows, rowCount);
    assertEquals(rows, valueCount);

    // Read lazily
    MemPageReader pageReader = new MemPageReader(rows, pages.iterator(), pageWriter.getDictionaryPage(), false);
    ValidatingConverter converter = new ValidatingConverter();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, pageReader, converter, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < rows; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      columnReader.writeCurrentValueToConverter();
      columnReader.consume();
    }
    assertEquals(rows, converter.count);
  }

  @Test
  public void testOptional() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { optional binary foo; }");
    ColumnDescriptor col = schema.getColumns().get(0);
    MemPageWriter pageWriter = new MemPageWriter();
    ColumnWriterV2 columnWriterV2 = new ColumnWriterV2(
        col,
        pageWriter,
        ParquetProperties.builder()
            .withDictionaryPageSize(1024)
            .withWriterVersion(PARQUET_2_0)
            .withPageSize(2048)
            .build());
    for (int i = 0; i < rows; i++) {
      columnWriterV2.writeNull(0, 0);
      if ((i + 1) % 1000 == 0) {
        columnWriterV2.writePage();
      }
    }
    columnWriterV2.writePage();
    columnWriterV2.finalizeColumnChunk();
    List<DataPage> pages = pageWriter.getPages();
    int valueCount = 0;
    int rowCount = 0;
    for (DataPage dataPage : pages) {
      valueCount += dataPage.getValueCount();
      rowCount += ((DataPageV2) dataPage).getRowCount();
    }
    assertEquals(rows, rowCount);
    assertEquals(rows, valueCount);
    MemPageReader pageReader = new MemPageReader(rows, pages.iterator(), pageWriter.getDictionaryPage(), true);
    ValidatingConverter converter = new ValidatingConverter();
    ColumnReader columnReader =
        new ColumnReaderImpl(col, pageReader, converter, VersionParser.parse(Version.FULL_VERSION));
    for (int i = 0; i < rows; i++) {
      assertEquals(0, columnReader.getCurrentRepetitionLevel());
      assertEquals(0, columnReader.getCurrentDefinitionLevel());
      columnReader.consume();
    }
    assertEquals(0, converter.count);
  }
}
