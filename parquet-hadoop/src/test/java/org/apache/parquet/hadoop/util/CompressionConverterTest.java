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
package org.apache.parquet.hadoop.util;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class CompressionConverterTest {

  private Configuration conf = new Configuration();
  private Map<String, String> extraMeta
    = ImmutableMap.of("key1", "value1", "key2", "value2");
  private CompressionConverter compressionConverter = new CompressionConverter();
  private Random rnd = new Random(5);

  @Test
  public void testTransCompression() throws Exception {
    String[] codecs = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD"};
    for (int i = 0; i < codecs.length; i++) {
      for (int j = 0; j <codecs.length; j++) {
        // Same codec for both are considered as valid test case
        testInternal(codecs[i], codecs[j], ParquetProperties.WriterVersion.PARQUET_1_0, ParquetProperties.DEFAULT_PAGE_SIZE);
        testInternal(codecs[i], codecs[j], ParquetProperties.WriterVersion.PARQUET_2_0, ParquetProperties.DEFAULT_PAGE_SIZE);
        testInternal(codecs[i], codecs[j], ParquetProperties.WriterVersion.PARQUET_1_0, 64);
        testInternal(codecs[i], codecs[j], ParquetProperties.WriterVersion.PARQUET_1_0, ParquetProperties.DEFAULT_PAGE_SIZE * 100);
      }
    }
  }

  private void testInternal(String srcCodec, String destCodec, ParquetProperties.WriterVersion writerVersion, int pageSize) throws Exception {
    int numRecord = 1000;
    TestDocs testDocs = new TestDocs(numRecord);
    String inputFile = createParquetFile(conf, extraMeta, numRecord, "input", srcCodec, writerVersion, pageSize, testDocs);
    String outputFile = createTempFile("output_trans");

    convertCompression(conf, inputFile, outputFile, destCodec);

    validateColumns(outputFile, numRecord, testDocs);
    validMeta(inputFile, outputFile);
    validColumnIndex(inputFile, outputFile);
  }

  private void convertCompression(Configuration conf, String inputFile, String outputFile, String codec) throws IOException {
    Path inPath = new Path(inputFile);
    Path outPath = new Path(outputFile);
    CompressionCodecName codecName = CompressionCodecName.valueOf(codec);

    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    ParquetFileWriter writer = new ParquetFileWriter(conf, schema, outPath, ParquetFileWriter.Mode.CREATE);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(HadoopInputFile.fromPath(inPath, conf), HadoopReadOptions.builder(conf).build())) {
      compressionConverter.processBlocks(reader, writer, metaData, schema, metaData.getFileMetaData().getCreatedBy(), codecName);
    } finally {
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
  }

  private void validateColumns(String file, int numRecord, TestDocs testDocs) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertTrue(group.getLong("DocId", 0) == testDocs.docId[i]);
      assertArrayEquals(group.getBinary("Name", 0).getBytes(), testDocs.name[i].getBytes());
      assertArrayEquals(group.getBinary("Gender", 0).getBytes(), testDocs.gender[i].getBytes());
      Group subGroup = group.getGroup("Links", 0);
      assertArrayEquals(subGroup.getBinary("Backward", 0).getBytes(), testDocs.linkBackward[i].getBytes());
      assertArrayEquals(subGroup.getBinary("Forward", 0).getBytes(), testDocs.linkForward[i].getBytes());
    }
    reader.close();
  }

  private void validMeta(String inputFile, String outFile) throws Exception {
    ParquetMetadata inMetaData = ParquetFileReader.readFooter(conf, new Path(inputFile), NO_FILTER);
    ParquetMetadata outMetaData = ParquetFileReader.readFooter(conf, new Path(outFile), NO_FILTER);
    Assert.assertEquals(inMetaData.getFileMetaData().getSchema(), outMetaData.getFileMetaData().getSchema());
    Assert.assertEquals(inMetaData.getFileMetaData().getKeyValueMetaData(), outMetaData.getFileMetaData().getKeyValueMetaData());
  }

  private void validColumnIndex(String inputFile, String outFile) throws Exception {
    ParquetMetadata inMetaData = ParquetFileReader.readFooter(conf, new Path(inputFile), NO_FILTER);
    ParquetMetadata outMetaData = ParquetFileReader.readFooter(conf, new Path(outFile), NO_FILTER);
    Assert.assertEquals(inMetaData.getBlocks().size(), outMetaData.getBlocks().size());
    try (TransParquetFileReader inReader = new TransParquetFileReader(HadoopInputFile.fromPath(new Path(inputFile), conf), HadoopReadOptions.builder(conf).build());
         TransParquetFileReader outReader = new TransParquetFileReader(HadoopInputFile.fromPath(new Path(outFile), conf), HadoopReadOptions.builder(conf).build())) {
      for (int i = 0; i < inMetaData.getBlocks().size(); i++) {
        BlockMetaData inBlockMetaData = inMetaData.getBlocks().get(i);
        BlockMetaData outBlockMetaData = outMetaData.getBlocks().get(i);
        Assert.assertEquals(inBlockMetaData.getColumns().size(), outBlockMetaData.getColumns().size());
        for (int j = 0; j < inBlockMetaData.getColumns().size(); j++) {
          ColumnChunkMetaData inChunk = inBlockMetaData.getColumns().get(j);
          ColumnIndex inColumnIndex = inReader.readColumnIndex(inChunk);
          OffsetIndex inOffsetIndex = inReader.readOffsetIndex(inChunk);
          ColumnChunkMetaData outChunk = outBlockMetaData.getColumns().get(j);
          ColumnIndex outColumnIndex = outReader.readColumnIndex(outChunk);
          OffsetIndex outOffsetIndex = outReader.readOffsetIndex(outChunk);
          if (inColumnIndex != null) {
            Assert.assertEquals(inColumnIndex.getBoundaryOrder(), outColumnIndex.getBoundaryOrder());
            Assert.assertEquals(inColumnIndex.getMaxValues(), outColumnIndex.getMaxValues());
            Assert.assertEquals(inColumnIndex.getMinValues(), outColumnIndex.getMinValues());
            Assert.assertEquals(inColumnIndex.getNullCounts(), outColumnIndex.getNullCounts());
          }
          if (inOffsetIndex != null) {
            List<Long> inOffsets = getOffsets(inReader, inChunk);
            List<Long> outOffsets = getOffsets(outReader, outChunk);
            Assert.assertEquals(inOffsets.size(), outOffsets.size());
            Assert.assertEquals(inOffsets.size(), inOffsetIndex.getPageCount());
            Assert.assertEquals(inOffsetIndex.getPageCount(), outOffsetIndex.getPageCount());
            for (int k = 0; k < inOffsetIndex.getPageCount(); k++) {
              Assert.assertEquals(inOffsetIndex.getFirstRowIndex(k), outOffsetIndex.getFirstRowIndex(k));
              Assert.assertEquals(inOffsetIndex.getLastRowIndex(k, inChunk.getValueCount()),
                outOffsetIndex.getLastRowIndex(k, outChunk.getValueCount()));
              Assert.assertEquals(inOffsetIndex.getOffset(k), (long)inOffsets.get(k));
              Assert.assertEquals(outOffsetIndex.getOffset(k), (long)outOffsets.get(k));
            }
          }
        }
      }
    }
  }

  private List<Long> getOffsets(TransParquetFileReader reader, ColumnChunkMetaData chunk) throws IOException {
    List<Long> offsets = new ArrayList<>();
    reader.setStreamPosition(chunk.getStartingPos());
    long readValues = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      long curOffset = reader.getPos();
      PageHeader pageHeader = reader.readPageHeader();
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          compressionConverter.readBlock(pageHeader.getCompressed_page_size(), reader);
          break;
        case DATA_PAGE:
          DataPageHeader headerV1 = pageHeader.data_page_header;
          offsets.add(curOffset);
          compressionConverter.readBlock(pageHeader.getCompressed_page_size(), reader);
          readValues += headerV1.getNum_values();
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          offsets.add(curOffset);
          int rlLength = headerV2.getRepetition_levels_byte_length();
          compressionConverter.readBlock(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          compressionConverter.readBlock(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          compressionConverter.readBlock(payLoadLength, reader);
          readValues += headerV2.getNum_values();
          break;
        default:
          throw new IOException("Not recognized page type");
      }
    }
    return offsets;
  }

  private String createParquetFile(Configuration conf, Map<String, String> extraMeta, int numRecord, String prefix, String codec,
                                         ParquetProperties.WriterVersion writerVersion, int pageSize, TestDocs testDocs) throws IOException {
    MessageType schema = new MessageType("schema",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "Name"),
      new PrimitiveType(REQUIRED, BINARY, "Gender"),
      new GroupType(OPTIONAL, "Links",
        new PrimitiveType(REPEATED, BINARY, "Backward"),
        new PrimitiveType(REPEATED, BINARY, "Forward")));

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    String file = createTempFile(prefix);
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file))
      .withConf(conf)
      .withWriterVersion(writerVersion)
      .withExtraMetaData(extraMeta)
      .withDictionaryEncoding("DocId", true)
      .withValidation(true)
      .enablePageWriteChecksum()
      .withPageSize(pageSize)
      .withCompressionCodec(CompressionCodecName.valueOf(codec));
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("DocId", testDocs.docId[i]);
        g.add("Name", testDocs.name[i]);
        g.add("Gender", testDocs.gender[i]);
        Group links = g.addGroup("Links");
        links.add(0, testDocs.linkBackward[i]);
        links.add(1, testDocs.linkForward[i]);
        writer.write(g);
      }
    }

    return file;
  }

  private static long getLong() {
    return ThreadLocalRandom.current().nextLong(1000);
  }

  private String getString() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append(chars[rnd.nextInt(10)]);
    }
    return sb.toString();
  }

  private String createTempFile(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private  class TestDocs {
    public long[] docId;
    public String[] name;
    public String[] gender;
    public String[] linkBackward;
    public String[] linkForward;

    public TestDocs(int numRecord) {
      docId = new long[numRecord];
      for (int i = 0; i < numRecord; i++) {
        docId[i] = getLong();
      }

      name = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        name[i] = getString();
      }

      gender = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        gender[i] = getString();
      }

      linkBackward = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        linkBackward[i] = getString();
      }

      linkForward = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        linkForward[i] = getString();
      }
    }
  }
}
