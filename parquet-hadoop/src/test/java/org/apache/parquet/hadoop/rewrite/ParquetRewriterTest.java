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
package org.apache.parquet.hadoop.rewrite;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.EncDecProperties;
import org.apache.parquet.hadoop.util.EncryptionTestFile;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.TestFileBuilder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ParquetRewriterTest {

  private final int numRecord = 100000;
  private Configuration conf = new Configuration();
  private List<EncryptionTestFile> inputFiles = null;
  private String outputFile = null;
  private ParquetRewriter rewriter = null;

  private void testPruneSingleColumnTranslateCodec(List<Path> inputPaths) throws Exception {
    Path outputPath = new Path(outputFile);
    List<String> pruneColumns = Arrays.asList("Gender");
    CompressionCodecName newCodec = CompressionCodecName.ZSTD;
    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, inputPaths, outputPath);
    RewriteOptions options = builder.prune(pruneColumns).transform(newCodec).build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Links");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");

    // Verify codec has been translated
    verifyCodec(outputFile, new HashSet<CompressionCodecName>() {{
      add(CompressionCodecName.ZSTD);
    }}, null);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(new HashSet<>(pruneColumns), Collections.emptySet(), null);

    // Verify the page index
    validatePageIndex(new HashMap<Integer, Integer>() {{
      put(0, 0);
      put(1, 1);
      put(2, 3);
      put(3, 4);
    }});

    // Verify original.created.by is preserved
    validateCreatedBy();
  }

  @Test
  public void testPruneSingleColumnTranslateCodecSingleFile() throws Exception {
    testSingleInputFileSetup("GZIP");
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
    }};
    testPruneSingleColumnTranslateCodec(inputPaths);
  }

  @Test
  public void testPruneSingleColumnTranslateCodecTwoFiles() throws Exception {
    testMultipleInputFilesSetup();
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
      add(new Path(inputFiles.get(1).getFileName()));
    }};
    testPruneSingleColumnTranslateCodec(inputPaths);
  }

  private void testPruneNullifyTranslateCodec(List<Path> inputPaths) throws Exception {
    Path outputPath = new Path(outputFile);
    List<String> pruneColumns = Arrays.asList("Gender");
    Map<String, MaskMode> maskColumns = new HashMap<>();
    maskColumns.put("Links.Forward", MaskMode.NULLIFY);
    CompressionCodecName newCodec = CompressionCodecName.ZSTD;
    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, inputPaths, outputPath);
    RewriteOptions options = builder.prune(pruneColumns).mask(maskColumns).transform(newCodec).build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Links");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");

    // Verify codec has been translated
    verifyCodec(outputFile, new HashSet<CompressionCodecName>() {{
      add(newCodec);
    }}, null);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(new HashSet<>(pruneColumns), maskColumns.keySet(), null);

    // Verify the page index
    validatePageIndex(new HashMap<Integer, Integer>() {{
      put(0, 0);
      put(1, 1);
      put(2, 3);
    }});

    // Verify original.created.by is preserved
    validateCreatedBy();
  }

  @Test
  public void testPruneNullifyTranslateCodecSingleFile() throws Exception {
    testSingleInputFileSetup("GZIP");
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
    }};
    testPruneNullifyTranslateCodec(inputPaths);
  }

  @Test
  public void testPruneNullifyTranslateCodecTwoFiles() throws Exception {
    testMultipleInputFilesSetup();
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
      add(new Path(inputFiles.get(1).getFileName()));
    }};
    testPruneNullifyTranslateCodec(inputPaths);
  }

  private void testPruneEncryptTranslateCodec(List<Path> inputPaths) throws Exception {
    Path outputPath = new Path(outputFile);
    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, inputPaths, outputPath);

    // Prune
    List<String> pruneColumns = Arrays.asList("Gender");
    builder.prune(pruneColumns);

    // Translate codec
    CompressionCodecName newCodec = CompressionCodecName.ZSTD;
    builder.transform(newCodec);

    // Encrypt
    String[] encryptColumns = {"DocId"};
    FileEncryptionProperties fileEncryptionProperties =
            EncDecProperties.getFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false);
    builder.encrypt(Arrays.asList(encryptColumns)).encryptionProperties(fileEncryptionProperties);

    RewriteOptions options = builder.build();
    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema are not changed for the columns not pruned
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    assertEquals(fields.size(), 3);
    assertEquals(fields.get(0).getName(), "DocId");
    assertEquals(fields.get(1).getName(), "Name");
    assertEquals(fields.get(2).getName(), "Links");
    List<Type> subFields = fields.get(2).asGroupType().getFields();
    assertEquals(subFields.size(), 2);
    assertEquals(subFields.get(0).getName(), "Backward");
    assertEquals(subFields.get(1).getName(), "Forward");

    // Verify codec has been translated
    FileDecryptionProperties fileDecryptionProperties = EncDecProperties.getFileDecryptionProperties();
    verifyCodec(outputFile, new HashSet<CompressionCodecName>() {{
      add(newCodec);
    }}, fileDecryptionProperties);

    // Verify the data are not changed for the columns not pruned
    validateColumnData(new HashSet<>(pruneColumns), Collections.emptySet(), fileDecryptionProperties);

    // Verify column encryption
    ParquetMetadata metaData = getFileMetaData(outputFile, fileDecryptionProperties);
    assertTrue(metaData.getBlocks().size() > 0);
    List<ColumnChunkMetaData> columns = metaData.getBlocks().get(0).getColumns();
    Set<String> set = new HashSet<>(Arrays.asList(encryptColumns));
    for (ColumnChunkMetaData column : columns) {
      if (set.contains(column.getPath().toDotString())) {
        assertTrue(column.isEncrypted());
      } else {
        assertFalse(column.isEncrypted());
      }
    }

    // Verify original.created.by is preserved
    validateCreatedBy();
  }

  @Test
  public void testPruneEncryptTranslateCodecSingleFile() throws Exception {
    testSingleInputFileSetup("GZIP");
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
    }};
    testPruneEncryptTranslateCodec(inputPaths);
  }

  @Test
  public void testPruneEncryptTranslateCodecTwoFiles() throws Exception {
    testMultipleInputFilesSetup();
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
      add(new Path(inputFiles.get(1).getFileName()));
    }};
    testPruneEncryptTranslateCodec(inputPaths);
  }

  private void testNullifyAndEncryptColumn(List<Path> inputPaths) throws Exception {
    Map<String, MaskMode> maskColumns = new HashMap<>();
    maskColumns.put("DocId", MaskMode.NULLIFY);

    String[] encryptColumns = {"DocId"};
    FileEncryptionProperties fileEncryptionProperties = EncDecProperties.getFileEncryptionProperties(
            encryptColumns, ParquetCipher.AES_GCM_CTR_V1, false);

    Path outputPath = new Path(outputFile);
    RewriteOptions options = new RewriteOptions.Builder(conf, inputPaths, outputPath).mask(maskColumns)
            .transform(CompressionCodecName.ZSTD)
            .encrypt(Arrays.asList(encryptColumns)).encryptionProperties(fileEncryptionProperties).build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    FileDecryptionProperties fileDecryptionProperties = EncDecProperties.getFileDecryptionProperties();

    // Verify codec has not been changed
    verifyCodec(outputFile, new HashSet<CompressionCodecName>() {{
      add(CompressionCodecName.ZSTD);
    }}, fileDecryptionProperties);

    // Verify the data are not changed for non-encrypted and non-masked columns.
    // Also make sure the masked column is nullified.
    validateColumnData(Collections.emptySet(), maskColumns.keySet(), fileDecryptionProperties);

    // Verify the page index
    validatePageIndex(new HashMap<Integer, Integer>() {{
      put(1, 1);
      put(2, 2);
      put(3, 3);
      put(4, 4);
    }});

    // Verify the column is encrypted
    ParquetMetadata metaData = getFileMetaData(outputFile, fileDecryptionProperties);
    assertTrue(metaData.getBlocks().size() > 0);
    Set<String> encryptedColumns = new HashSet<>(Arrays.asList(encryptColumns));
    for (BlockMetaData blockMetaData : metaData.getBlocks()) {
      List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
      for (ColumnChunkMetaData column : columns) {
        if (encryptedColumns.contains(column.getPath().toDotString())) {
          assertTrue(column.isEncrypted());
        } else {
          assertFalse(column.isEncrypted());
        }
      }
    }
  }

  @Test
  public void testNullifyEncryptSingleFile() throws Exception {
    testSingleInputFileSetup("GZIP");
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
    }};
    testNullifyAndEncryptColumn(inputPaths);
  }

  @Test
  public void testNullifyEncryptTwoFiles() throws Exception {
    testMultipleInputFilesSetup();
    List<Path> inputPaths = new ArrayList<Path>() {{
      add(new Path(inputFiles.get(0).getFileName()));
      add(new Path(inputFiles.get(1).getFileName()));
    }};
    testNullifyAndEncryptColumn(inputPaths);
  }

  @Test
  public void testMergeTwoFilesOnly() throws Exception {
    testMultipleInputFilesSetup();

    // Only merge two files but do not change anything.
    List<Path> inputPaths = new ArrayList<>();
    for (EncryptionTestFile inputFile : inputFiles) {
      inputPaths.add(new Path(inputFile.getFileName()));
    }
    Path outputPath = new Path(outputFile);
    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, inputPaths, outputPath);
    RewriteOptions options = builder.build();

    rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();

    // Verify the schema are not changed
    ParquetMetadata pmd = ParquetFileReader.readFooter(conf, new Path(outputFile), ParquetMetadataConverter.NO_FILTER);
    MessageType schema = pmd.getFileMetaData().getSchema();
    MessageType expectSchema = createSchema();
    assertEquals(expectSchema, schema);

    // Verify codec has not been translated
    verifyCodec(outputFile, new HashSet<CompressionCodecName>() {{
      add(CompressionCodecName.GZIP);
      add(CompressionCodecName.UNCOMPRESSED);
    }}, null);

    // Verify the merged data are not changed
    validateColumnData(Collections.emptySet(), Collections.emptySet(), null);

    // Verify the page index
    validatePageIndex(new HashMap<Integer, Integer>() {{
      put(0, 0);
      put(1, 1);
      put(2, 2);
      put(3, 3);
      put(4, 4);
    }});

    // Verify original.created.by is preserved
    validateCreatedBy();
  }

  @Test(expected = InvalidSchemaException.class)
  public void testMergeTwoFilesWithDifferentSchema() throws Exception {
    MessageType schema1 = new MessageType("schema",
            new PrimitiveType(OPTIONAL, INT64, "DocId"),
            new PrimitiveType(REQUIRED, BINARY, "Name"),
            new PrimitiveType(OPTIONAL, BINARY, "Gender"),
            new GroupType(OPTIONAL, "Links",
                    new PrimitiveType(REPEATED, BINARY, "Backward"),
                    new PrimitiveType(REPEATED, BINARY, "Forward")));
    MessageType schema2 = new MessageType("schema",
            new PrimitiveType(OPTIONAL, INT64, "DocId"),
            new PrimitiveType(REQUIRED, BINARY, "Name"),
            new PrimitiveType(OPTIONAL, BINARY, "Gender"));
    inputFiles = Lists.newArrayList();
    inputFiles.add(new TestFileBuilder(conf, schema1)
            .withNumRecord(numRecord)
            .withCodec("UNCOMPRESSED")
            .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
            .build());
    inputFiles.add(new TestFileBuilder(conf, schema2)
            .withNumRecord(numRecord)
            .withCodec("UNCOMPRESSED")
            .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
            .build());
    outputFile = TestFileBuilder.createTempFile("test");

    List<Path> inputPaths = new ArrayList<>();
    for (EncryptionTestFile inputFile : inputFiles) {
      inputPaths.add(new Path(inputFile.getFileName()));
    }
    Path outputPath = new Path(outputFile);
    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, inputPaths, outputPath);
    RewriteOptions options = builder.build();

    // This should throw an exception because the schemas are different
    rewriter = new ParquetRewriter(options);
  }

  private void testSingleInputFileSetup(String compression) throws IOException {
    MessageType schema = createSchema();
    inputFiles = Lists.newArrayList();
    inputFiles.add(new TestFileBuilder(conf, schema)
            .withNumRecord(numRecord)
            .withCodec(compression)
            .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
            .build());
    outputFile = TestFileBuilder.createTempFile("test");
  }

  private void testMultipleInputFilesSetup() throws IOException {
    MessageType schema = createSchema();
    inputFiles = Lists.newArrayList();
    inputFiles.add(new TestFileBuilder(conf, schema)
            .withNumRecord(numRecord)
            .withCodec("GZIP")
            .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
            .build());
    inputFiles.add(new TestFileBuilder(conf, schema)
            .withNumRecord(numRecord)
            .withCodec("UNCOMPRESSED")
            .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
            .build());
    outputFile = TestFileBuilder.createTempFile("test");
  }

  private MessageType createSchema() {
    return new MessageType("schema",
            new PrimitiveType(OPTIONAL, INT64, "DocId"),
            new PrimitiveType(REQUIRED, BINARY, "Name"),
            new PrimitiveType(OPTIONAL, BINARY, "Gender"),
            new GroupType(OPTIONAL, "Links",
                    new PrimitiveType(REPEATED, BINARY, "Backward"),
                    new PrimitiveType(REPEATED, BINARY, "Forward")));
  }

  private void validateColumnData(Set<String> prunePaths,
                                  Set<String> nullifiedPaths,
                                  FileDecryptionProperties fileDecryptionProperties) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(outputFile))
            .withConf(conf).withDecryption(fileDecryptionProperties).build();

    // Get total number of rows from input files
    int totalRows = 0;
    for (EncryptionTestFile inputFile : inputFiles) {
      totalRows += inputFile.getFileContent().length;
    }

    for (int i = 0; i < totalRows; i++) {
      Group group = reader.read();
      assertNotNull(group);

      SimpleGroup expectGroup = inputFiles.get(i / numRecord).getFileContent()[i % numRecord];

      if (!prunePaths.contains("DocId")) {
        if (nullifiedPaths.contains("DocId")) {
          assertThrows(RuntimeException.class, () -> group.getLong("DocId", 0));
        } else {
          assertEquals(group.getLong("DocId", 0), expectGroup.getLong("DocId", 0));
        }
      }

      if (!prunePaths.contains("Name") && !nullifiedPaths.contains("Name")) {
        assertArrayEquals(group.getBinary("Name", 0).getBytes(),
                expectGroup.getBinary("Name", 0).getBytes());
      }

      if (!prunePaths.contains("Gender") && !nullifiedPaths.contains("Gender")) {
        assertArrayEquals(group.getBinary("Gender", 0).getBytes(),
                expectGroup.getBinary("Gender", 0).getBytes());
      }

      Group subGroup = group.getGroup("Links", 0);

      if (!prunePaths.contains("Links.Backward") && !nullifiedPaths.contains("Links.Backward")) {
        assertArrayEquals(subGroup.getBinary("Backward", 0).getBytes(),
                expectGroup.getGroup("Links", 0).getBinary("Backward", 0).getBytes());
      }

      if (!prunePaths.contains("Links.Forward")) {
        if (nullifiedPaths.contains("Links.Forward")) {
          assertThrows(RuntimeException.class, () -> subGroup.getBinary("Forward", 0));
        } else {
          assertArrayEquals(subGroup.getBinary("Forward", 0).getBytes(),
                  expectGroup.getGroup("Links", 0).getBinary("Forward", 0).getBytes());
        }
      }
    }

    reader.close();
  }

  private ParquetMetadata getFileMetaData(String file,
                                          FileDecryptionProperties fileDecryptionProperties) throws IOException {
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
            .withDecryption(fileDecryptionProperties)
            .build();
    ParquetMetadata pmd = null;
    InputFile inputFile = HadoopInputFile.fromPath(new Path(file), conf);
    try (SeekableInputStream in = inputFile.newStream()) {
      pmd = ParquetFileReader.readFooter(inputFile, readOptions, in);
    }
    return pmd;
  }

  private void verifyCodec(String file,
                           Set<CompressionCodecName> expectedCodecs,
                           FileDecryptionProperties fileDecryptionProperties) throws IOException {
    Set<CompressionCodecName> codecs = new HashSet<>();
    ParquetMetadata pmd = getFileMetaData(file, fileDecryptionProperties);
    for (int i = 0; i < pmd.getBlocks().size(); i++) {
      BlockMetaData block = pmd.getBlocks().get(i);
      for (int j = 0; j < block.getColumns().size(); ++j) {
        ColumnChunkMetaData columnChunkMetaData = block.getColumns().get(j);
        codecs.add(columnChunkMetaData.getCodec());
      }
    }
    assertEquals(expectedCodecs, codecs);
  }

  /**
   * Verify the page index is correct.
   *
   * @param outFileColumnMapping the column mapping from the output file to the input file.
   */
  private void validatePageIndex(Map<Integer, Integer> outFileColumnMapping) throws Exception {
    ParquetMetadata outMetaData = getFileMetaData(outputFile, null);

    int inputFileIndex = 0;
    TransParquetFileReader inReader = new TransParquetFileReader(HadoopInputFile.fromPath(
            new Path(inputFiles.get(inputFileIndex).getFileName()), conf), HadoopReadOptions.builder(conf).build());
    ParquetMetadata inMetaData = inReader.getFooter();

    try (TransParquetFileReader outReader = new TransParquetFileReader(
            HadoopInputFile.fromPath(new Path(outputFile), conf), HadoopReadOptions.builder(conf).build())) {

      for (int outBlockId = 0, inBlockId = 0; outBlockId < outMetaData.getBlocks().size(); ++outBlockId, ++inBlockId) {
        // Refresh reader of input file
        if (inBlockId == inMetaData.getBlocks().size()) {
          inReader = new TransParquetFileReader(
                  HadoopInputFile.fromPath(new Path(inputFiles.get(++inputFileIndex).getFileName()), conf),
                  HadoopReadOptions.builder(conf).build());
          inMetaData = inReader.getFooter();
          inBlockId = 0;
        }

        BlockMetaData inBlockMetaData = inMetaData.getBlocks().get(inBlockId);
        BlockMetaData outBlockMetaData = outMetaData.getBlocks().get(outBlockId);

        for (int j = 0; j < outBlockMetaData.getColumns().size(); j++) {
          if (!outFileColumnMapping.containsKey(j)) {
            continue;
          }
          int columnIdFromInputFile = outFileColumnMapping.get(j);
          ColumnChunkMetaData inChunk = inBlockMetaData.getColumns().get(columnIdFromInputFile);
          ColumnIndex inColumnIndex = inReader.readColumnIndex(inChunk);
          OffsetIndex inOffsetIndex = inReader.readOffsetIndex(inChunk);
          ColumnChunkMetaData outChunk = outBlockMetaData.getColumns().get(j);
          ColumnIndex outColumnIndex = outReader.readColumnIndex(outChunk);
          OffsetIndex outOffsetIndex = outReader.readOffsetIndex(outChunk);
          if (inColumnIndex != null) {
            assertEquals(inColumnIndex.getBoundaryOrder(), outColumnIndex.getBoundaryOrder());
            assertEquals(inColumnIndex.getMaxValues(), outColumnIndex.getMaxValues());
            assertEquals(inColumnIndex.getMinValues(), outColumnIndex.getMinValues());
            assertEquals(inColumnIndex.getNullCounts(), outColumnIndex.getNullCounts());
          }
          if (inOffsetIndex != null) {
            List<Long> inOffsets = getOffsets(inReader, inChunk);
            List<Long> outOffsets = getOffsets(outReader, outChunk);
            assertEquals(inOffsets.size(), outOffsets.size());
            assertEquals(inOffsets.size(), inOffsetIndex.getPageCount());
            assertEquals(inOffsetIndex.getPageCount(), outOffsetIndex.getPageCount());
            for (int k = 0; k < inOffsetIndex.getPageCount(); k++) {
              assertEquals(inOffsetIndex.getFirstRowIndex(k), outOffsetIndex.getFirstRowIndex(k));
              assertEquals(inOffsetIndex.getLastRowIndex(k, inChunk.getValueCount()),
                      outOffsetIndex.getLastRowIndex(k, outChunk.getValueCount()));
              assertEquals(inOffsetIndex.getOffset(k), (long) inOffsets.get(k));
              assertEquals(outOffsetIndex.getOffset(k), (long) outOffsets.get(k));
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
          rewriter.readBlock(pageHeader.getCompressed_page_size(), reader);
          break;
        case DATA_PAGE:
          DataPageHeader headerV1 = pageHeader.data_page_header;
          offsets.add(curOffset);
          rewriter.readBlock(pageHeader.getCompressed_page_size(), reader);
          readValues += headerV1.getNum_values();
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          offsets.add(curOffset);
          int rlLength = headerV2.getRepetition_levels_byte_length();
          rewriter.readBlock(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          rewriter.readBlock(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          rewriter.readBlock(payLoadLength, reader);
          readValues += headerV2.getNum_values();
          break;
        default:
          throw new IOException("Not recognized page type");
      }
    }
    return offsets;
  }

  private void validateCreatedBy() throws Exception {
    Set<String> createdBySet = new HashSet<>();
    for (EncryptionTestFile inputFile : inputFiles) {
      ParquetMetadata pmd = getFileMetaData(inputFile.getFileName(), null);
      createdBySet.add(pmd.getFileMetaData().getCreatedBy());
      assertNull(pmd.getFileMetaData().getKeyValueMetaData().get(ParquetRewriter.ORIGINAL_CREATED_BY_KEY));
    }

    // Verify created_by from input files have been deduplicated
    Object[] inputCreatedBys = createdBySet.toArray();
    assertEquals(1, inputCreatedBys.length);

    // Verify created_by has been set
    FileMetaData outFMD = getFileMetaData(outputFile, null).getFileMetaData();
    String inputCreatedBy = (String) inputCreatedBys[0];
    assertEquals(inputCreatedBy, outFMD.getCreatedBy());

    // Verify original.created.by has been set
    String originalCreatedBy = outFMD.getKeyValueMetaData().get(ParquetRewriter.ORIGINAL_CREATED_BY_KEY);
    assertEquals(inputCreatedBy, originalCreatedBy);
  }

}
