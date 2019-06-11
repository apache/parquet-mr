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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.codec.SnappyCompressor;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.*;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.*;

/**
 * Tests that page level checksums are correctly written and that checksum verification works as
 * expected
 */
public class TestDataPageV1Checksums {
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final Statistics<?> EMPTY_STATS_INT32 = Statistics.getBuilderForReading(
    Types.required(INT32).named("a")).build();

  private CRC32 crc = new CRC32();

  // Sample data, two columns 'a' and 'b' (both int32),

  private static final int PAGE_SIZE = 1024 * 1024; // 1MB

  private static final MessageType schemaSimple = MessageTypeParser.parseMessageType(
    "message m {" +
    "  required int32 a;" +
    "  required int32 b;" +
    "}");
  private static final ColumnDescriptor colADesc = schemaSimple.getColumns().get(0);
  private static final ColumnDescriptor colBDesc = schemaSimple.getColumns().get(1);
  private static final byte[] colAPage1Bytes = new byte[PAGE_SIZE];
  private static final byte[] colAPage2Bytes = new byte[PAGE_SIZE];
  private static final byte[] colBPage1Bytes = new byte[PAGE_SIZE];
  private static final byte[] colBPage2Bytes = new byte[PAGE_SIZE];
  private static final int numRecordsLargeFile = (2 * PAGE_SIZE) / Integer.BYTES;

  /** Write out sample Parquet file using ColumnChunkPageWriteStore directly, return path to file */
  private Path writeSimpleParquetFile(Configuration conf, CompressionCodecName compression)
    throws IOException {
    File file = tempFolder.newFile();
    file.delete();
    Path path = new Path(file.toURI());

    for (int i = 0; i < PAGE_SIZE; i++) {
      colAPage1Bytes[i] = (byte) i;
      colAPage2Bytes[i] = (byte) -i;
      colBPage1Bytes[i] = (byte) (i + 100);
      colBPage2Bytes[i] = (byte) (i - 100);
    }

    ParquetFileWriter writer =  new ParquetFileWriter(conf, schemaSimple, path,
      ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.MAX_PADDING_SIZE_DEFAULT);

    writer.start();
    writer.startBlock(numRecordsLargeFile);

    CodecFactory codecFactory = new CodecFactory(conf, 1024 * 1024);
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(compression);

    ColumnChunkPageWriteStore writeStore = new ColumnChunkPageWriteStore(
      compressor, schemaSimple, new HeapByteBufferAllocator(),
      Integer.MAX_VALUE, conf.getBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED,
      ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED));

    PageWriter pageWriter = writeStore.getPageWriter(colADesc);
    pageWriter.writePage(BytesInput.from(colAPage1Bytes), numRecordsLargeFile / 2,
      numRecordsLargeFile / 2, EMPTY_STATS_INT32, Encoding.RLE, Encoding.RLE, Encoding.PLAIN);
    pageWriter.writePage(BytesInput.from(colAPage2Bytes), numRecordsLargeFile / 2,
      numRecordsLargeFile / 2, EMPTY_STATS_INT32, Encoding.RLE, Encoding.RLE, Encoding.PLAIN);

    pageWriter = writeStore.getPageWriter(colBDesc);
    pageWriter.writePage(BytesInput.from(colBPage1Bytes), numRecordsLargeFile / 2,
      numRecordsLargeFile / 2, EMPTY_STATS_INT32, Encoding.RLE, Encoding.RLE, Encoding.PLAIN);
    pageWriter.writePage(BytesInput.from(colBPage2Bytes), numRecordsLargeFile / 2,
      numRecordsLargeFile / 2, EMPTY_STATS_INT32, Encoding.RLE, Encoding.RLE, Encoding.PLAIN);

    writeStore.flushToFileWriter(writer);

    writer.endBlock();
    writer.end(new HashMap<>());

    codecFactory.release();

    return path;
  }

  // Sample data, nested schema with nulls

  private static final MessageType schemaNestedWithNulls = MessageTypeParser.parseMessageType(
    "message m {" +
      "  optional group c {" +
      "    required int64 id;" +
      "    required group d {" +
      "      repeated int32 val;" +
      "    }" +
      "  }" +
      "}");
  private static final ColumnDescriptor colCIdDesc = schemaNestedWithNulls.getColumns().get(0);
  private static final ColumnDescriptor colDValDesc = schemaNestedWithNulls.getColumns().get(1);

  private static final double nullRatio = 0.3;
  private static final int numRecordsNestedWithNullsFile = 1000;

  private Path writeNestedWithNullsSampleParquetFile(Configuration conf,
                                                     CompressionCodecName compression)
    throws IOException {
    File file = tempFolder.newFile();
    file.delete();
    Path path = new Path(file.toURI());

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
      .withConf(conf)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withCompressionCodec(compression)
      .withType(schemaNestedWithNulls)
      .withPageWriteChecksumEnabled(conf.getBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED,
        true))
      .build();

    GroupFactory groupFactory = new SimpleGroupFactory(schemaNestedWithNulls);
    Random rand = new Random(42);

    for (int i = 0; i < numRecordsNestedWithNullsFile; i++) {
      Group group = groupFactory.newGroup();
      if (rand.nextDouble() > nullRatio) {
        // With equal probability, write out either 1 or 3 values in group e
        if (rand.nextDouble() > 0.5) {
          group.addGroup("c").append("id", (long) i).addGroup("d")
            .append("val", rand.nextInt());
        } else {
          group.addGroup("c").append("id", (long) i).addGroup("d")
            .append("val", rand.nextInt())
            .append("val", rand.nextInt())
            .append("val", rand.nextInt());
        }
      }
      writer.write(group);
    }
    writer.close();

    return path;
  }

  /**
   * Enable writing out page level crc checksum, disable verification in read path but check that
   * the crc checksums are correct. Tests whether we successfully write out correct crc checksums
   * without potentially failing on the read path verification .
   */
  @Test
  public void testWriteOnVerifyOff() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED);

    try (ParquetFileReader reader = getParquetFileReader(path, conf,
      Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      DataPageV1 colAPage1 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage1, colAPage1Bytes);
      assertCorrectContent(colAPage1, colAPage1Bytes);

      DataPageV1 colAPage2 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage2, colAPage2Bytes);
      assertCorrectContent(colAPage2, colAPage2Bytes);

      DataPageV1 colBPage1 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage1, colBPage1Bytes);
      assertCorrectContent(colBPage1, colBPage1Bytes);

      DataPageV1 colBPage2 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage2, colBPage2Bytes);
      assertCorrectContent(colBPage2, colBPage2Bytes);
    }
  }

  /** Test that we do not write out checksums if the feature is turned off */
  @Test
  public void testWriteOffVerifyOff() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED);

    try (ParquetFileReader reader = getParquetFileReader(path, conf,
      Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      assertCrcNotSet(readNextPage(colADesc, pageReadStore));
      assertCrcNotSet(readNextPage(colADesc, pageReadStore));
      assertCrcNotSet(readNextPage(colBDesc, pageReadStore));
      assertCrcNotSet(readNextPage(colBDesc, pageReadStore));
    }
  }

  /**
   * Do not write out page level crc checksums, but enable verification on the read path. Tests
   * that the read still succeeds and does not throw an exception.
   */
  @Test
  public void testWriteOffVerifyOn() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED);

    try (ParquetFileReader reader = getParquetFileReader(path, conf,
      Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      assertCorrectContent(readNextPage(colADesc, pageReadStore), colAPage1Bytes);
      assertCorrectContent(readNextPage(colADesc, pageReadStore), colAPage2Bytes);
      assertCorrectContent(readNextPage(colBDesc, pageReadStore), colBPage1Bytes);
      assertCorrectContent(readNextPage(colBDesc, pageReadStore), colBPage2Bytes);
    }
  }

  /**
   * Write out checksums and verify them on the read path. Tests that crc is set and that we can
   * read back what we wrote if checksums are enabled on both the write and read path.
   */
  @Test
  public void testWriteOnVerifyOn() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED);

    try (ParquetFileReader reader = getParquetFileReader(path, conf,
      Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      DataPageV1 colAPage1 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage1, colAPage1Bytes);
      assertCorrectContent(colAPage1, colAPage1Bytes);

      DataPageV1 colAPage2 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage2, colAPage2Bytes);
      assertCorrectContent(colAPage2, colAPage2Bytes);

      DataPageV1 colBPage1 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage1, colBPage1Bytes);
      assertCorrectContent(colBPage1, colBPage1Bytes);

      DataPageV1 colBPage2 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage2, colBPage2Bytes);
      assertCorrectContent(colBPage2, colBPage2Bytes);
    }
  }

  /**
   * Test whether corruption in the page content is detected by checksum verification
   */
  @Test
  public void testCorruptedPage() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.UNCOMPRESSED);

    InputFile inputFile = HadoopInputFile.fromPath(path, conf);
    SeekableInputStream inputStream = inputFile.newStream();
    int fileLen = (int) inputFile.getLength();

    byte[] fileBytes = new byte[fileLen];
    inputStream.readFully(fileBytes);
    inputStream.close();

    // There are 4 pages in total (2 per column), we corrupt the first page of the first column and
    // the second page of the second column. We do this by altering a byte roughly in the middle of
    // each page to be corrupted
    fileBytes[fileLen / 8]++;
    fileBytes[fileLen / 8 + ((fileLen / 4) * 3)]++;

    OutputFile outputFile = HadoopOutputFile.fromPath(path, conf);
    PositionOutputStream outputStream = outputFile.createOrOverwrite(1024 * 1024);
    outputStream.write(fileBytes);
    outputStream.close();

    // First we disable checksum verification, the corruption will go undetected as it is in the
    // data section of the page
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);
    try (ParquetFileReader reader = getParquetFileReader(path, conf,
      Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      DataPageV1 colAPage1 = readNextPage(colADesc, pageReadStore);
      assertFalse("Data in page was not corrupted",
        Arrays.equals(colAPage1.getBytes().toByteArray(), colAPage1Bytes));
      readNextPage(colADesc, pageReadStore);
      readNextPage(colBDesc, pageReadStore);
      DataPageV1 colBPage2 = readNextPage(colBDesc, pageReadStore);
      assertFalse("Data in page was not corrupted",
        Arrays.equals(colBPage2.getBytes().toByteArray(), colBPage2Bytes));
    }

    // Now we enable checksum verification, the corruption should be detected
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);
    try (ParquetFileReader reader =
      getParquetFileReader(path, conf, Arrays.asList(colADesc, colBDesc))) {

      PageReadStore pageReadStore = reader.readNextRowGroup();

      assertVerificationFailed(colADesc, pageReadStore);
      readNextPage(colADesc, pageReadStore);
      readNextPage(colBDesc, pageReadStore);
      assertVerificationFailed(colBDesc, pageReadStore);
    }
  }

  /**
   * Tests that the checksum is calculated using the compressed version of the data and that
   * checksum verification succeeds
   */
  @Test
  public void testCompression() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);

    Path path = writeSimpleParquetFile(conf, CompressionCodecName.SNAPPY);

    try (ParquetFileReader reader = getParquetFileReader(path, conf,
      Arrays.asList(colADesc, colBDesc))) {
      PageReadStore pageReadStore = reader.readNextRowGroup();

      DataPageV1 colAPage1 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage1, snappy(colAPage1Bytes));
      assertCorrectContent(colAPage1, colAPage1Bytes);

      DataPageV1 colAPage2 = readNextPage(colADesc, pageReadStore);
      assertCrcSetAndCorrect(colAPage2, snappy(colAPage2Bytes));
      assertCorrectContent(colAPage2, colAPage2Bytes);

      DataPageV1 colBPage1 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage1, snappy(colBPage1Bytes));
      assertCorrectContent(colBPage1, colBPage1Bytes);

      DataPageV1 colBPage2 = readNextPage(colBDesc, pageReadStore);
      assertCrcSetAndCorrect(colBPage2, snappy(colBPage2Bytes));
      assertCorrectContent(colBPage2, colBPage2Bytes);
    }
  }

  /**
   * Tests that we adhere to the checksum calculation specification, namely that the crc is
   * calculated using the compressed concatenation of the repetition levels, definition levels and
   * the actual data. This is done by generating sample data with a nested schema containing nulls
   * (generating non trivial repetition and definition levels).
   */
  @Test
  public void testNestedWithNulls() throws IOException {
    Configuration conf = new Configuration();

    // Write out sample file via the non-checksum code path, extract the raw bytes to calculate the
    // reference crc with
    conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, false);
    conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false);
    Path refPath = writeNestedWithNullsSampleParquetFile(conf, CompressionCodecName.SNAPPY);

    try (ParquetFileReader refReader = getParquetFileReader(refPath, conf,
      Arrays.asList(colCIdDesc, colDValDesc))) {
      PageReadStore refPageReadStore = refReader.readNextRowGroup();
      byte[] colCIdPageBytes = readNextPage(colCIdDesc, refPageReadStore).getBytes().toByteArray();
      byte[] colDValPageBytes = readNextPage(colDValDesc, refPageReadStore).getBytes().toByteArray();

      // Write out sample file with checksums
      conf.setBoolean(ParquetOutputFormat.PAGE_WRITE_CHECKSUM_ENABLED, true);
      conf.setBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true);
      Path path = writeNestedWithNullsSampleParquetFile(conf, CompressionCodecName.SNAPPY);

      try (ParquetFileReader reader = getParquetFileReader(path, conf,
        Arrays.asList(colCIdDesc, colDValDesc))) {
        PageReadStore pageReadStore = reader.readNextRowGroup();

        DataPageV1 colCIdPage = readNextPage(colCIdDesc, pageReadStore);
        assertCrcSetAndCorrect(colCIdPage, snappy(colCIdPageBytes));
        assertCorrectContent(colCIdPage, colCIdPageBytes);

        DataPageV1 colDValPage = readNextPage(colDValDesc, pageReadStore);
        assertCrcSetAndCorrect(colDValPage, snappy(colDValPageBytes));
        assertCorrectContent(colDValPage, colDValPageBytes);
      }
    }
  }

  /** Compress using snappy */
  private byte[] snappy(byte[] bytes) throws IOException {
    SnappyCompressor compressor = new SnappyCompressor();
    compressor.reset();
    compressor.setInput(bytes, 0, bytes.length);
    compressor.finish();
    byte[] buffer = new byte[bytes.length * 2];
    int compressedSize = compressor.compress(buffer, 0, buffer.length);
    return Arrays.copyOfRange(buffer, 0, compressedSize);
  }

  /** Construct ParquetFileReader for input file and columns */
  private ParquetFileReader getParquetFileReader(Path path, Configuration conf,
                                                 List<ColumnDescriptor> columns)
    throws IOException {
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, path);
    return new ParquetFileReader(conf, footer.getFileMetaData(), path,
      footer.getBlocks(), columns);
  }

  /** Read the next page for a column */
  private DataPageV1 readNextPage(ColumnDescriptor colDesc, PageReadStore pageReadStore) {
    return (DataPageV1) pageReadStore.getPageReader(colDesc).readPage();
  }

  /**
   * Compare the extracted (decompressed) bytes to the reference bytes
   */
  private void assertCorrectContent(DataPageV1 page, byte[] referenceBytes) throws IOException {
    assertArrayEquals("Read page content was different from expected page content", referenceBytes,
      page.getBytes().toByteArray());
  }

  /**
   * Verify that the crc is set in a page, calculate the reference crc using the reference bytes and
   * check that the crc's are identical.
   */
  private void assertCrcSetAndCorrect(DataPageV1 page, byte[] referenceBytes) {
    assertTrue("Checksum was not set in page", page.isSetCrc32());
    int crcFromPage = page.getCrc32();
    crc.reset();
    crc.update(referenceBytes);
    assertEquals("Checksum found in page did not match calculated reference checksum",
      (int) crc.getValue(), crcFromPage);
  }

  /** Verify that the crc is not set and that is has the default value */
  private void assertCrcNotSet(DataPageV1 page) {
    assertFalse("Checksum was set in page", page.isSetCrc32());
    assertEquals("Checksum does not have default value", 0, page.getCrc32());
  }

  /**
   * Read the next page for a column, fail if this did not throw an checksum verification exception,
   * if the read succeeds (no exception was thrown ), verify that the checksum was not set.
   */
  private void assertVerificationFailed(ColumnDescriptor columnDesc, PageReadStore pageReadStore) {
    try {
      DataPage page = pageReadStore.getPageReader(columnDesc).readPage();
      fail("Expected checksum verification exception to be thrown");
    } catch (Exception e) {
      assertTrue("Thrown exception is of incorrect type", e instanceof ParquetDecodingException);
      assertTrue("Did not catch checksum verification ParquetDecodingException",
        e.getMessage().contains("could not verify page integrity, CRC checksum verification " +
          "failed"));
    }
  }
}
