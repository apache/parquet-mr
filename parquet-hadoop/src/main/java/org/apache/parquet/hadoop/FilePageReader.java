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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.BlockCipher.Decryptor;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.hadoop.ParquetFileReader.Chunk;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the reading of a single page.
 */
public class FilePageReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(FilePageReader.class);

  private final ParquetFileReader parquetFileReader;
  private final Chunk chunk;
  private final int currentBlock;
  private final BlockCipher.Decryptor headerBlockDecryptor;
  private final BlockCipher.Decryptor pageBlockDecryptor;
  private final byte[] aadPrefix;
  private final int rowGroupOrdinal;
  private final int columnOrdinal;

  // state
  private final LinkedBlockingDeque<Optional<DataPage>> pagesInChunk = new LinkedBlockingDeque<>();
  private DictionaryPage dictionaryPage = null;
  private int pageIndex = 0;
  private long valuesCountReadSoFar = 0;
  private int dataPageCountReadSoFar = 0;

  // derived
  private final PrimitiveType type;
  private final byte[] dataPageAAD;
  private byte[] dataPageHeaderAAD = null;

  private final BytesInputDecompressor decompressor;

  private final ConcurrentLinkedQueue<Future<Void>> readFutures = new ConcurrentLinkedQueue<>();

  private final LongAdder totalTimeReadOnePage = new LongAdder();
  private final LongAdder totalCountReadOnePage = new LongAdder();
  private final LongAccumulator maxTimeReadOnePage = new LongAccumulator(Long::max, 0L);
  private final LongAdder totalTimeBlockedPagesInChunk = new LongAdder();
  private final LongAdder totalCountBlockedPagesInChunk = new LongAdder();
  private final LongAccumulator maxTimeBlockedPagesInChunk = new LongAccumulator(Long::max, 0L);

  public FilePageReader(
      ParquetFileReader parquetFileReader,
      Chunk chunk,
      int currentBlock,
      Decryptor headerBlockDecryptor,
      Decryptor pageBlockDecryptor,
      byte[] aadPrefix,
      int rowGroupOrdinal,
      int columnOrdinal,
      BytesInputDecompressor decompressor) {
    this.parquetFileReader = parquetFileReader;
    this.chunk = chunk;
    this.currentBlock = currentBlock;
    this.headerBlockDecryptor = headerBlockDecryptor;
    this.pageBlockDecryptor = pageBlockDecryptor;
    this.aadPrefix = aadPrefix;
    this.rowGroupOrdinal = rowGroupOrdinal;
    this.columnOrdinal = columnOrdinal;
    this.decompressor = decompressor;

    this.type = parquetFileReader
        .getFileMetaData()
        .getSchema()
        .getType(chunk.getDescriptor().getCol().getPath())
        .asPrimitiveType();

    if (null != headerBlockDecryptor) {
      dataPageHeaderAAD = AesCipher.createModuleAAD(
          aadPrefix,
          ModuleType.DataPageHeader,
          rowGroupOrdinal,
          columnOrdinal,
          chunk.getPageOrdinal(dataPageCountReadSoFar));
    }
    if (null != pageBlockDecryptor) {
      dataPageAAD = AesCipher.createModuleAAD(aadPrefix, ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
    } else {
      dataPageAAD = null;
    }
  }

  public DictionaryPage getDictionaryPage() {
    return this.dictionaryPage;
  }

  public LinkedBlockingDeque<Optional<DataPage>> getPagesInChunk() {
    return this.pagesInChunk;
  }

  void readAllRemainingPagesAsync() {
    readFutures.offer(parquetFileReader.options.getProcessThreadPool().submit(new FilePageReaderTask(this)));
  }

  void readAllRemainingPages() throws IOException {
    while (hasMorePages(valuesCountReadSoFar, dataPageCountReadSoFar)) {
      readOnePage();
    }
    if (chunk.offsetIndex == null
        && valuesCountReadSoFar != chunk.getDescriptor().getMetadata().getValueCount()) {
      // Would be nice to have a CorruptParquetFileException or something as a subclass?
      throw new IOException(
          "Expected " + chunk.getDescriptor().getMetadata().getValueCount()
              + " values in column chunk at " + parquetFileReader.getPath()
              + " offset "
              + chunk.descriptor.getMetadata().getFirstDataPageOffset() + " but got "
              + valuesCountReadSoFar + " values instead over " + pagesInChunk.size()
              + " pages ending at file offset "
              + (chunk.getDescriptor().getFileOffset() + chunk.stream.position()));
    }
    try {
      pagesInChunk.put(Optional.empty()); // add a marker for end of data
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while reading page data", e);
    }
  }

  void readOnePage() throws IOException {
    long startRead = System.nanoTime();
    try {
      byte[] pageHeaderAAD = dataPageHeaderAAD;
      if (null != headerBlockDecryptor) {
        // Important: this verifies file integrity (makes sure dictionary page had not been removed)
        if (null == dictionaryPage
            && chunk.getDescriptor().getMetadata().hasDictionaryPage()) {
          pageHeaderAAD = AesCipher.createModuleAAD(
              aadPrefix, ModuleType.DictionaryPageHeader, rowGroupOrdinal, columnOrdinal, -1);
        } else {
          int pageOrdinal = chunk.getPageOrdinal(dataPageCountReadSoFar);
          AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
        }
      }
      PageHeader pageHeader = chunk.readPageHeader(headerBlockDecryptor, pageHeaderAAD);
      int uncompressedPageSize = pageHeader.getUncompressed_page_size();
      int compressedPageSize = pageHeader.getCompressed_page_size();
      final BytesInput pageBytes;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          // there is only one dictionary page per column chunk
          if (dictionaryPage != null) {
            throw new ParquetDecodingException("more than one dictionary page in column "
                + chunk.getDescriptor().getCol());
          }
          pageBytes = chunk.readAsBytesInput(compressedPageSize);
          if (parquetFileReader.options.usePageChecksumVerification() && pageHeader.isSetCrc()) {
            chunk.verifyCrc(
                pageHeader.getCrc(),
                pageBytes,
                "could not verify dictionary page integrity, CRC checksum verification failed");
          }
          DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
          DictionaryPage compressedDictionaryPage = new DictionaryPage(
              pageBytes,
              uncompressedPageSize,
              dicHeader.getNum_values(),
              parquetFileReader.converter.getEncoding(dicHeader.getEncoding()));
          // Copy crc to new page, used for testing
          if (pageHeader.isSetCrc()) {
            compressedDictionaryPage.setCrc(pageHeader.getCrc());
          }
          dictionaryPage = compressedDictionaryPage;
          break;
        case DATA_PAGE:
          DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
          pageBytes = chunk.readAsBytesInput(compressedPageSize);
          if (null != pageBlockDecryptor) {
            AesCipher.quickUpdatePageAAD(dataPageAAD, chunk.getPageOrdinal(pageIndex));
          }
          if (parquetFileReader.options.usePageChecksumVerification() && pageHeader.isSetCrc()) {
            chunk.verifyCrc(
                pageHeader.getCrc(),
                pageBytes,
                "could not verify page integrity, CRC checksum verification failed");
          }
          DataPageV1 dataPageV1 = new DataPageV1(
              pageBytes,
              dataHeaderV1.getNum_values(),
              uncompressedPageSize,
              parquetFileReader.converter.fromParquetStatistics(
                  parquetFileReader.getFileMetaData().getCreatedBy(),
                  dataHeaderV1.getStatistics(),
                  type),
              parquetFileReader.converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
              parquetFileReader.converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
              parquetFileReader.converter.getEncoding(dataHeaderV1.getEncoding()));
          // Copy crc to new page, used for testing
          if (pageHeader.isSetCrc()) {
            dataPageV1.setCrc(pageHeader.getCrc());
          }
          writePageToChunk(dataPageV1);
          valuesCountReadSoFar += dataHeaderV1.getNum_values();
          ++dataPageCountReadSoFar;
          pageIndex++;
          break;
        case DATA_PAGE_V2:
          DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
          int dataSize = compressedPageSize
              - dataHeaderV2.getRepetition_levels_byte_length()
              - dataHeaderV2.getDefinition_levels_byte_length();
          if (null != pageBlockDecryptor) {
            AesCipher.quickUpdatePageAAD(dataPageAAD, chunk.getPageOrdinal(pageIndex));
          }
          final BytesInput repetitionLevels =
              chunk.readAsBytesInput(dataHeaderV2.getRepetition_levels_byte_length());
          final BytesInput definitionLevels =
              chunk.readAsBytesInput(dataHeaderV2.getDefinition_levels_byte_length());
          final BytesInput values = chunk.readAsBytesInput(dataSize);
          if (parquetFileReader.options.usePageChecksumVerification() && pageHeader.isSetCrc()) {
            pageBytes = BytesInput.concat(repetitionLevels, definitionLevels, values);
            chunk.verifyCrc(
                pageHeader.getCrc(),
                pageBytes,
                "could not verify page integrity, CRC checksum verification failed");
          }
          DataPage dataPageV2 = new DataPageV2(
              dataHeaderV2.getNum_rows(),
              dataHeaderV2.getNum_nulls(),
              dataHeaderV2.getNum_values(),
              repetitionLevels,
              definitionLevels,
              parquetFileReader.converter.getEncoding(dataHeaderV2.getEncoding()),
              values,
              uncompressedPageSize,
              parquetFileReader.converter.fromParquetStatistics(
                  parquetFileReader.getFileMetaData().getCreatedBy(),
                  dataHeaderV2.getStatistics(),
                  type),
              dataHeaderV2.isIs_compressed());
          // Copy crc to new page, used for testing
          if (pageHeader.isSetCrc()) {
            dataPageV2.setCrc(pageHeader.getCrc());
          }
          writePageToChunk(dataPageV2);
          valuesCountReadSoFar += dataHeaderV2.getNum_values();
          ++dataPageCountReadSoFar;
          pageIndex++;
          break;
        default:
          LOG.debug("skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
          chunk.stream.skipFully(compressedPageSize);
          break;
      }
    } catch (Exception e) {
      LOG.info(
          "Exception while reading one more page for: [{} ]: {} ",
          Thread.currentThread().getName(),
          this);
      throw e;
    } finally {
      long timeSpentInRead = System.nanoTime() - startRead;
      totalCountReadOnePage.add(1);
      totalTimeReadOnePage.add(timeSpentInRead);
      maxTimeReadOnePage.accumulate(timeSpentInRead);
    }
  }

  private void writePageToChunk(DataPage page) {
    long writeStart = System.nanoTime();
    try {
      pagesInChunk.put(Optional.of(page));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while reading page data", e);
    }
    long timeSpent = System.nanoTime() - writeStart;
    totalTimeBlockedPagesInChunk.add(timeSpent);
    totalCountBlockedPagesInChunk.add(1);
    maxTimeBlockedPagesInChunk.accumulate(timeSpent);
  }

  private boolean hasMorePages(long valuesCountReadSoFar, int dataPageCountReadSoFar) {
    return chunk.offsetIndex == null
        ? valuesCountReadSoFar < chunk.getDescriptor().getMetadata().getValueCount()
        : dataPageCountReadSoFar < chunk.offsetIndex.getPageCount();
  }

  @Override
  public void close() throws IOException {
    Future<Void> readResult;
    while (!readFutures.isEmpty()) {
      try {
        readResult = readFutures.poll();
        readResult.get();
        if (!readResult.isDone() && !readResult.isCancelled()) {
          readResult.cancel(true);
        } else {
          readResult.get(1, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        // Do nothing
      }
    }
    if (LOG.isDebugEnabled()) {
      String mode = parquetFileReader.isParallelColumnReaderEnabled() ? "ASYNC" : "SYNC";
      LOG.debug(
          "File Page Reader stats: {}, {}, {}, {}, {}, {}, {}",
          mode,
          totalTimeReadOnePage.longValue() / 1000.0,
          totalCountReadOnePage.longValue(),
          maxTimeReadOnePage.longValue() / 1000.0,
          totalTimeBlockedPagesInChunk.longValue() / 1000.0,
          totalCountBlockedPagesInChunk.longValue(),
          maxTimeBlockedPagesInChunk.longValue() / 1000.0);
    }
  }

  @Override
  public String toString() {
    return "PageReader{" + "chunk="
        + chunk + ", currentBlock="
        + currentBlock + ", headerBlockDecryptor="
        + headerBlockDecryptor + ", pageBlockDecryptor="
        + pageBlockDecryptor + ", aadPrefix="
        + Arrays.toString(aadPrefix) + ", rowGroupOrdinal="
        + rowGroupOrdinal + ", columnOrdinal="
        + columnOrdinal + ", pagesInChunk="
        + pagesInChunk + ", dictionaryPage="
        + dictionaryPage + ", pageIndex="
        + pageIndex + ", valuesCountReadSoFar="
        + valuesCountReadSoFar + ", dataPageCountReadSoFar="
        + dataPageCountReadSoFar + ", type="
        + type + ", dataPageAAD="
        + Arrays.toString(dataPageAAD) + ", dataPageHeaderAAD="
        + Arrays.toString(dataPageHeaderAAD) + ", decompressor="
        + decompressor + '}';
  }

  private static class FilePageReaderTask implements Callable<Void> {

    final FilePageReader filePageReader;

    FilePageReaderTask(FilePageReader filePageReader) {
      this.filePageReader = filePageReader;
    }

    @Override
    public Void call() throws Exception {
      filePageReader.readAllRemainingPages();
      return null;
    }
  }
}
