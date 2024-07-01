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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 */
public class ColumnChunkPageReadStore implements PageReadStore, DictionaryPageReadStore {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReadStore.class);

  /**
   * PageReader for a single column chunk. A column chunk contains
   * several pages, which are yielded one by one in order.
   * <p>
   * This implementation is provided with a list of pages, each of which
   * is decompressed and passed through.
   */
  public static final class ColumnChunkPageReader implements PageReader {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReader.class);

    private final BytesInputDecompressor decompressor;
    private final long valueCount;
    private final LinkedBlockingDeque<Optional<DataPage>> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    // null means no page synchronization is required; firstRowIndex will not be returned by the pages
    private final OffsetIndex offsetIndex;
    private final long rowCount;
    private final ParquetReadOptions options;
    private int pageIndex = 0;

    private final BlockCipher.Decryptor blockDecryptor;
    private final byte[] dataPageAAD;
    private final byte[] dictionaryPageAAD;
    private final ByteBufferReleaser releaser;
    private final FilePageReader filePageReader;

    ColumnChunkPageReader(
        BytesInputDecompressor decompressor,
        LinkedBlockingDeque<Optional<DataPage>> compressedPages,
        DictionaryPage compressedDictionaryPage,
        OffsetIndex offsetIndex,
        long valueCount,
        long rowCount,
        BlockCipher.Decryptor blockDecryptor,
        byte[] fileAAD,
        int rowGroupOrdinal,
        int columnOrdinal,
        FilePageReader filePageReader,
        ParquetReadOptions options) {
      this.decompressor = decompressor;
      this.compressedPages = compressedPages;
      this.compressedDictionaryPage = compressedDictionaryPage;
      this.valueCount = valueCount;
      this.offsetIndex = offsetIndex;
      this.rowCount = rowCount;
      this.options = options;
      this.releaser = new ByteBufferReleaser(options.getAllocator());
      this.blockDecryptor = blockDecryptor;

      this.filePageReader = filePageReader;

      if (null != blockDecryptor) {
        dataPageAAD =
            AesCipher.createModuleAAD(fileAAD, ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
        dictionaryPageAAD = AesCipher.createModuleAAD(
            fileAAD, ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, -1);
      } else {
        dataPageAAD = null;
        dictionaryPageAAD = null;
      }
    }

    @Override
    public void close() throws IOException {
      this.filePageReader.close();
    }

    private int getPageOrdinal(int currentPageIndex) {
      if (null == offsetIndex) {
        return currentPageIndex;
      }

      return offsetIndex.getPageOrdinal(currentPageIndex);
    }

    @Override
    public long getTotalValueCount() {
      return valueCount;
    }

    @Override
    public DataPage readPage() {
      final DataPage compressedPage;
      try {
        compressedPage = compressedPages.take().orElse(null);
      } catch (InterruptedException e) {
        throw new RuntimeException("Error reading parquet page data.", e);
      }
      if (compressedPage == null) {
        return null;
      }
      final int currentPageIndex = pageIndex++;

      if (null != blockDecryptor) {
        AesCipher.quickUpdatePageAAD(dataPageAAD, getPageOrdinal(currentPageIndex));
      }

      return compressedPage.accept(new DataPage.Visitor<DataPage>() {
        @Override
        public DataPage visit(DataPageV1 dataPageV1) {
          try {
            BytesInput bytes = dataPageV1.getBytes();
            BytesInput decompressed;

            if (options.getAllocator().isDirect() && options.useOffHeapDecryptBuffer()) {
              ByteBuffer byteBuffer = bytes.toByteBuffer(releaser);
              if (!byteBuffer.isDirect()) {
                throw new ParquetDecodingException("Expected a direct buffer");
              }
              if (blockDecryptor != null) {
                byteBuffer = blockDecryptor.decrypt(byteBuffer, dataPageAAD);
              }
              long compressedSize = byteBuffer.limit();

              ByteBuffer decompressedBuffer =
                  options.getAllocator().allocate(dataPageV1.getUncompressedSize());
              releaser.releaseLater(decompressedBuffer);
              long start = System.nanoTime();
              decompressor.decompress(
                  byteBuffer,
                  (int) compressedSize,
                  decompressedBuffer,
                  dataPageV1.getUncompressedSize());
              setDecompressMetrics(bytes, start);
              decompressedBuffer.flip();
              decompressed = BytesInput.from(decompressedBuffer);
            } else { // use on-heap buffer
              if (null != blockDecryptor) {
                bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
              }
              long start = System.nanoTime();
              decompressed = decompressor.decompress(bytes, dataPageV1.getUncompressedSize());
              setDecompressMetrics(bytes, start);
            }

            final DataPageV1 decompressedPage;
            if (offsetIndex == null) {
              decompressedPage = new DataPageV1(
                  decompressed,
                  dataPageV1.getValueCount(),
                  dataPageV1.getUncompressedSize(),
                  dataPageV1.getStatistics(),
                  dataPageV1.getRlEncoding(),
                  dataPageV1.getDlEncoding(),
                  dataPageV1.getValueEncoding());
            } else {
              long firstRowIndex = offsetIndex.getFirstRowIndex(currentPageIndex);
              decompressedPage = new DataPageV1(
                  decompressed,
                  dataPageV1.getValueCount(),
                  dataPageV1.getUncompressedSize(),
                  firstRowIndex,
                  Math.toIntExact(offsetIndex.getLastRowIndex(currentPageIndex, rowCount)
                      - firstRowIndex
                      + 1),
                  dataPageV1.getStatistics(),
                  dataPageV1.getRlEncoding(),
                  dataPageV1.getDlEncoding(),
                  dataPageV1.getValueEncoding());
            }
            if (dataPageV1.getCrc().isPresent()) {
              decompressedPage.setCrc(dataPageV1.getCrc().getAsInt());
            }
            return decompressedPage;
          } catch (IOException e) {
            throw new ParquetDecodingException("could not decompress page", e);
          }
        }

        @Override
        public DataPage visit(DataPageV2 dataPageV2) {
          if (!dataPageV2.isCompressed() && offsetIndex == null && null == blockDecryptor) {
            return dataPageV2;
          }
          BytesInput pageBytes = dataPageV2.getData();
          try {
            if (options.getAllocator().isDirect() && options.useOffHeapDecryptBuffer()) {
              ByteBuffer byteBuffer = pageBytes.toByteBuffer(releaser);
              if (!byteBuffer.isDirect()) {
                throw new ParquetDecodingException("Expected a direct buffer");
              }
              if (blockDecryptor != null) {
                byteBuffer = blockDecryptor.decrypt(byteBuffer, dataPageAAD);
              }
              long compressedSize = byteBuffer.limit();
              if (dataPageV2.isCompressed()) {
                int uncompressedSize = Math.toIntExact(dataPageV2.getUncompressedSize()
                    - dataPageV2.getDefinitionLevels().size()
                    - dataPageV2.getRepetitionLevels().size());
                ByteBuffer decompressedBuffer =
                    options.getAllocator().allocate(uncompressedSize);
                releaser.releaseLater(decompressedBuffer);
                long start = System.nanoTime();
                decompressor.decompress(
                    byteBuffer, (int) compressedSize, decompressedBuffer, uncompressedSize);
                setDecompressMetrics(pageBytes, start);
                decompressedBuffer.flip();
                pageBytes = BytesInput.from(decompressedBuffer);
              } else {
                pageBytes = BytesInput.from(byteBuffer);
              }
            } else {
              if (null != blockDecryptor) {
                pageBytes =
                    BytesInput.from(blockDecryptor.decrypt(pageBytes.toByteArray(), dataPageAAD));
              }
              if (dataPageV2.isCompressed()) {
                int uncompressedSize = Math.toIntExact(dataPageV2.getUncompressedSize()
                    - dataPageV2.getDefinitionLevels().size()
                    - dataPageV2.getRepetitionLevels().size());
                long start = System.nanoTime();
                pageBytes = decompressor.decompress(pageBytes, uncompressedSize);
                setDecompressMetrics(pageBytes, start);
              }
            }
          } catch (IOException e) {
            throw new ParquetDecodingException("could not decompress page", e);
          }

          final DataPageV2 decompressedPage;
          if (offsetIndex == null) {
            decompressedPage = DataPageV2.uncompressed(
                dataPageV2.getRowCount(),
                dataPageV2.getNullCount(),
                dataPageV2.getValueCount(),
                dataPageV2.getRepetitionLevels(),
                dataPageV2.getDefinitionLevels(),
                dataPageV2.getDataEncoding(),
                pageBytes,
                dataPageV2.getStatistics());
          } else {
            decompressedPage = DataPageV2.uncompressed(
                dataPageV2.getRowCount(),
                dataPageV2.getNullCount(),
                dataPageV2.getValueCount(),
                offsetIndex.getFirstRowIndex(currentPageIndex),
                dataPageV2.getRepetitionLevels(),
                dataPageV2.getDefinitionLevels(),
                dataPageV2.getDataEncoding(),
                pageBytes,
                dataPageV2.getStatistics());
          }
          if (dataPageV2.getCrc().isPresent()) {
            decompressedPage.setCrc(dataPageV2.getCrc().getAsInt());
          }
          return decompressedPage;
        }
      });
    }

    private void setDecompressMetrics(BytesInput bytes, long start) {
      final ParquetMetricsCallback metricsCallback = options.getMetricsCallback();
      if (metricsCallback != null) {
        long time = Math.max(System.nanoTime() - start, 0);
        long len = bytes.size();
        double throughput = ((double) len / time) * ((double) 1000_000_000L) / (1024 * 1024);
        LOG.debug(
            "Decompress block: Length: {} MB, Time: {} msecs, throughput: {} MB/s",
            len / (1024 * 1024),
            time / 1000_000L,
            throughput);
        metricsCallback.setDuration(ParquetFileReaderMetrics.DecompressTime.name(), time);
        metricsCallback.setValueLong(ParquetFileReaderMetrics.DecompressSize.name(), len);
        metricsCallback.setValueDouble(ParquetFileReaderMetrics.DecompressThroughput.name(), throughput);
      }
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (compressedDictionaryPage == null) {
        return null;
      }
      try {
        BytesInput bytes = compressedDictionaryPage.getBytes();
        if (null != blockDecryptor) {
          bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dictionaryPageAAD));
        }
        long start = System.nanoTime();
        setDecompressMetrics(bytes, start);
        DictionaryPage decompressedPage = new DictionaryPage(
            decompressor.decompress(bytes, compressedDictionaryPage.getUncompressedSize()),
            compressedDictionaryPage.getDictionarySize(),
            compressedDictionaryPage.getEncoding());
        if (compressedDictionaryPage.getCrc().isPresent()) {
          decompressedPage.setCrc(compressedDictionaryPage.getCrc().getAsInt());
        }
        return decompressedPage;
      } catch (IOException e) {
        throw new ParquetDecodingException("Could not decompress dictionary page", e);
      }
    }

    private void releaseBuffers() {
      releaser.close();
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageReader> readers =
      new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
  private final long rowCount;
  private final long rowIndexOffset;
  private final RowRanges rowRanges;
  private ByteBufferAllocator allocator;
  private ByteBufferReleaser releaser;

  public ColumnChunkPageReadStore(long rowCount) {
    this(rowCount, -1);
  }

  ColumnChunkPageReadStore(RowRanges rowRanges) {
    this(rowRanges, -1);
  }

  ColumnChunkPageReadStore(long rowCount, long rowIndexOffset) {
    this.rowCount = rowCount;
    this.rowIndexOffset = rowIndexOffset;
    rowRanges = null;
  }

  ColumnChunkPageReadStore(RowRanges rowRanges, long rowIndexOffset) {
    this.rowRanges = rowRanges;
    this.rowIndexOffset = rowIndexOffset;
    rowCount = rowRanges.rowCount();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public Optional<Long> getRowIndexOffset() {
    return rowIndexOffset < 0 ? Optional.empty() : Optional.of(rowIndexOffset);
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor path) {
    final PageReader pageReader = readers.get(path);
    if (pageReader == null) {
      throw new IllegalArgumentException(path + " is not in the store: " + readers.keySet() + " " + rowCount);
    }
    return pageReader;
  }

  @Override
  public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
    return readers.get(descriptor).readDictionaryPage();
  }

  @Override
  public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    return rowRanges == null ? Optional.empty() : Optional.of(rowRanges.iterator());
  }

  void addColumn(ColumnDescriptor path, ColumnChunkPageReader reader) {
    if (readers.put(path, reader) != null) {
      throw new RuntimeException(path + " was added twice");
    }
  }

  void setReleaser(ByteBufferReleaser releaser) {
    this.releaser = releaser;
  }

  @Override
  public void close() {
    for (ColumnChunkPageReader reader : readers.values()) {
      reader.releaseBuffers();
      try {
        reader.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    releaser.close();
  }
}
