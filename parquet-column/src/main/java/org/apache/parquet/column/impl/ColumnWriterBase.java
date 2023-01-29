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

import java.io.IOException;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.bytes.BytesInput;

/**
 * Base implementation for {@link ColumnWriter} to be extended to specialize for V1 and V2 pages.
 */
abstract class ColumnWriterBase implements ColumnWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnWriterBase.class);

  // By default: Debugging disabled this way (using the "if (DEBUG)" IN the methods) to allow
  // the java compiler (not the JIT) to remove the unused statements during build time.
  private static final boolean DEBUG = false;

  final ColumnDescriptor path;
  final PageWriter pageWriter;
  private ValuesWriter repetitionLevelColumn;
  private ValuesWriter definitionLevelColumn;
  private ValuesWriter dataColumn;
  private int valueCount;

  private Statistics<?> statistics;
  private long rowsWrittenSoFar = 0;
  private int pageRowCount;

  private final BloomFilterWriter bloomFilterWriter;
  private final BloomFilter bloomFilter;

  ColumnWriterBase(
      ColumnDescriptor path,
      PageWriter pageWriter,
      ParquetProperties props) {
    this(path, pageWriter, null, props);
  }

  ColumnWriterBase(
    ColumnDescriptor path,
    PageWriter pageWriter,
    BloomFilterWriter bloomFilterWriter,
    ParquetProperties props
  ) {
    this.path = path;
    this.pageWriter = pageWriter;
    resetStatistics();

    this.repetitionLevelColumn = createRLWriter(props, path);
    this.definitionLevelColumn = createDLWriter(props, path);
    this.dataColumn = props.newValuesWriter(path);

    this.bloomFilterWriter = bloomFilterWriter;
    if (bloomFilterWriter == null) {
      this.bloomFilter = null;
      return;
    }
    int maxBloomFilterSize = props.getMaxBloomFilterBytes();

    OptionalLong ndv = props.getBloomFilterNDV(path);
    OptionalDouble fpp = props.getBloomFilterFPP(path);
    // If user specify the column NDV, we construct Bloom filter from it.
    if (ndv.isPresent()) {
      int optimalNumOfBits = BlockSplitBloomFilter.optimalNumOfBits(ndv.getAsLong(), fpp.getAsDouble());
      this.bloomFilter = new BlockSplitBloomFilter(optimalNumOfBits / 8, maxBloomFilterSize);
    } else {
      this.bloomFilter = new BlockSplitBloomFilter(maxBloomFilterSize);
    }
  }

  abstract ValuesWriter createRLWriter(ParquetProperties props, ColumnDescriptor path);

  abstract ValuesWriter createDLWriter(ParquetProperties props, ColumnDescriptor path);

  private void log(Object value, int r, int d) {
    LOG.debug("{} {} r:{} d:{}", path, value, r, d);
  }

  private void resetStatistics() {
    this.statistics = Statistics.createStats(path.getPrimitiveType());
  }

  private void definitionLevel(int definitionLevel) {
    definitionLevelColumn.writeInteger(definitionLevel);
  }

  private void repetitionLevel(int repetitionLevel) {
    repetitionLevelColumn.writeInteger(repetitionLevel);
    assert pageRowCount == 0 ? repetitionLevel == 0 : true : "Every page shall start on record boundaries";
    if (repetitionLevel == 0) {
      ++pageRowCount;
    }
  }

  /**
   * Writes the current null value
   *
   * @param repetitionLevel
   * @param definitionLevel
   */
  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (DEBUG)
      log(null, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    statistics.incrementNumNulls();
    ++valueCount;
  }

  @Override
  public void close() {
    // Close the Values writers.
    repetitionLevelColumn.close();
    definitionLevelColumn.close();
    dataColumn.close();
  }

  @Override
  public long getBufferedSizeInMemory() {
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize()
        + pageWriter.getMemSize();
  }

  private void updateBloomFilter(int value) {
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  private void updateBloomFilter(long value) {
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  private void updateBloomFilter(double value) {
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  private void updateBloomFilter(float value) {
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  private void updateBloomFilter(Binary value) {
    if (bloomFilter != null) {
      bloomFilter.insertHash(bloomFilter.hash(value));
    }
  }

  /**
   * Writes the current value
   *
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    if (DEBUG)
      log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeDouble(value);
    statistics.updateStats(value);
    updateBloomFilter(value);
    ++valueCount;
  }

  /**
   * Writes the current value
   *
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    if (DEBUG)
      log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeFloat(value);
    statistics.updateStats(value);
    updateBloomFilter(value);
    ++valueCount;
  }

  /**
   * Writes the current value
   *
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    if (DEBUG)
      log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeBytes(value);
    statistics.updateStats(value);
    updateBloomFilter(value);
    ++valueCount;
  }

  /**
   * Writes the current value
   *
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (DEBUG)
      log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeBoolean(value);
    statistics.updateStats(value);
    ++valueCount;
  }

  /**
   * Writes the current value
   *
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (DEBUG)
      log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeInteger(value);
    statistics.updateStats(value);
    updateBloomFilter(value);
    ++valueCount;
  }

  /**
   * Writes the current value
   *
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    if (DEBUG)
      log(value, repetitionLevel, definitionLevel);
    repetitionLevel(repetitionLevel);
    definitionLevel(definitionLevel);
    dataColumn.writeLong(value);
    statistics.updateStats(value);
    updateBloomFilter(value);
    ++valueCount;
  }

  /**
   * Finalizes the Column chunk. Possibly adding extra pages if needed (dictionary, ...)
   * Is called right after writePage
   */
  void finalizeColumnChunk() {
    final DictionaryPage dictionaryPage = dataColumn.toDictPageAndClose();
    if (dictionaryPage != null) {
      if (DEBUG)
        LOG.debug("write dictionary");
      try {
        pageWriter.writeDictionaryPage(dictionaryPage);
      } catch (IOException e) {
        throw new ParquetEncodingException("could not write dictionary page for " + path, e);
      }
      dataColumn.resetDictionary();
    }

    if (bloomFilterWriter != null && bloomFilter != null) {
      bloomFilterWriter.writeBloomFilter(bloomFilter);
    }
  }

  /**
   * Used to decide when to write a page
   *
   * @return the number of bytes of memory used to buffer the current data
   */
  long getCurrentPageBufferedSize() {
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize();
  }

  /**
   * Used to decide when to write a page or row group
   *
   * @return the number of bytes of memory used to buffer the current data and the previously written pages
   */
  long getTotalBufferedSize() {
    return repetitionLevelColumn.getBufferedSize()
        + definitionLevelColumn.getBufferedSize()
        + dataColumn.getBufferedSize()
        + pageWriter.getMemSize();
  }

  /**
   * @return actual memory used
   */
  long allocatedSize() {
    return repetitionLevelColumn.getAllocatedSize()
        + definitionLevelColumn.getAllocatedSize()
        + dataColumn.getAllocatedSize()
        + pageWriter.allocatedSize();
  }

  /**
   * @param indent
   *          a prefix to format lines
   * @return a formatted string showing how memory is used
   */
  String memUsageString(String indent) {
    StringBuilder b = new StringBuilder(indent).append(path).append(" {\n");
    b.append(indent).append(" r:").append(repetitionLevelColumn.getAllocatedSize()).append(" bytes\n");
    b.append(indent).append(" d:").append(definitionLevelColumn.getAllocatedSize()).append(" bytes\n");
    b.append(dataColumn.memUsageString(indent + "  data:")).append("\n");
    b.append(pageWriter.memUsageString(indent + "  pages:")).append("\n");
    b.append(indent).append(String.format("  total: %,d/%,d", getTotalBufferedSize(), allocatedSize())).append("\n");
    b.append(indent).append("}\n");
    return b.toString();
  }

  long getRowsWrittenSoFar() {
    return this.rowsWrittenSoFar;
  }

  /**
   * Writes the current data to a new page in the page store
   */
  void writePage() {
    if (valueCount == 0) {
      throw new ParquetEncodingException("writing empty page");
    }
    this.rowsWrittenSoFar += pageRowCount;
    if (DEBUG)
      LOG.debug("write page");
    try {
      writePage(pageRowCount, valueCount, statistics, repetitionLevelColumn, definitionLevelColumn, dataColumn);
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page for " + path, e);
    }
    repetitionLevelColumn.reset();
    definitionLevelColumn.reset();
    dataColumn.reset();
    valueCount = 0;
    resetStatistics();
    pageRowCount = 0;
  }

  abstract void writePage(int rowCount, int valueCount, Statistics<?> statistics, ValuesWriter repetitionLevels,
      ValuesWriter definitionLevels, ValuesWriter values) throws IOException;
}
