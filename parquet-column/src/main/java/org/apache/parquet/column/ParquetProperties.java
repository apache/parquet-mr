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
package org.apache.parquet.column;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory;
import org.apache.parquet.column.values.factory.ValuesWriterFactory;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.MessageType;

/**
 * This class represents all the configurable Parquet properties.
 */
public class ParquetProperties {

  public static final int DEFAULT_PAGE_SIZE = 1024 * 1024;
  public static final int DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = true;
  public static final boolean DEFAULT_IS_BYTE_STREAM_SPLIT_ENABLED = false;
  public static final boolean DEFAULT_IS_EXTENDED_BYTE_STREAM_SPLIT_ENABLED = false;
  public static final WriterVersion DEFAULT_WRITER_VERSION = WriterVersion.PARQUET_1_0;
  public static final boolean DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK = true;
  public static final int DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  public static final int DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;
  public static final int DEFAULT_PAGE_VALUE_COUNT_THRESHOLD = Integer.MAX_VALUE / 2;
  public static final int DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH = 64;
  public static final int DEFAULT_STATISTICS_TRUNCATE_LENGTH = Integer.MAX_VALUE;
  public static final int DEFAULT_PAGE_ROW_COUNT_LIMIT = 20_000;
  public static final int DEFAULT_MAX_BLOOM_FILTER_BYTES = 1024 * 1024;
  public static final boolean DEFAULT_BLOOM_FILTER_ENABLED = false;
  public static final double DEFAULT_BLOOM_FILTER_FPP = 0.01;
  public static final boolean DEFAULT_ADAPTIVE_BLOOM_FILTER_ENABLED = false;
  public static final int DEFAULT_BLOOM_FILTER_CANDIDATES_NUMBER = 5;

  public static final boolean DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED = true;

  public static final ValuesWriterFactory DEFAULT_VALUES_WRITER_FACTORY = new DefaultValuesWriterFactory();

  private static final int MIN_SLAB_SIZE = 64;

  public enum WriterVersion {
    PARQUET_1_0("v1"),
    PARQUET_2_0("v2");

    private final String shortName;

    WriterVersion(String shortname) {
      this.shortName = shortname;
    }

    public static WriterVersion fromString(String name) {
      for (WriterVersion v : WriterVersion.values()) {
        if (v.shortName.equals(name)) {
          return v;
        }
      }
      // Throws IllegalArgumentException if name does not exact match with enum name
      return WriterVersion.valueOf(name);
    }
  }

  private final int initialSlabSize;
  private final int pageSizeThreshold;
  private final int pageValueCountThreshold;
  private final int dictionaryPageSizeThreshold;
  private final WriterVersion writerVersion;
  private final ColumnProperty<Boolean> dictionaryEnabled;
  private final int minRowCountForPageSizeCheck;
  private final int maxRowCountForPageSizeCheck;
  private final boolean estimateNextSizeCheck;
  private final ByteBufferAllocator allocator;
  private final ValuesWriterFactory valuesWriterFactory;
  private final int columnIndexTruncateLength;
  private final int statisticsTruncateLength;

  // The expected NDV (number of distinct values) for each columns
  private final ColumnProperty<Long> bloomFilterNDVs;
  private final ColumnProperty<Double> bloomFilterFPPs;
  private final int maxBloomFilterBytes;
  private final ColumnProperty<Boolean> bloomFilterEnabled;
  private final ColumnProperty<Boolean> adaptiveBloomFilterEnabled;
  private final ColumnProperty<Integer> numBloomFilterCandidates;
  private final int pageRowCountLimit;
  private final boolean pageWriteChecksumEnabled;
  private final boolean enableByteStreamSplit;
  private final boolean enableExtendedByteStreamSplit;
  private final Map<String, String> extraMetaData;

  private ParquetProperties(Builder builder) {
    this.pageSizeThreshold = builder.pageSize;
    this.pageValueCountThreshold = builder.pageValueCountThreshold;
    this.initialSlabSize =
        CapacityByteArrayOutputStream.initialSlabSizeHeuristic(MIN_SLAB_SIZE, pageSizeThreshold, 10);
    this.dictionaryPageSizeThreshold = builder.dictPageSize;
    this.writerVersion = builder.writerVersion;
    this.dictionaryEnabled = builder.enableDict.build();
    this.minRowCountForPageSizeCheck = builder.minRowCountForPageSizeCheck;
    this.maxRowCountForPageSizeCheck = builder.maxRowCountForPageSizeCheck;
    this.estimateNextSizeCheck = builder.estimateNextSizeCheck;
    this.allocator = builder.allocator;

    this.valuesWriterFactory = builder.valuesWriterFactory;
    this.columnIndexTruncateLength = builder.columnIndexTruncateLength;
    this.statisticsTruncateLength = builder.statisticsTruncateLength;
    this.bloomFilterNDVs = builder.bloomFilterNDVs.build();
    this.bloomFilterFPPs = builder.bloomFilterFPPs.build();
    this.bloomFilterEnabled = builder.bloomFilterEnabled.build();
    this.maxBloomFilterBytes = builder.maxBloomFilterBytes;
    this.adaptiveBloomFilterEnabled = builder.adaptiveBloomFilterEnabled.build();
    this.numBloomFilterCandidates = builder.numBloomFilterCandidates.build();
    this.pageRowCountLimit = builder.pageRowCountLimit;
    this.pageWriteChecksumEnabled = builder.pageWriteChecksumEnabled;
    this.enableByteStreamSplit = builder.enableByteStreamSplit;
    this.enableExtendedByteStreamSplit = builder.enableExtendedByteStreamSplit;
    this.extraMetaData = builder.extraMetaData;
  }

  public ValuesWriter newRepetitionLevelWriter(ColumnDescriptor path) {
    return newColumnDescriptorValuesWriter(path.getMaxRepetitionLevel());
  }

  public ValuesWriter newDefinitionLevelWriter(ColumnDescriptor path) {
    return newColumnDescriptorValuesWriter(path.getMaxDefinitionLevel());
  }

  private ValuesWriter newColumnDescriptorValuesWriter(int maxLevel) {
    if (maxLevel == 0) {
      return new DevNullValuesWriter();
    } else {
      return new RunLengthBitPackingHybridValuesWriter(
          getWidthFromMaxInt(maxLevel), MIN_SLAB_SIZE, pageSizeThreshold, allocator);
    }
  }

  public RunLengthBitPackingHybridEncoder newRepetitionLevelEncoder(ColumnDescriptor path) {
    return newLevelEncoder(path.getMaxRepetitionLevel());
  }

  public RunLengthBitPackingHybridEncoder newDefinitionLevelEncoder(ColumnDescriptor path) {
    return newLevelEncoder(path.getMaxDefinitionLevel());
  }

  private RunLengthBitPackingHybridEncoder newLevelEncoder(int maxLevel) {
    return new RunLengthBitPackingHybridEncoder(
        getWidthFromMaxInt(maxLevel), MIN_SLAB_SIZE, pageSizeThreshold, allocator);
  }

  public ValuesWriter newValuesWriter(ColumnDescriptor path) {
    return valuesWriterFactory.newValuesWriter(path);
  }

  public int getPageSizeThreshold() {
    return pageSizeThreshold;
  }

  public int getPageValueCountThreshold() {
    return pageValueCountThreshold;
  }

  public int getInitialSlabSize() {
    return initialSlabSize;
  }

  public int getDictionaryPageSizeThreshold() {
    return dictionaryPageSizeThreshold;
  }

  public WriterVersion getWriterVersion() {
    return writerVersion;
  }

  @Deprecated
  public boolean isEnableDictionary() {
    return dictionaryEnabled.getDefaultValue();
  }

  public boolean isDictionaryEnabled(ColumnDescriptor column) {
    return dictionaryEnabled.getValue(column);
  }

  public boolean isByteStreamSplitEnabled() {
    return enableByteStreamSplit;
  }

  public boolean isExtendedByteStreamSplitEnabled() {
    return enableExtendedByteStreamSplit;
  }

  public ByteBufferAllocator getAllocator() {
    return allocator;
  }

  public ColumnWriteStore newColumnWriteStore(MessageType schema, PageWriteStore pageStore) {
    switch (writerVersion) {
      case PARQUET_1_0:
        return new ColumnWriteStoreV1(schema, pageStore, this);
      case PARQUET_2_0:
        return new ColumnWriteStoreV2(schema, pageStore, this);
      default:
        throw new IllegalArgumentException("unknown version " + writerVersion);
    }
  }

  public ColumnWriteStore newColumnWriteStore(
      MessageType schema, PageWriteStore pageStore, BloomFilterWriteStore bloomFilterWriteStore) {
    switch (writerVersion) {
      case PARQUET_1_0:
        return new ColumnWriteStoreV1(schema, pageStore, bloomFilterWriteStore, this);
      case PARQUET_2_0:
        return new ColumnWriteStoreV2(schema, pageStore, bloomFilterWriteStore, this);
      default:
        throw new IllegalArgumentException("unknown version " + writerVersion);
    }
  }

  public int getMinRowCountForPageSizeCheck() {
    return minRowCountForPageSizeCheck;
  }

  public int getMaxRowCountForPageSizeCheck() {
    return maxRowCountForPageSizeCheck;
  }

  public ValuesWriterFactory getValuesWriterFactory() {
    return valuesWriterFactory;
  }

  public int getColumnIndexTruncateLength() {
    return columnIndexTruncateLength;
  }

  public int getStatisticsTruncateLength() {
    return statisticsTruncateLength;
  }

  public boolean estimateNextSizeCheck() {
    return estimateNextSizeCheck;
  }

  public int getPageRowCountLimit() {
    return pageRowCountLimit;
  }

  public boolean getPageWriteChecksumEnabled() {
    return pageWriteChecksumEnabled;
  }

  public OptionalLong getBloomFilterNDV(ColumnDescriptor column) {
    Long ndv = bloomFilterNDVs.getValue(column);
    return ndv == null ? OptionalLong.empty() : OptionalLong.of(ndv);
  }

  public OptionalDouble getBloomFilterFPP(ColumnDescriptor column) {
    Double fpp = bloomFilterFPPs.getValue(column);
    return fpp == null ? OptionalDouble.empty() : OptionalDouble.of(fpp);
  }

  public boolean isBloomFilterEnabled(ColumnDescriptor column) {
    return bloomFilterEnabled.getValue(column);
  }

  public int getMaxBloomFilterBytes() {
    return maxBloomFilterBytes;
  }

  public boolean getAdaptiveBloomFilterEnabled(ColumnDescriptor column) {
    return adaptiveBloomFilterEnabled.getValue(column);
  }

  public int getBloomFilterCandidatesCount(ColumnDescriptor column) {
    return numBloomFilterCandidates.getValue(column);
  }

  public Map<String, String> getExtraMetaData() {
    return extraMetaData;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder copy(ParquetProperties toCopy) {
    return new Builder(toCopy);
  }

  @Override
  public String toString() {
    return "Parquet page size to " + getPageSizeThreshold() + '\n'
        + "Parquet dictionary page size to " + getDictionaryPageSizeThreshold() + '\n'
        + "Dictionary is " + dictionaryEnabled + '\n'
        + "Writer version is: " + getWriterVersion() + '\n'
        + "Page size checking is: " + (estimateNextSizeCheck() ? "estimated" : "constant") + '\n'
        + "Min row count for page size check is: " + getMinRowCountForPageSizeCheck() + '\n'
        + "Max row count for page size check is: " + getMaxRowCountForPageSizeCheck() + '\n'
        + "Truncate length for column indexes is: " + getColumnIndexTruncateLength() + '\n'
        + "Truncate length for statistics min/max  is: " + getStatisticsTruncateLength() + '\n'
        + "Bloom filter enabled: " + bloomFilterEnabled + '\n'
        + "Max Bloom filter size for a column is " + getMaxBloomFilterBytes() + '\n'
        + "Bloom filter expected number of distinct values are: " + bloomFilterNDVs + '\n'
        + "Bloom filter false positive probabilities are: " + bloomFilterFPPs + '\n'
        + "Page row count limit to " + getPageRowCountLimit() + '\n'
        + "Writing page checksums is: " + (getPageWriteChecksumEnabled() ? "on" : "off");
  }

  public static class Builder {
    private int pageSize = DEFAULT_PAGE_SIZE;
    private int dictPageSize = DEFAULT_DICTIONARY_PAGE_SIZE;
    private final ColumnProperty.Builder<Boolean> enableDict;
    private WriterVersion writerVersion = DEFAULT_WRITER_VERSION;
    private int minRowCountForPageSizeCheck = DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
    private int maxRowCountForPageSizeCheck = DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK;
    private int pageValueCountThreshold = DEFAULT_PAGE_VALUE_COUNT_THRESHOLD;
    private boolean estimateNextSizeCheck = DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK;
    private ByteBufferAllocator allocator = new HeapByteBufferAllocator();
    private ValuesWriterFactory valuesWriterFactory = DEFAULT_VALUES_WRITER_FACTORY;
    private int columnIndexTruncateLength = DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
    private int statisticsTruncateLength = DEFAULT_STATISTICS_TRUNCATE_LENGTH;
    private final ColumnProperty.Builder<Long> bloomFilterNDVs;
    private final ColumnProperty.Builder<Double> bloomFilterFPPs;
    private int maxBloomFilterBytes = DEFAULT_MAX_BLOOM_FILTER_BYTES;
    private final ColumnProperty.Builder<Boolean> adaptiveBloomFilterEnabled;
    private final ColumnProperty.Builder<Integer> numBloomFilterCandidates;
    private final ColumnProperty.Builder<Boolean> bloomFilterEnabled;
    private int pageRowCountLimit = DEFAULT_PAGE_ROW_COUNT_LIMIT;
    private boolean pageWriteChecksumEnabled = DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED;
    private boolean enableByteStreamSplit = DEFAULT_IS_BYTE_STREAM_SPLIT_ENABLED;
    private boolean enableExtendedByteStreamSplit = DEFAULT_IS_EXTENDED_BYTE_STREAM_SPLIT_ENABLED;
    private Map<String, String> extraMetaData = new HashMap<>();

    private Builder() {
      enableDict = ColumnProperty.<Boolean>builder().withDefaultValue(DEFAULT_IS_DICTIONARY_ENABLED);
      bloomFilterEnabled = ColumnProperty.<Boolean>builder().withDefaultValue(DEFAULT_BLOOM_FILTER_ENABLED);
      bloomFilterNDVs = ColumnProperty.<Long>builder().withDefaultValue(null);
      bloomFilterFPPs = ColumnProperty.<Double>builder().withDefaultValue(DEFAULT_BLOOM_FILTER_FPP);
      adaptiveBloomFilterEnabled =
          ColumnProperty.<Boolean>builder().withDefaultValue(DEFAULT_ADAPTIVE_BLOOM_FILTER_ENABLED);
      numBloomFilterCandidates =
          ColumnProperty.<Integer>builder().withDefaultValue(DEFAULT_BLOOM_FILTER_CANDIDATES_NUMBER);
    }

    private Builder(ParquetProperties toCopy) {
      this.pageSize = toCopy.pageSizeThreshold;
      this.enableDict = ColumnProperty.<Boolean>builder(toCopy.dictionaryEnabled);
      this.dictPageSize = toCopy.dictionaryPageSizeThreshold;
      this.writerVersion = toCopy.writerVersion;
      this.minRowCountForPageSizeCheck = toCopy.minRowCountForPageSizeCheck;
      this.maxRowCountForPageSizeCheck = toCopy.maxRowCountForPageSizeCheck;
      this.estimateNextSizeCheck = toCopy.estimateNextSizeCheck;
      this.valuesWriterFactory = toCopy.valuesWriterFactory;
      this.allocator = toCopy.allocator;
      this.pageRowCountLimit = toCopy.pageRowCountLimit;
      this.pageWriteChecksumEnabled = toCopy.pageWriteChecksumEnabled;
      this.bloomFilterNDVs = ColumnProperty.<Long>builder(toCopy.bloomFilterNDVs);
      this.bloomFilterFPPs = ColumnProperty.<Double>builder(toCopy.bloomFilterFPPs);
      this.bloomFilterEnabled = ColumnProperty.<Boolean>builder(toCopy.bloomFilterEnabled);
      this.adaptiveBloomFilterEnabled = ColumnProperty.<Boolean>builder(toCopy.adaptiveBloomFilterEnabled);
      this.numBloomFilterCandidates = ColumnProperty.<Integer>builder(toCopy.numBloomFilterCandidates);
      this.maxBloomFilterBytes = toCopy.maxBloomFilterBytes;
      this.enableByteStreamSplit = toCopy.enableByteStreamSplit;
      this.enableExtendedByteStreamSplit = toCopy.enableExtendedByteStreamSplit;
      this.extraMetaData = toCopy.extraMetaData;
    }

    /**
     * Set the Parquet format page size.
     *
     * @param pageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public Builder withPageSize(int pageSize) {
      Preconditions.checkArgument(pageSize > 0, "Invalid page size (negative): %s", pageSize);
      this.pageSize = pageSize;
      return this;
    }

    /**
     * Enable or disable dictionary encoding.
     *
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public Builder withDictionaryEncoding(boolean enableDictionary) {
      this.enableDict.withDefaultValue(enableDictionary);
      return this;
    }

    /**
     * Enable or disable dictionary encoding for the specified column.
     *
     * @param columnPath       the path of the column (dot-string)
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public Builder withDictionaryEncoding(String columnPath, boolean enableDictionary) {
      this.enableDict.withValue(columnPath, enableDictionary);
      return this;
    }

    /**
     * Enable or disable BYTE_STREAM_SPLIT encoding for FLOAT and DOUBLE columns.
     *
     * @param enable whether BYTE_STREAM_SPLIT encoding should be enabled
     * @return this builder for method chaining.
     */
    public Builder withByteStreamSplitEncoding(boolean enable) {
      this.enableByteStreamSplit = enable;
      this.enableExtendedByteStreamSplit &= enable;
      return this;
    }

    /**
     * Enable or disable BYTE_STREAM_SPLIT encoding for FLOAT, DOUBLE, INT32, INT64 and FIXED_LEN_BYTE_ARRAY columns.
     *
     * @param enable whether BYTE_STREAM_SPLIT encoding should be enabled
     * @return this builder for method chaining.
     */
    public Builder withExtendedByteStreamSplitEncoding(boolean enable) {
      this.enableByteStreamSplit |= enable;
      this.enableExtendedByteStreamSplit = enable;
      return this;
    }

    /**
     * Set the Parquet format dictionary page size.
     *
     * @param dictionaryPageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public Builder withDictionaryPageSize(int dictionaryPageSize) {
      Preconditions.checkArgument(
          dictionaryPageSize > 0, "Invalid dictionary page size (negative): %s", dictionaryPageSize);
      this.dictPageSize = dictionaryPageSize;
      return this;
    }

    /**
     * Set the {@link WriterVersion format version}.
     *
     * @param version a {@code WriterVersion}
     * @return this builder for method chaining.
     */
    public Builder withWriterVersion(WriterVersion version) {
      this.writerVersion = version;
      return this;
    }

    public Builder withMinRowCountForPageSizeCheck(int min) {
      Preconditions.checkArgument(min > 0, "Invalid row count for page size check (negative): %s", min);
      this.minRowCountForPageSizeCheck = min;
      return this;
    }

    public Builder withMaxRowCountForPageSizeCheck(int max) {
      Preconditions.checkArgument(max > 0, "Invalid row count for page size check (negative): %s", max);
      this.maxRowCountForPageSizeCheck = max;
      return this;
    }

    public Builder withPageValueCountThreshold(int value) {
      Preconditions.checkArgument(value > 0, "Invalid page value count threshold (negative): %s", value);
      this.pageValueCountThreshold = value;
      return this;
    }

    // Do not attempt to predict next size check.  Prevents issues with rows that vary significantly in size.
    public Builder estimateRowCountForPageSizeCheck(boolean estimateNextSizeCheck) {
      this.estimateNextSizeCheck = estimateNextSizeCheck;
      return this;
    }

    public Builder withAllocator(ByteBufferAllocator allocator) {
      this.allocator = Objects.requireNonNull(allocator, "ByteBufferAllocator cannot be null");
      return this;
    }

    public Builder withValuesWriterFactory(ValuesWriterFactory factory) {
      this.valuesWriterFactory = Objects.requireNonNull(factory, "ValuesWriterFactory cannot be null");
      return this;
    }

    public Builder withColumnIndexTruncateLength(int length) {
      Preconditions.checkArgument(
          length > 0, "Invalid column index min/max truncate length (negative or zero) : %s", length);
      this.columnIndexTruncateLength = length;
      return this;
    }

    public Builder withStatisticsTruncateLength(int length) {
      Preconditions.checkArgument(
          length > 0, "Invalid statistics min/max truncate length (negative or zero) : %s", length);
      this.statisticsTruncateLength = length;
      return this;
    }

    /**
     * Set max Bloom filter bytes for related columns.
     *
     * @param maxBloomFilterBytes the max bytes of a Bloom filter bitset for a column.
     * @return this builder for method chaining
     */
    public Builder withMaxBloomFilterBytes(int maxBloomFilterBytes) {
      this.maxBloomFilterBytes = maxBloomFilterBytes;
      return this;
    }

    /**
     * Set Bloom filter NDV (number of distinct values) for the specified column.
     * If set for a column then the writing of the bloom filter for that column will be automatically enabled (see
     * {@link #withBloomFilterEnabled(String, boolean)}).
     *
     * @param columnPath the path of the column (dot-string)
     * @param ndv        the NDV of the column
     * @return this builder for method chaining
     */
    public Builder withBloomFilterNDV(String columnPath, long ndv) {
      Preconditions.checkArgument(ndv > 0, "Invalid NDV for column \"%s\": %s", columnPath, ndv);
      this.bloomFilterNDVs.withValue(columnPath, ndv);
      // Setting an NDV for a column implies writing a bloom filter
      this.bloomFilterEnabled.withValue(columnPath, true);
      return this;
    }

    public Builder withBloomFilterFPP(String columnPath, double fpp) {
      Preconditions.checkArgument(fpp > 0.0 && fpp < 1.0, "Invalid FPP for column \"%s\": %s", columnPath, fpp);
      this.bloomFilterFPPs.withValue(columnPath, fpp);
      return this;
    }

    /**
     * Enable or disable the bloom filter for the columns not specified by
     * {@link #withBloomFilterEnabled(String, boolean)}.
     *
     * @param enabled whether bloom filter shall be enabled for all columns
     * @return this builder for method chaining
     */
    public Builder withBloomFilterEnabled(boolean enabled) {
      this.bloomFilterEnabled.withDefaultValue(enabled);
      return this;
    }

    /**
     * Whether to use adaptive bloom filter to automatically adjust the bloom filter size according to
     * `parquet.bloom.filter.max.bytes`.
     * If NDV (number of distinct values) for a specified column is set, it will be ignored
     *
     * @param enabled whether to use adaptive bloom filter
     */
    public Builder withAdaptiveBloomFilterEnabled(boolean enabled) {
      this.adaptiveBloomFilterEnabled.withDefaultValue(enabled);
      return this;
    }

    /**
     * When `AdaptiveBloomFilter` is enabled, set how many bloom filter candidates to use.
     *
     * @param columnPath the path of the column (dot-string)
     * @param number     the number of candidates
     */
    public Builder withBloomFilterCandidatesNumber(String columnPath, int number) {
      Preconditions.checkArgument(
          number > 0, "Invalid candidates number for column \"%s\": %d", columnPath, number);
      this.numBloomFilterCandidates.withDefaultValue(number);
      return this;
    }

    /**
     * Enable or disable the bloom filter for the specified column.
     * One may either disable bloom filters for all columns by invoking {@link #withBloomFilterEnabled(boolean)} with a
     * {@code false} value and then enable the bloom filters for the required columns one-by-one by invoking this
     * method or vice versa.
     *
     * @param columnPath the path of the column (dot-string)
     * @param enabled    whether bloom filter shall be enabled
     * @return this builder for method chaining
     */
    public Builder withBloomFilterEnabled(String columnPath, boolean enabled) {
      this.bloomFilterEnabled.withValue(columnPath, enabled);
      return this;
    }

    public Builder withPageRowCountLimit(int rowCount) {
      Preconditions.checkArgument(rowCount > 0, "Invalid row count limit for pages: %s", rowCount);
      pageRowCountLimit = rowCount;
      return this;
    }

    public Builder withPageWriteChecksumEnabled(boolean val) {
      this.pageWriteChecksumEnabled = val;
      return this;
    }

    public Builder withExtraMetaData(Map<String, String> extraMetaData) {
      this.extraMetaData = extraMetaData;
      return this;
    }

    public ParquetProperties build() {
      ParquetProperties properties = new ParquetProperties(this);
      // we pass a constructed but uninitialized factory to ParquetProperties above as currently
      // creation of ValuesWriters is invoked from within ParquetProperties. In the future
      // we'd like to decouple that and won't need to pass an object to properties and then pass the
      // properties to the object.
      valuesWriterFactory.initialize(properties);

      return properties;
    }
  }
}
