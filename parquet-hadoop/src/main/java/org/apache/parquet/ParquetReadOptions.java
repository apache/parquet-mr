/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetInputFormat.BLOOM_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.COLUMN_CHUNK_BUFFER_SIZE;
import static org.apache.parquet.hadoop.ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.DICTIONARY_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.RECORD_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.STATS_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;
import static org.apache.parquet.hadoop.UnmaterializableRecordCounter.BAD_RECORD_THRESHOLD_CONF_KEY;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetMetricsCallback;
import org.apache.parquet.hadoop.util.HadoopCodecs;

// Internal use only
public class ParquetReadOptions {

  private static final String ALLOCATION_SIZE = "parquet.read.allocation.size";

  private static final boolean RECORD_FILTERING_ENABLED_DEFAULT = true;
  private static final boolean STATS_FILTERING_ENABLED_DEFAULT = true;
  private static final boolean DICTIONARY_FILTERING_ENABLED_DEFAULT = true;
  private static final boolean COLUMN_INDEX_FILTERING_ENABLED_DEFAULT = true;
  private static final int ALLOCATION_SIZE_DEFAULT = 8388608; // 8MB
  private static final boolean PAGE_VERIFY_CHECKSUM_ENABLED_DEFAULT = false;
  private static final boolean BLOOM_FILTER_ENABLED_DEFAULT = true;
  private static final boolean USE_OFF_HEAP_DECRYPT_BUFFER_DEFAULT = false;

  private static final int COLUMN_CHUNK_BUFFER_SIZE_DEFAULT = -1;

  private final boolean useSignedStringMinMax;
  private final boolean useStatsFilter;
  private final boolean useDictionaryFilter;
  private final boolean useRecordFilter;
  private final boolean useColumnIndexFilter;
  private final boolean usePageChecksumVerification;
  private final boolean useBloomFilter;
  private final boolean useOffHeapDecryptBuffer;

  private final int columnChunkBufferSize;
  private final FilterCompat.Filter recordFilter;
  private final ParquetMetadataConverter.MetadataFilter metadataFilter;
  private final CompressionCodecFactory codecFactory;
  private final ByteBufferAllocator allocator;
  private final int maxAllocationSize;
  private final Map<String, String> properties;
  private final FileDecryptionProperties fileDecryptionProperties;
  private final ParquetConfiguration conf;
  private final ParquetMetricsCallback metricsCallback;

  ParquetReadOptions(
      boolean useSignedStringMinMax,
      boolean useStatsFilter,
      boolean useDictionaryFilter,
      boolean useRecordFilter,
      boolean useColumnIndexFilter,
      boolean usePageChecksumVerification,
      boolean useBloomFilter,
      boolean useOffHeapDecryptBuffer,
      int columnChunkBufferSize,
      FilterCompat.Filter recordFilter,
      ParquetMetadataConverter.MetadataFilter metadataFilter,
      CompressionCodecFactory codecFactory,
      ByteBufferAllocator allocator,
      int maxAllocationSize,
      Map<String, String> properties,
      FileDecryptionProperties fileDecryptionProperties,
      ParquetMetricsCallback metricsCallback) {
    this(
        useSignedStringMinMax,
        useStatsFilter,
        useDictionaryFilter,
        useRecordFilter,
        useColumnIndexFilter,
        usePageChecksumVerification,
        useBloomFilter,
        useOffHeapDecryptBuffer,
        columnChunkBufferSize,
        recordFilter,
        metadataFilter,
        codecFactory,
        allocator,
        maxAllocationSize,
        properties,
        fileDecryptionProperties,
        metricsCallback,
        new HadoopParquetConfiguration());
  }

  ParquetReadOptions(
      boolean useSignedStringMinMax,
      boolean useStatsFilter,
      boolean useDictionaryFilter,
      boolean useRecordFilter,
      boolean useColumnIndexFilter,
      boolean usePageChecksumVerification,
      boolean useBloomFilter,
      boolean useOffHeapDecryptBuffer,
      int columnChunkBufferSize,
      FilterCompat.Filter recordFilter,
      ParquetMetadataConverter.MetadataFilter metadataFilter,
      CompressionCodecFactory codecFactory,
      ByteBufferAllocator allocator,
      int maxAllocationSize,
      Map<String, String> properties,
      FileDecryptionProperties fileDecryptionProperties,
      ParquetMetricsCallback metricsCallback,
      ParquetConfiguration conf) {
    this.useSignedStringMinMax = useSignedStringMinMax;
    this.useStatsFilter = useStatsFilter;
    this.useDictionaryFilter = useDictionaryFilter;
    this.useRecordFilter = useRecordFilter;
    this.useColumnIndexFilter = useColumnIndexFilter;
    this.usePageChecksumVerification = usePageChecksumVerification;
    this.useBloomFilter = useBloomFilter;
    this.useOffHeapDecryptBuffer = useOffHeapDecryptBuffer;
    this.columnChunkBufferSize = columnChunkBufferSize;
    this.recordFilter = recordFilter;
    this.metadataFilter = metadataFilter;
    this.codecFactory = codecFactory;
    this.allocator = allocator;
    this.maxAllocationSize = maxAllocationSize;
    this.properties = Collections.unmodifiableMap(properties);
    this.fileDecryptionProperties = fileDecryptionProperties;
    this.metricsCallback = metricsCallback;
    this.conf = conf;
  }

  public boolean useSignedStringMinMax() {
    return useSignedStringMinMax;
  }

  public boolean useStatsFilter() {
    return useStatsFilter;
  }

  public boolean useDictionaryFilter() {
    return useDictionaryFilter;
  }

  public boolean useRecordFilter() {
    return useRecordFilter;
  }

  public boolean useColumnIndexFilter() {
    return useColumnIndexFilter;
  }

  public boolean useBloomFilter() {
    return useBloomFilter;
  }

  public boolean useOffHeapDecryptBuffer() {
    return useOffHeapDecryptBuffer;
  }

  public boolean usePageChecksumVerification() {
    return usePageChecksumVerification;
  }

  public int columnChunkBufferSize() {
    return columnChunkBufferSize;
  }

  public FilterCompat.Filter getRecordFilter() {
    return recordFilter;
  }

  public ParquetMetadataConverter.MetadataFilter getMetadataFilter() {
    return metadataFilter;
  }

  public CompressionCodecFactory getCodecFactory() {
    return codecFactory;
  }

  public ByteBufferAllocator getAllocator() {
    return allocator;
  }

  public int getMaxAllocationSize() {
    return maxAllocationSize;
  }

  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  public String getProperty(String property) {
    return properties.get(property);
  }

  public FileDecryptionProperties getDecryptionProperties() {
    return fileDecryptionProperties;
  }

  public ParquetMetricsCallback getMetricsCallback() {
    return metricsCallback;
  }

  public boolean isEnabled(String property, boolean defaultValue) {
    Optional<String> propValue = Optional.ofNullable(properties.get(property));
    return propValue.map(Boolean::parseBoolean).orElse(defaultValue);
  }

  public ParquetConfiguration getConfiguration() {
    return conf;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(ParquetConfiguration conf) {
    return new Builder(conf);
  }

  public static class Builder {
    protected boolean useSignedStringMinMax = false;
    protected boolean useStatsFilter = STATS_FILTERING_ENABLED_DEFAULT;
    protected boolean useDictionaryFilter = DICTIONARY_FILTERING_ENABLED_DEFAULT;
    protected boolean useRecordFilter = RECORD_FILTERING_ENABLED_DEFAULT;
    protected boolean useColumnIndexFilter = COLUMN_INDEX_FILTERING_ENABLED_DEFAULT;
    protected boolean usePageChecksumVerification = PAGE_VERIFY_CHECKSUM_ENABLED_DEFAULT;
    protected boolean useBloomFilter = BLOOM_FILTER_ENABLED_DEFAULT;
    protected boolean useOffHeapDecryptBuffer = USE_OFF_HEAP_DECRYPT_BUFFER_DEFAULT;
    protected int columnChunkBufferSize = COLUMN_CHUNK_BUFFER_SIZE_DEFAULT;
    protected FilterCompat.Filter recordFilter = null;
    protected ParquetMetadataConverter.MetadataFilter metadataFilter = NO_FILTER;
    // the page size parameter isn't used when only using the codec factory to get decompressors
    protected CompressionCodecFactory codecFactory = null;
    protected ByteBufferAllocator allocator = new HeapByteBufferAllocator();
    protected int maxAllocationSize = ALLOCATION_SIZE_DEFAULT;
    protected Map<String, String> properties = new HashMap<>();
    protected FileDecryptionProperties fileDecryptionProperties = null;
    protected ParquetConfiguration conf;
    protected ParquetMetricsCallback metricsCallback;

    public Builder() {
      this(new HadoopParquetConfiguration());
    }

    public Builder(ParquetConfiguration conf) {
      this.conf = conf;
      useSignedStringMinMax(conf.getBoolean("parquet.strings.signed-min-max.enabled", false));
      useDictionaryFilter(conf.getBoolean(DICTIONARY_FILTERING_ENABLED, true));
      useStatsFilter(conf.getBoolean(STATS_FILTERING_ENABLED, true));
      useRecordFilter(conf.getBoolean(RECORD_FILTERING_ENABLED, true));
      useColumnIndexFilter(conf.getBoolean(COLUMN_INDEX_FILTERING_ENABLED, true));
      usePageChecksumVerification(conf.getBoolean(PAGE_VERIFY_CHECKSUM_ENABLED, usePageChecksumVerification));
      useBloomFilter(conf.getBoolean(BLOOM_FILTERING_ENABLED, true));
      useOffHeapDecryptBuffer(conf.getBoolean(OFF_HEAP_DECRYPT_BUFFER_ENABLED, false));
      withCodecFactory(HadoopCodecs.newFactory(conf, 0));
      withRecordFilter(getFilter(conf));
      withMaxAllocationInBytes(conf.getInt(ALLOCATION_SIZE, 8388608));
      withColumnChunkBufferSize(conf.getInt(COLUMN_CHUNK_BUFFER_SIZE, -1));
      String badRecordThresh = conf.get(BAD_RECORD_THRESHOLD_CONF_KEY);
      if (badRecordThresh != null) {
        set(BAD_RECORD_THRESHOLD_CONF_KEY, badRecordThresh);
      }
    }

    public Builder useSignedStringMinMax(boolean useSignedStringMinMax) {
      this.useSignedStringMinMax = useSignedStringMinMax;
      return this;
    }

    public Builder useSignedStringMinMax() {
      this.useSignedStringMinMax = true;
      return this;
    }

    public Builder useStatsFilter(boolean useStatsFilter) {
      this.useStatsFilter = useStatsFilter;
      return this;
    }

    public Builder useStatsFilter() {
      this.useStatsFilter = true;
      return this;
    }

    public Builder useDictionaryFilter(boolean useDictionaryFilter) {
      this.useDictionaryFilter = useDictionaryFilter;
      return this;
    }

    public Builder useDictionaryFilter() {
      this.useDictionaryFilter = true;
      return this;
    }

    public Builder useRecordFilter(boolean useRecordFilter) {
      this.useRecordFilter = useRecordFilter;
      return this;
    }

    public Builder useRecordFilter() {
      this.useRecordFilter = true;
      return this;
    }

    public Builder useColumnIndexFilter(boolean useColumnIndexFilter) {
      this.useColumnIndexFilter = useColumnIndexFilter;
      return this;
    }

    public Builder useColumnIndexFilter() {
      return useColumnIndexFilter(true);
    }

    public Builder usePageChecksumVerification(boolean usePageChecksumVerification) {
      this.usePageChecksumVerification = usePageChecksumVerification;
      return this;
    }

    public Builder usePageChecksumVerification() {
      return usePageChecksumVerification(true);
    }

    public Builder useBloomFilter() {
      this.useBloomFilter = true;
      return this;
    }

    public Builder useOffHeapDecryptBuffer() {
      return useOffHeapDecryptBuffer(true);
    }

    public Builder useOffHeapDecryptBuffer(boolean useOffHeapDecryptBuffer) {
      this.useOffHeapDecryptBuffer = useOffHeapDecryptBuffer;
      return this;
    }

    public Builder withColumnChunkBufferSize(int columnChunkBufferSize) {
      this.columnChunkBufferSize = columnChunkBufferSize;
      return this;
    }

    public Builder useBloomFilter(boolean useBloomFilter) {
      this.useBloomFilter = useBloomFilter;
      return this;
    }

    public Builder withRecordFilter(FilterCompat.Filter rowGroupFilter) {
      this.recordFilter = rowGroupFilter;
      return this;
    }

    public Builder withRange(long start, long end) {
      this.metadataFilter = ParquetMetadataConverter.range(start, end);
      return this;
    }

    public Builder withOffsets(long... rowGroupOffsets) {
      this.metadataFilter = ParquetMetadataConverter.offsets(rowGroupOffsets);
      return this;
    }

    public Builder withMetadataFilter(ParquetMetadataConverter.MetadataFilter metadataFilter) {
      this.metadataFilter = metadataFilter;
      return this;
    }

    public Builder withCodecFactory(CompressionCodecFactory codecFactory) {
      this.codecFactory = codecFactory;
      return this;
    }

    public Builder withAllocator(ByteBufferAllocator allocator) {
      this.allocator = allocator;
      return this;
    }

    public Builder withMaxAllocationInBytes(int allocationSizeInBytes) {
      this.maxAllocationSize = allocationSizeInBytes;
      return this;
    }

    public Builder withPageChecksumVerification(boolean val) {
      this.usePageChecksumVerification = val;
      return this;
    }

    public Builder withDecryption(FileDecryptionProperties fileDecryptionProperties) {
      this.fileDecryptionProperties = fileDecryptionProperties;
      return this;
    }

    public Builder withMetricsCallback(ParquetMetricsCallback metricsCallback) {
      this.metricsCallback = metricsCallback;
      return this;
    }

    public Builder set(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public Builder copy(ParquetReadOptions options) {
      useSignedStringMinMax(options.useSignedStringMinMax);
      useStatsFilter(options.useStatsFilter);
      useDictionaryFilter(options.useDictionaryFilter);
      useRecordFilter(options.useRecordFilter);
      withRecordFilter(options.recordFilter);
      withMetadataFilter(options.metadataFilter);
      withCodecFactory(options.codecFactory);
      withAllocator(options.allocator);
      withPageChecksumVerification(options.usePageChecksumVerification);
      withDecryption(options.fileDecryptionProperties);
      withMetricsCallback(options.metricsCallback);
      withColumnChunkBufferSize(options.columnChunkBufferSize);
      conf = options.conf;
      for (Map.Entry<String, String> keyValue : options.properties.entrySet()) {
        set(keyValue.getKey(), keyValue.getValue());
      }
      return this;
    }

    public ParquetReadOptions build() {
      if (codecFactory == null) {
        if (conf == null) {
          codecFactory = HadoopCodecs.newFactory(0);
        } else {
          codecFactory = HadoopCodecs.newFactory(conf, 0);
        }
      }

      return new ParquetReadOptions(
          useSignedStringMinMax,
          useStatsFilter,
          useDictionaryFilter,
          useRecordFilter,
          useColumnIndexFilter,
          usePageChecksumVerification,
          useBloomFilter,
          useOffHeapDecryptBuffer,
          columnChunkBufferSize,
          recordFilter,
          metadataFilter,
          codecFactory,
          allocator,
          maxAllocationSize,
          properties,
          fileDecryptionProperties,
          metricsCallback,
          conf);
    }
  }
}
