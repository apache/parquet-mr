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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.ParquetMetricsCallback;

public class HadoopReadOptions extends ParquetReadOptions {
  private final Configuration conf;

  private HadoopReadOptions(
      boolean useSignedStringMinMax,
      boolean useStatsFilter,
      boolean useDictionaryFilter,
      boolean useRecordFilter,
      boolean useColumnIndexFilter,
      boolean usePageChecksumVerification,
      boolean useBloomFilter,
      boolean useOffHeapDecryptBuffer,
      int columnChunkReadSize,
      FilterCompat.Filter recordFilter,
      MetadataFilter metadataFilter,
      CompressionCodecFactory codecFactory,
      ByteBufferAllocator allocator,
      int maxAllocationSize,
      Map<String, String> properties,
      Configuration conf,
      FileDecryptionProperties fileDecryptionProperties,
      ParquetMetricsCallback metricsCallback) {
    super(
        useSignedStringMinMax,
        useStatsFilter,
        useDictionaryFilter,
        useRecordFilter,
        useColumnIndexFilter,
        usePageChecksumVerification,
        useBloomFilter,
        useOffHeapDecryptBuffer,
        columnChunkReadSize,
        recordFilter,
        metadataFilter,
        codecFactory,
        allocator,
        maxAllocationSize,
        properties,
        fileDecryptionProperties,
        metricsCallback,
        new HadoopParquetConfiguration(conf));
    this.conf = conf;
  }

  @Override
  public String getProperty(String property) {
    String value = super.getProperty(property);
    if (value != null) {
      return value;
    }
    return conf.get(property);
  }

  public Configuration getConf() {
    return conf;
  }

  public static Builder builder(Configuration conf) {
    return new Builder(conf);
  }

  public static Builder builder(Configuration conf, Path filePath) {
    return new Builder(conf, filePath);
  }

  public static class Builder extends ParquetReadOptions.Builder {
    private final Configuration conf;
    private final Path filePath;

    public Builder(Configuration conf) {
      this(conf, null);
    }

    public Builder(Configuration conf, Path filePath) {
      super(new HadoopParquetConfiguration(conf));
      this.conf = conf;
      this.filePath = filePath;
    }

    @Override
    public ParquetReadOptions build() {
      if (null == fileDecryptionProperties) {
        // if not set, check if Hadoop conf defines decryption factory and properties
        fileDecryptionProperties = createDecryptionProperties(filePath, conf);
      }
      return new HadoopReadOptions(
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
          conf,
          fileDecryptionProperties,
          metricsCallback);
    }
  }

  private static FileDecryptionProperties createDecryptionProperties(Path file, Configuration hadoopConfig) {
    DecryptionPropertiesFactory cryptoFactory = DecryptionPropertiesFactory.loadFactory(hadoopConfig);
    if (null == cryptoFactory) {
      return null;
    }
    return cryptoFactory.getFileDecryptionProperties(hadoopConfig, file);
  }
}
