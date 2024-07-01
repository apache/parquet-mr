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
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {

  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = ParquetProperties.DEFAULT_PAGE_SIZE;
  public static final CompressionCodecName DEFAULT_COMPRESSION_CODEC_NAME = CompressionCodecName.UNCOMPRESSED;
  public static final boolean DEFAULT_IS_DICTIONARY_ENABLED = ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED;
  public static final boolean DEFAULT_IS_VALIDATING_ENABLED = false;
  public static final WriterVersion DEFAULT_WRITER_VERSION = ParquetProperties.DEFAULT_WRITER_VERSION;

  public static final String OBJECT_MODEL_NAME_PROP = "writer.model.name";

  // max size (bytes) to write as padding and the min size of a row group
  public static final int MAX_PADDING_SIZE_DEFAULT = 8 * 1024 * 1024; // 8MB

  private final InternalParquetRecordWriter<T> writer;
  private final CompressionCodecFactory codecFactory;

  /**
   * Create a new ParquetWriter.
   * (with dictionary encoding enabled and validation off)
   *
   * @param file                 the file to create
   * @param writeSupport         the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize            the block size threshold
   * @param pageSize             the page size threshold
   * @throws IOException if there is an error while writing
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize)
      throws IOException {
    this(
        file,
        writeSupport,
        compressionCodecName,
        blockSize,
        pageSize,
        DEFAULT_IS_DICTIONARY_ENABLED,
        DEFAULT_IS_VALIDATING_ENABLED);
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file                 the file to create
   * @param writeSupport         the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize            the block size threshold
   * @param pageSize             the page size threshold (both data and dictionary)
   * @param enableDictionary     to turn dictionary encoding on
   * @param validating           to turn on validation using the schema
   * @throws IOException if there is an error while writing
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      boolean enableDictionary,
      boolean validating)
      throws IOException {
    this(file, writeSupport, compressionCodecName, blockSize, pageSize, pageSize, enableDictionary, validating);
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file                 the file to create
   * @param writeSupport         the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize            the block size threshold
   * @param pageSize             the page size threshold
   * @param dictionaryPageSize   the page size threshold for the dictionary pages
   * @param enableDictionary     to turn dictionary encoding on
   * @param validating           to turn on validation using the schema
   * @throws IOException if there is an error while writing
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating)
      throws IOException {
    this(
        file,
        writeSupport,
        compressionCodecName,
        blockSize,
        pageSize,
        dictionaryPageSize,
        enableDictionary,
        validating,
        DEFAULT_WRITER_VERSION);
  }

  /**
   * Create a new ParquetWriter.
   * <p>
   * Directly instantiates a Hadoop {@link org.apache.hadoop.conf.Configuration} which reads
   * configuration from the classpath.
   *
   * @param file                 the file to create
   * @param writeSupport         the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize            the block size threshold
   * @param pageSize             the page size threshold
   * @param dictionaryPageSize   the page size threshold for the dictionary pages
   * @param enableDictionary     to turn dictionary encoding on
   * @param validating           to turn on validation using the schema
   * @param writerVersion        version of parquetWriter from {@link ParquetProperties.WriterVersion}
   * @throws IOException if there is an error while writing
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion)
      throws IOException {
    this(
        file,
        writeSupport,
        compressionCodecName,
        blockSize,
        pageSize,
        dictionaryPageSize,
        enableDictionary,
        validating,
        writerVersion,
        new Configuration());
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file                 the file to create
   * @param writeSupport         the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize            the block size threshold
   * @param pageSize             the page size threshold
   * @param dictionaryPageSize   the page size threshold for the dictionary pages
   * @param enableDictionary     to turn dictionary encoding on
   * @param validating           to turn on validation using the schema
   * @param writerVersion        version of parquetWriter from {@link ParquetProperties.WriterVersion}
   * @param conf                 Hadoop configuration to use while accessing the filesystem
   * @throws IOException if there is an error while writing
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      Configuration conf)
      throws IOException {
    this(
        file,
        ParquetFileWriter.Mode.CREATE,
        writeSupport,
        compressionCodecName,
        blockSize,
        pageSize,
        dictionaryPageSize,
        enableDictionary,
        validating,
        writerVersion,
        conf);
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file                 the file to create
   * @param mode                 file creation mode
   * @param writeSupport         the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize            the block size threshold
   * @param pageSize             the page size threshold
   * @param dictionaryPageSize   the page size threshold for the dictionary pages
   * @param enableDictionary     to turn dictionary encoding on
   * @param validating           to turn on validation using the schema
   * @param writerVersion        version of parquetWriter from {@link ParquetProperties.WriterVersion}
   * @param conf                 Hadoop configuration to use while accessing the filesystem
   * @throws IOException if there is an error while writing
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public ParquetWriter(
      Path file,
      ParquetFileWriter.Mode mode,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion,
      Configuration conf)
      throws IOException {
    this(
        HadoopOutputFile.fromPath(file, conf),
        mode,
        writeSupport,
        compressionCodecName,
        blockSize,
        validating,
        conf,
        MAX_PADDING_SIZE_DEFAULT,
        ParquetProperties.builder()
            .withPageSize(pageSize)
            .withDictionaryPageSize(dictionaryPageSize)
            .withDictionaryEncoding(enableDictionary)
            .withWriterVersion(writerVersion)
            .build(),
        null);
  }

  /**
   * Create a new ParquetWriter. The default block size is 128 MB. The default
   * page size is 1 MB. Default compression is no compression. Dictionary encoding is disabled.
   *
   * @param file         the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @throws IOException if there is an error while writing
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public ParquetWriter(Path file, WriteSupport<T> writeSupport) throws IOException {
    this(file, writeSupport, DEFAULT_COMPRESSION_CODEC_NAME, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
  }

  @Deprecated
  public ParquetWriter(Path file, Configuration conf, WriteSupport<T> writeSupport) throws IOException {
    this(
        file,
        writeSupport,
        DEFAULT_COMPRESSION_CODEC_NAME,
        DEFAULT_BLOCK_SIZE,
        DEFAULT_PAGE_SIZE,
        DEFAULT_PAGE_SIZE,
        DEFAULT_IS_DICTIONARY_ENABLED,
        DEFAULT_IS_VALIDATING_ENABLED,
        DEFAULT_WRITER_VERSION,
        conf);
  }

  ParquetWriter(
      OutputFile file,
      ParquetFileWriter.Mode mode,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      long rowGroupSize,
      boolean validating,
      Configuration conf,
      int maxPaddingSize,
      ParquetProperties encodingProps,
      FileEncryptionProperties encryptionProperties)
      throws IOException {
    this(
        file,
        mode,
        writeSupport,
        compressionCodecName,
        rowGroupSize,
        validating,
        new HadoopParquetConfiguration(conf),
        maxPaddingSize,
        encodingProps,
        encryptionProperties);
  }

  ParquetWriter(
      OutputFile file,
      ParquetFileWriter.Mode mode,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      long rowGroupSize,
      boolean validating,
      ParquetConfiguration conf,
      int maxPaddingSize,
      ParquetProperties encodingProps,
      FileEncryptionProperties encryptionProperties)
      throws IOException {
    this(
        file,
        mode,
        writeSupport,
        compressionCodecName,
        new CodecFactory(conf, encodingProps.getPageSizeThreshold()),
        rowGroupSize,
        validating,
        conf,
        maxPaddingSize,
        encodingProps,
        encryptionProperties);
  }

  ParquetWriter(
      OutputFile file,
      ParquetFileWriter.Mode mode,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      CompressionCodecFactory codecFactory,
      long rowGroupSize,
      boolean validating,
      ParquetConfiguration conf,
      int maxPaddingSize,
      ParquetProperties encodingProps,
      FileEncryptionProperties encryptionProperties)
      throws IOException {
    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    MessageType schema = writeContext.getSchema();

    // encryptionProperties could be built from the implementation of EncryptionPropertiesFactory when it is
    // attached.
    if (encryptionProperties == null) {
      String path = file == null ? null : file.getPath();
      encryptionProperties = EncryptionPropertiesHelper.createEncryptionProperties(
          conf, path == null ? null : Paths.get(path), writeContext);
    }

    ParquetFileWriter fileWriter = new ParquetFileWriter(
        file, schema, mode, rowGroupSize, maxPaddingSize, encryptionProperties, encodingProps);
    fileWriter.start();

    this.codecFactory = codecFactory;
    CompressionCodecFactory.BytesInputCompressor compressor = codecFactory.getCompressor(compressionCodecName);

    final Map<String, String> extraMetadata;
    if (encodingProps.getExtraMetaData() == null
        || encodingProps.getExtraMetaData().isEmpty()) {
      extraMetadata = writeContext.getExtraMetaData();
    } else {
      extraMetadata = new HashMap<>(writeContext.getExtraMetaData());

      encodingProps.getExtraMetaData().forEach((metadataKey, metadataValue) -> {
        if (metadataKey.equals(OBJECT_MODEL_NAME_PROP)) {
          throw new IllegalArgumentException("Cannot overwrite metadata key " + OBJECT_MODEL_NAME_PROP
              + ". Please use another key name.");
        }

        if (extraMetadata.put(metadataKey, metadataValue) != null) {
          throw new IllegalArgumentException(
              "Duplicate metadata key " + metadataKey + ". Please use another key name.");
        }
      });
    }

    this.writer = new InternalParquetRecordWriter<T>(
        fileWriter, writeSupport, schema, extraMetadata, rowGroupSize, compressor, validating, encodingProps);
  }

  public void write(T object) throws IOException {
    try {
      writer.write(object);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      writer.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      // release after the writer closes in case it is used for a last flush
      codecFactory.release();
    }
  }

  /**
   * @return the ParquetMetadata written to the (closed) file.
   */
  public ParquetMetadata getFooter() {
    return writer.getFooter();
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return writer.getDataSize();
  }

  /**
   * An abstract builder class for ParquetWriter instances.
   * <p>
   * Object models should extend this builder to provide writer configuration
   * options.
   *
   * @param <T>    The type of objects written by the constructed ParquetWriter.
   * @param <SELF> The type of this builder that is returned by builder methods
   */
  public abstract static class Builder<T, SELF extends Builder<T, SELF>> {
    private OutputFile file = null;
    private Path path = null;
    private FileEncryptionProperties encryptionProperties = null;
    private ParquetConfiguration conf = null;
    private ParquetFileWriter.Mode mode;
    private CompressionCodecFactory codecFactory = null;
    private CompressionCodecName codecName = DEFAULT_COMPRESSION_CODEC_NAME;
    private long rowGroupSize = DEFAULT_BLOCK_SIZE;
    private int maxPaddingSize = MAX_PADDING_SIZE_DEFAULT;
    private boolean enableValidation = DEFAULT_IS_VALIDATING_ENABLED;
    private ParquetProperties.Builder encodingPropsBuilder = ParquetProperties.builder();

    protected Builder(Path path) {
      this.path = path;
    }

    protected Builder(OutputFile path) {
      this.file = path;
    }

    /**
     * @return this as the correct subclass of ParquetWriter.Builder.
     */
    protected abstract SELF self();

    /**
     * @param conf a configuration
     * @return an appropriate WriteSupport for the object model.
     * @deprecated Use {@link #getWriteSupport(ParquetConfiguration)} instead
     */
    @Deprecated
    protected abstract WriteSupport<T> getWriteSupport(Configuration conf);

    /**
     * @param conf a configuration
     * @return an appropriate WriteSupport for the object model.
     */
    protected WriteSupport<T> getWriteSupport(ParquetConfiguration conf) {
      return getWriteSupport(ConfigurationUtil.createHadoopConfiguration(conf));
    }

    /**
     * Set the {@link Configuration} used by the constructed writer.
     *
     * @param conf a {@code Configuration}
     * @return this builder for method chaining.
     */
    public SELF withConf(Configuration conf) {
      this.conf = new HadoopParquetConfiguration(conf);
      return self();
    }

    /**
     * Set the {@link ParquetConfiguration} used by the constructed writer.
     *
     * @param conf a {@code ParquetConfiguration}
     * @return this builder for method chaining.
     */
    public SELF withConf(ParquetConfiguration conf) {
      this.conf = conf;
      return self();
    }

    /**
     * Set the {@link ParquetFileWriter.Mode write mode} used when creating the
     * backing file for this writer.
     *
     * @param mode a {@code ParquetFileWriter.Mode}
     * @return this builder for method chaining.
     */
    public SELF withWriteMode(ParquetFileWriter.Mode mode) {
      this.mode = mode;
      return self();
    }

    /**
     * Set the {@link CompressionCodecName compression codec} used by the
     * constructed writer.
     *
     * @param codecName a {@code CompressionCodecName}
     * @return this builder for method chaining.
     */
    public SELF withCompressionCodec(CompressionCodecName codecName) {
      this.codecName = codecName;
      return self();
    }

    /**
     * Set the {@link CompressionCodecFactory codec factory} used by the
     * constructed writer.
     *
     * @param codecFactory a {@link CompressionCodecFactory}
     * @return this builder for method chaining.
     */
    public SELF withCodecFactory(CompressionCodecFactory codecFactory) {
      this.codecFactory = codecFactory;
      return self();
    }

    /**
     * Set the {@link FileEncryptionProperties file encryption properties} used by the
     * constructed writer.
     *
     * @param encryptionProperties a {@code FileEncryptionProperties}
     * @return this builder for method chaining.
     */
    public SELF withEncryption(FileEncryptionProperties encryptionProperties) {
      this.encryptionProperties = encryptionProperties;
      return self();
    }

    /**
     * Set the Parquet format row group size used by the constructed writer.
     *
     * @param rowGroupSize an integer size in bytes
     * @return this builder for method chaining.
     * @deprecated Use {@link #withRowGroupSize(long)} instead
     */
    @Deprecated
    public SELF withRowGroupSize(int rowGroupSize) {
      return withRowGroupSize((long) rowGroupSize);
    }

    /**
     * Set the Parquet format row group size used by the constructed writer.
     *
     * @param rowGroupSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withRowGroupSize(long rowGroupSize) {
      this.rowGroupSize = rowGroupSize;
      return self();
    }

    /**
     * Set the Parquet format page size used by the constructed writer.
     *
     * @param pageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withPageSize(int pageSize) {
      encodingPropsBuilder.withPageSize(pageSize);
      return self();
    }

    /**
     * Sets the Parquet format page row count limit used by the constructed writer.
     *
     * @param rowCount limit for the number of rows stored in a page
     * @return this builder for method chaining
     */
    public SELF withPageRowCountLimit(int rowCount) {
      encodingPropsBuilder.withPageRowCountLimit(rowCount);
      return self();
    }

    /**
     * Set the Parquet format dictionary page size used by the constructed
     * writer.
     *
     * @param dictionaryPageSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withDictionaryPageSize(int dictionaryPageSize) {
      encodingPropsBuilder.withDictionaryPageSize(dictionaryPageSize);
      return self();
    }

    /**
     * Set the maximum amount of padding, in bytes, that will be used to align
     * row groups with blocks in the underlying filesystem. If the underlying
     * filesystem is not a block filesystem like HDFS, this has no effect.
     *
     * @param maxPaddingSize an integer size in bytes
     * @return this builder for method chaining.
     */
    public SELF withMaxPaddingSize(int maxPaddingSize) {
      this.maxPaddingSize = maxPaddingSize;
      return self();
    }

    /**
     * Enables dictionary encoding for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enableDictionaryEncoding() {
      encodingPropsBuilder.withDictionaryEncoding(true);
      return self();
    }

    /**
     * Enable or disable dictionary encoding for the constructed writer.
     *
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public SELF withDictionaryEncoding(boolean enableDictionary) {
      encodingPropsBuilder.withDictionaryEncoding(enableDictionary);
      return self();
    }

    public SELF withByteStreamSplitEncoding(boolean enableByteStreamSplit) {
      encodingPropsBuilder.withByteStreamSplitEncoding(enableByteStreamSplit);
      return self();
    }

    /**
     * Enable or disable dictionary encoding of the specified column for the constructed writer.
     *
     * @param columnPath       the path of the column (dot-string)
     * @param enableDictionary whether dictionary encoding should be enabled
     * @return this builder for method chaining.
     */
    public SELF withDictionaryEncoding(String columnPath, boolean enableDictionary) {
      encodingPropsBuilder.withDictionaryEncoding(columnPath, enableDictionary);
      return self();
    }

    /**
     * Enables validation for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enableValidation() {
      this.enableValidation = true;
      return self();
    }

    /**
     * Enable or disable validation for the constructed writer.
     *
     * @param enableValidation whether validation should be enabled
     * @return this builder for method chaining.
     */
    public SELF withValidation(boolean enableValidation) {
      this.enableValidation = enableValidation;
      return self();
    }

    /**
     * Set the {@link WriterVersion format version} used by the constructed
     * writer.
     *
     * @param version a {@code WriterVersion}
     * @return this builder for method chaining.
     */
    public SELF withWriterVersion(WriterVersion version) {
      encodingPropsBuilder.withWriterVersion(version);
      return self();
    }

    /**
     * Enables writing page level checksums for the constructed writer.
     *
     * @return this builder for method chaining.
     */
    public SELF enablePageWriteChecksum() {
      encodingPropsBuilder.withPageWriteChecksumEnabled(true);
      return self();
    }

    /**
     * Enables writing page level checksums for the constructed writer.
     *
     * @param enablePageWriteChecksum whether page checksums should be written out
     * @return this builder for method chaining.
     */
    public SELF withPageWriteChecksumEnabled(boolean enablePageWriteChecksum) {
      encodingPropsBuilder.withPageWriteChecksumEnabled(enablePageWriteChecksum);
      return self();
    }

    /**
     * Set max Bloom filter bytes for related columns.
     *
     * @param maxBloomFilterBytes the max bytes of a Bloom filter bitset for a column.
     * @return this builder for method chaining
     */
    public SELF withMaxBloomFilterBytes(int maxBloomFilterBytes) {
      encodingPropsBuilder.withMaxBloomFilterBytes(maxBloomFilterBytes);
      return self();
    }

    /**
     * Sets the NDV (number of distinct values) for the specified column.
     *
     * @param columnPath the path of the column (dot-string)
     * @param ndv        the NDV of the column
     * @return this builder for method chaining.
     */
    public SELF withBloomFilterNDV(String columnPath, long ndv) {
      encodingPropsBuilder.withBloomFilterNDV(columnPath, ndv);

      return self();
    }

    public SELF withBloomFilterFPP(String columnPath, double fpp) {
      encodingPropsBuilder.withBloomFilterFPP(columnPath, fpp);
      return self();
    }

    /**
     * When NDV (number of distinct values) for a specified column is not set, whether to use
     * `AdaptiveBloomFilter` to automatically adjust the BloomFilter size according to `parquet.bloom.filter.max.bytes`
     *
     * @param enabled whether to write bloom filter for the column
     */
    public SELF withAdaptiveBloomFilterEnabled(boolean enabled) {
      encodingPropsBuilder.withAdaptiveBloomFilterEnabled(enabled);
      return self();
    }

    /**
     * When `AdaptiveBloomFilter` is enabled, set how many bloom filter candidates to use.
     *
     * @param columnPath the path of the column (dot-string)
     * @param number     the number of candidate
     */
    public SELF withBloomFilterCandidateNumber(String columnPath, int number) {
      encodingPropsBuilder.withBloomFilterCandidatesNumber(columnPath, number);
      return self();
    }

    /**
     * Sets the bloom filter enabled/disabled
     *
     * @param enabled whether to write bloom filters
     * @return this builder for method chaining
     */
    public SELF withBloomFilterEnabled(boolean enabled) {
      encodingPropsBuilder.withBloomFilterEnabled(enabled);
      return self();
    }

    /**
     * Sets the bloom filter enabled/disabled for the specified column. If not set for the column specifically the
     * default enabled/disabled state will take place. See {@link #withBloomFilterEnabled(boolean)}.
     *
     * @param columnPath the path of the column (dot-string)
     * @param enabled    whether to write bloom filter for the column
     * @return this builder for method chaining
     */
    public SELF withBloomFilterEnabled(String columnPath, boolean enabled) {
      encodingPropsBuilder.withBloomFilterEnabled(columnPath, enabled);
      return self();
    }

    /**
     * Sets the minimum number of rows to write before a page size check is done.
     *
     * @param min writes at least `min` rows before invoking a page size check
     * @return this builder for method chaining
     */
    public SELF withMinRowCountForPageSizeCheck(int min) {
      encodingPropsBuilder.withMinRowCountForPageSizeCheck(min);
      return self();
    }

    /**
     * Sets the maximum number of rows to write before a page size check is done.
     *
     * @param max makes a page size check after `max` rows have been written
     * @return this builder for method chaining
     */
    public SELF withMaxRowCountForPageSizeCheck(int max) {
      encodingPropsBuilder.withMaxRowCountForPageSizeCheck(max);
      return self();
    }

    /**
     * Sets the length to be used for truncating binary values in a binary column index.
     *
     * @param length the length to truncate to
     * @return this builder for method chaining
     */
    public SELF withColumnIndexTruncateLength(int length) {
      encodingPropsBuilder.withColumnIndexTruncateLength(length);
      return self();
    }

    /**
     * Sets the length which the min/max binary values in row groups are truncated to.
     *
     * @param length the length to truncate to
     * @return this builder for method chaining
     */
    public SELF withStatisticsTruncateLength(int length) {
      encodingPropsBuilder.withStatisticsTruncateLength(length);
      return self();
    }

    /**
     * Sets additional metadata entries to be included in the file footer.
     *
     * @param extraMetaData a Map of additional stringly-typed metadata entries
     * @return this builder for method chaining
     */
    public SELF withExtraMetaData(Map<String, String> extraMetaData) {
      encodingPropsBuilder.withExtraMetaData(extraMetaData);
      return self();
    }

    /**
     * Sets the ByteBuffer allocator instance to be used for allocating memory for writing.
     *
     * @param allocator the allocator instance
     * @return this builder for method chaining
     */
    public SELF withAllocator(ByteBufferAllocator allocator) {
      encodingPropsBuilder.withAllocator(allocator);
      return self();
    }

    /**
     * Set a property that will be available to the read path. For writers that use a Hadoop
     * configuration, this is the recommended way to add configuration values.
     *
     * @param property a String property name
     * @param value    a String property value
     * @return this builder for method chaining.
     */
    public SELF config(String property, String value) {
      if (conf == null) {
        conf = new HadoopParquetConfiguration();
      }
      conf.set(property, value);
      return self();
    }

    /**
     * Build a {@link ParquetWriter} with the accumulated configuration.
     *
     * @return a configured {@code ParquetWriter} instance.
     * @throws IOException if there is an error while creating the writer
     */
    public ParquetWriter<T> build() throws IOException {
      if (conf == null) {
        conf = new HadoopParquetConfiguration();
      }
      ParquetProperties encodingProps = encodingPropsBuilder.build();
      if (codecFactory == null) {
        codecFactory = new CodecFactory(conf, encodingProps.getPageSizeThreshold());
      }

      return new ParquetWriter<>(
          (file != null)
              ? file
              : HadoopOutputFile.fromPath(path, ConfigurationUtil.createHadoopConfiguration(conf)),
          mode,
          getWriteSupport(conf),
          codecName,
          codecFactory,
          rowGroupSize,
          enableValidation,
          conf,
          maxPaddingSize,
          encodingProps,
          encryptionProperties);
    }
  }
}
