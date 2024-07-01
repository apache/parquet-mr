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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.InputFile;

/**
 * Read records from a Parquet file.
 * TODO: too many constructors (https://issues.apache.org/jira/browse/PARQUET-39)
 */
public class ParquetReader<T> implements Closeable {

  private final ReadSupport<T> readSupport;
  private final Iterator<InputFile> filesIterator;
  private final ParquetReadOptions options;

  private InternalParquetRecordReader<T> reader;

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Path file, ReadSupport<T> readSupport) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport) throws IOException {
    this(conf, file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter)
      throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException if there is an error while reading
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public ParquetReader(
      Configuration conf, Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter)
      throws IOException {
    this(conf, file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  private ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport, FilterCompat.Filter filter)
      throws IOException {
    this(
        Collections.singletonList((InputFile) HadoopInputFile.fromPath(file, conf)),
        HadoopReadOptions.builder(conf, file)
            .withRecordFilter(Objects.requireNonNull(filter, "filter cannot be null"))
            .build(),
        readSupport);
  }

  private ParquetReader(List<InputFile> files, ParquetReadOptions options, ReadSupport<T> readSupport)
      throws IOException {
    this.readSupport = readSupport;
    this.options = options;
    this.filesIterator = files.iterator();
  }

  /**
   * @return the next record or null if finished
   * @throws IOException if there is an error while reading
   */
  public T read() throws IOException {
    try {
      if (reader != null && reader.nextKeyValue()) {
        return reader.getCurrentValue();
      } else {
        initReader();
        return reader == null ? null : read();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return the row index of the last read row. If no row has been processed, returns -1.
   */
  public long getCurrentRowIndex() {
    if (reader == null) {
      return -1;
    }
    return reader.getCurrentRowIndex();
  }

  private void initReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }

    if (filesIterator.hasNext()) {
      InputFile file = filesIterator.next();

      ParquetFileReader fileReader = ParquetFileReader.open(file, options);

      reader = new InternalParquetRecordReader<>(readSupport, options.getRecordFilter());

      reader.initialize(fileReader, options);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  public static <T> Builder<T> read(InputFile file) throws IOException {
    return new Builder<>(file);
  }

  public static <T> Builder<T> read(InputFile file, ParquetConfiguration conf) throws IOException {
    return new Builder<>(file, conf);
  }

  public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path) {
    return new Builder<>(readSupport, path);
  }

  public static class Builder<T> {
    private final ReadSupport<T> readSupport;
    private final InputFile file;
    private final Path path;
    private Filter filter = null;
    private ByteBufferAllocator allocator = new HeapByteBufferAllocator();
    protected ParquetConfiguration configuration;
    private ParquetReadOptions.Builder optionsBuilder;

    // May be null if parquetConfiguration is not an instance of org.apache.parquet.conf.HadoopParquetConfiguration
    @Deprecated
    protected Configuration conf;

    @Deprecated
    private Builder(ReadSupport<T> readSupport, Path path) {
      this.readSupport = Objects.requireNonNull(readSupport, "readSupport cannot be null");
      this.file = null;
      this.path = Objects.requireNonNull(path, "path cannot be null");
      this.conf = new Configuration();
      this.configuration = new HadoopParquetConfiguration(this.conf);
      this.optionsBuilder = HadoopReadOptions.builder(this.conf, path);
    }

    @Deprecated
    protected Builder(Path path) {
      this.readSupport = null;
      this.file = null;
      this.path = Objects.requireNonNull(path, "path cannot be null");
      this.conf = new Configuration();
      this.configuration = new HadoopParquetConfiguration(this.conf);
      this.optionsBuilder = HadoopReadOptions.builder(this.conf, path);
    }

    protected Builder(InputFile file) {
      this.readSupport = null;
      this.file = Objects.requireNonNull(file, "file cannot be null");
      this.path = null;
      if (file instanceof HadoopInputFile) {
        HadoopInputFile hadoopFile = (HadoopInputFile) file;
        this.conf = hadoopFile.getConfiguration();
        this.configuration = new HadoopParquetConfiguration(this.conf);
        optionsBuilder = HadoopReadOptions.builder(this.conf, hadoopFile.getPath());
      } else {
        optionsBuilder = ParquetReadOptions.builder(new HadoopParquetConfiguration());
      }
    }

    protected Builder(InputFile file, ParquetConfiguration conf) {
      this.readSupport = null;
      this.file = Objects.requireNonNull(file, "file cannot be null");
      this.path = null;
      this.configuration = conf;
      if (file instanceof HadoopInputFile) {
        this.conf = ConfigurationUtil.createHadoopConfiguration(conf);
        HadoopInputFile hadoopFile = (HadoopInputFile) file;
        optionsBuilder = HadoopReadOptions.builder(this.conf, hadoopFile.getPath());
      } else {
        optionsBuilder = ParquetReadOptions.builder(conf);
      }
    }

    // when called, resets options to the defaults from conf
    public Builder<T> withConf(Configuration conf) {
      this.conf = Objects.requireNonNull(conf, "conf cannot be null");
      this.configuration = new HadoopParquetConfiguration(this.conf);

      // previous versions didn't use the builder, so may set filter before conf. this maintains
      // compatibility for filter. other options are reset by a new conf.
      this.optionsBuilder = HadoopReadOptions.builder(conf, path);
      if (filter != null) {
        optionsBuilder.withRecordFilter(filter);
      }

      return this;
    }

    public Builder<T> withConf(ParquetConfiguration conf) {
      this.configuration = conf;
      this.optionsBuilder = ParquetReadOptions.builder(conf);
      if (filter != null) {
        optionsBuilder.withRecordFilter(filter);
      }
      return this;
    }

    public Builder<T> withFilter(Filter filter) {
      this.filter = filter;
      optionsBuilder.withRecordFilter(filter);
      return this;
    }

    public Builder<T> withAllocator(ByteBufferAllocator allocator) {
      this.allocator = allocator;
      optionsBuilder.withAllocator(allocator);
      return this;
    }

    public Builder<T> useSignedStringMinMax(boolean useSignedStringMinMax) {
      optionsBuilder.useSignedStringMinMax(useSignedStringMinMax);
      return this;
    }

    public Builder<T> useSignedStringMinMax() {
      optionsBuilder.useSignedStringMinMax();
      return this;
    }

    public Builder<T> useStatsFilter(boolean useStatsFilter) {
      optionsBuilder.useStatsFilter(useStatsFilter);
      return this;
    }

    public Builder<T> useStatsFilter() {
      optionsBuilder.useStatsFilter();
      return this;
    }

    public Builder<T> useDictionaryFilter(boolean useDictionaryFilter) {
      optionsBuilder.useDictionaryFilter(useDictionaryFilter);
      return this;
    }

    public Builder<T> useDictionaryFilter() {
      optionsBuilder.useDictionaryFilter();
      return this;
    }

    public Builder<T> useRecordFilter(boolean useRecordFilter) {
      optionsBuilder.useRecordFilter(useRecordFilter);
      return this;
    }

    public Builder<T> useRecordFilter() {
      optionsBuilder.useRecordFilter();
      return this;
    }

    public Builder<T> useColumnIndexFilter(boolean useColumnIndexFilter) {
      optionsBuilder.useColumnIndexFilter(useColumnIndexFilter);
      return this;
    }

    public Builder<T> useColumnIndexFilter() {
      optionsBuilder.useColumnIndexFilter();
      return this;
    }

    public Builder<T> usePageChecksumVerification(boolean usePageChecksumVerification) {
      optionsBuilder.usePageChecksumVerification(usePageChecksumVerification);
      return this;
    }

    public Builder<T> useBloomFilter(boolean useBloomFilter) {
      optionsBuilder.useBloomFilter(useBloomFilter);
      return this;
    }

    public Builder<T> useBloomFilter() {
      optionsBuilder.useBloomFilter();
      return this;
    }

    public Builder<T> usePageChecksumVerification() {
      optionsBuilder.usePageChecksumVerification();
      return this;
    }

    public Builder<T> withFileRange(long start, long end) {
      optionsBuilder.withRange(start, end);
      return this;
    }

    public Builder<T> withCodecFactory(CompressionCodecFactory codecFactory) {
      optionsBuilder.withCodecFactory(codecFactory);
      return this;
    }

    public Builder<T> withDecryption(FileDecryptionProperties fileDecryptionProperties) {
      optionsBuilder.withDecryption(fileDecryptionProperties);
      return this;
    }

    public Builder<T> withIOThreadPool(ExecutorService threadPool) {
      optionsBuilder.withIOThreadPool(threadPool);
      return this;
    }

    public Builder<T> withProcessThreadPool(ExecutorService threadPool) {
      optionsBuilder.withProcessThreadPool(threadPool);
      return this;
    }

    public Builder<T> set(String key, String value) {
      optionsBuilder.set(key, value);
      return this;
    }

    protected ReadSupport<T> getReadSupport() {
      // if readSupport is null, the protected constructor must have been used
      Preconditions.checkArgument(
          readSupport != null, "[BUG] Classes that extend Builder should override getReadSupport()");
      return readSupport;
    }

    public ParquetReader<T> build() throws IOException {
      ParquetReadOptions options = optionsBuilder.withAllocator(allocator).build();

      if (path != null) {
        Configuration hadoopConf = ConfigurationUtil.createHadoopConfiguration(configuration);
        FileSystem fs = path.getFileSystem(hadoopConf);
        FileStatus stat = fs.getFileStatus(path);

        if (stat.isFile()) {
          return new ParquetReader<>(
              Collections.singletonList((InputFile) HadoopInputFile.fromStatus(stat, hadoopConf)),
              options,
              getReadSupport());
        } else {
          List<InputFile> files = new ArrayList<>();
          for (FileStatus fileStatus : fs.listStatus(path, HiddenFileFilter.INSTANCE)) {
            files.add(HadoopInputFile.fromStatus(fileStatus, hadoopConf));
          }
          return new ParquetReader<T>(files, options, getReadSupport());
        }
      } else {
        return new ParquetReader<>(Collections.singletonList(file), options, getReadSupport());
      }
    }
  }
}
