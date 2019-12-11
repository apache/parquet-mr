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

import static org.apache.parquet.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.HadoopReadOptions;
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
  public ParquetReader(Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
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
  public ParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
    this(conf, file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  public ParquetReader(Configuration conf,
                        Path file,
                        ReadSupport<T> readSupport,
                        FilterCompat.Filter filter) throws IOException {
    this(Collections.singletonList((InputFile) HadoopInputFile.fromPath(file, conf)),
        HadoopReadOptions.builder(conf)
            .withRecordFilter(checkNotNull(filter, "filter"))
            .build(),
        readSupport);
  }

  private ParquetReader(List<InputFile> files,
                        ParquetReadOptions options,
                        ReadSupport<T> readSupport) throws IOException {
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

  public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path) {
    return new Builder<>(readSupport, path);
  }

  public static class Builder<T> {
    private final ReadSupport<T> readSupport;
    private final InputFile file;
    private final Path path;
    private Filter filter = null;
    protected Configuration conf;
    private ParquetReadOptions.Builder optionsBuilder;

    @Deprecated
    private Builder(ReadSupport<T> readSupport, Path path) {
      this.readSupport = checkNotNull(readSupport, "readSupport");
      this.file = null;
      this.path = checkNotNull(path, "path");
      this.conf = new Configuration();
      this.optionsBuilder = HadoopReadOptions.builder(conf);
    }

    @Deprecated
    protected Builder(Path path) {
      this.readSupport = null;
      this.file = null;
      this.path = checkNotNull(path, "path");
      this.conf = new Configuration();
      this.optionsBuilder = HadoopReadOptions.builder(conf);
    }

    protected Builder(InputFile file) {
      this.readSupport = null;
      this.file = checkNotNull(file, "file");
      this.path = null;
      if (file instanceof HadoopInputFile) {
        this.conf = ((HadoopInputFile) file).getConfiguration();
      } else {
        this.conf = new Configuration();
      }
      optionsBuilder = HadoopReadOptions.builder(conf);
    }

    // when called, resets options to the defaults from conf
    public Builder<T> withConf(Configuration conf) {
      this.conf = checkNotNull(conf, "conf");

      // previous versions didn't use the builder, so may set filter before conf. this maintains
      // compatibility for filter. other options are reset by a new conf.
      this.optionsBuilder = HadoopReadOptions.builder(conf);
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

    public Builder<T> set(String key, String value) {
      optionsBuilder.set(key, value);
      return this;
    }

    protected ReadSupport<T> getReadSupport() {
      // if readSupport is null, the protected constructor must have been used
      Preconditions.checkArgument(readSupport != null,
          "[BUG] Classes that extend Builder should override getReadSupport()");
      return readSupport;
    }

    public ParquetReader<T> build() throws IOException {
      ParquetReadOptions options = optionsBuilder.build();

      if (path != null) {
        FileSystem fs = path.getFileSystem(conf);
        FileStatus stat = fs.getFileStatus(path);

        if (stat.isFile()) {
          return new ParquetReader<>(
              Collections.singletonList((InputFile) HadoopInputFile.fromStatus(stat, conf)),
              options,
              getReadSupport());

        } else {
          List<InputFile> files = new ArrayList<>();
          for (FileStatus fileStatus : fs.listStatus(path, HiddenFileFilter.INSTANCE)) {
            files.add(HadoopInputFile.fromStatus(fileStatus, conf));
          }
          return new ParquetReader<T>(files, options, getReadSupport());
        }

      } else {
        return new ParquetReader<>(Collections.singletonList(file), options, getReadSupport());
      }
    }
  }
}
