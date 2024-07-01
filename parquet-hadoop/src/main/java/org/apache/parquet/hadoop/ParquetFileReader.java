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

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.BLOOMFILTER;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.DICTIONARY;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.STATISTICS;
import static org.apache.parquet.format.Util.readFileCryptoMetaData;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS;
import static org.apache.parquet.hadoop.ColumnIndexFilterUtils.calculateOffsetRanges;
import static org.apache.parquet.hadoop.ColumnIndexFilterUtils.filterOffsetIndex;
import static org.apache.parquet.hadoop.ParquetFileWriter.EFMAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.PARQUET_COMMON_METADATA_FILE;
import static org.apache.parquet.hadoop.ParquetFileWriter.PARQUET_METADATA_FILE;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.ByteBufferReleaser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.ReusingByteBufferAllocator;
import org.apache.parquet.bytes.SequenceByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.ColumnChunkPageReadStore.ColumnChunkPageReader;
import org.apache.parquet.hadoop.ColumnIndexFilterUtils.OffsetRange;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.hadoop.util.wrapped.io.FutureIO;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal implementation of the Parquet file reader as a block container
 */
public class ParquetFileReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileReader.class);

  public static final String PARQUET_READ_PARALLELISM = "parquet.metadata.read.parallelism";

  public static final long HADOOP_VECTORED_READ_TIMEOUT_SECONDS = 300;

  final ParquetMetadataConverter converter;

  private final CRC32 crc;
  private final ReusingByteBufferAllocator crcAllocator;

  /**
   * for files provided, check if there's a summary file.
   * If a summary file is found it is used otherwise the file footer is used.
   *
   * @param configuration the hadoop conf to connect to the file system;
   * @param partFiles the part files to read
   * @return the footers for those files using the summary file if possible.
   * @throws IOException if there is an exception while reading footers
   * @deprecated metadata files are not recommended and will be removed in 2.0.0
   */
  @Deprecated
  public static List<Footer> readAllFootersInParallelUsingSummaryFiles(
      Configuration configuration, List<FileStatus> partFiles) throws IOException {
    return readAllFootersInParallelUsingSummaryFiles(configuration, partFiles, false);
  }

  private static MetadataFilter filter(boolean skipRowGroups) {
    return skipRowGroups ? SKIP_ROW_GROUPS : NO_FILTER;
  }

  /**
   * for files provided, check if there's a summary file.
   * If a summary file is found it is used otherwise the file footer is used.
   *
   * @param configuration the hadoop conf to connect to the file system;
   * @param partFiles the part files to read
   * @param skipRowGroups to skipRowGroups in the footers
   * @return the footers for those files using the summary file if possible.
   * @throws IOException if there is an exception while reading footers
   * @deprecated metadata files are not recommended and will be removed in 2.0.0
   */
  @Deprecated
  public static List<Footer> readAllFootersInParallelUsingSummaryFiles(
      final Configuration configuration, final Collection<FileStatus> partFiles, final boolean skipRowGroups)
      throws IOException {

    // figure out list of all parents to part files
    Set<Path> parents = new HashSet<Path>();
    for (FileStatus part : partFiles) {
      parents.add(part.getPath().getParent());
    }

    // read corresponding summary files if they exist
    List<Callable<Map<Path, Footer>>> summaries = new ArrayList<Callable<Map<Path, Footer>>>();
    for (final Path path : parents) {
      summaries.add(() -> {
        ParquetMetadata mergedMetadata = readSummaryMetadata(configuration, path, skipRowGroups);
        if (mergedMetadata != null) {
          final List<Footer> footers;
          if (skipRowGroups) {
            footers = new ArrayList<Footer>();
            for (FileStatus f : partFiles) {
              footers.add(new Footer(f.getPath(), mergedMetadata));
            }
          } else {
            footers = footersFromSummaryFile(path, mergedMetadata);
          }
          Map<Path, Footer> map = new HashMap<Path, Footer>();
          for (Footer footer : footers) {
            // the folder may have been moved
            footer = new Footer(new Path(path, footer.getFile().getName()), footer.getParquetMetadata());
            map.put(footer.getFile(), footer);
          }
          return map;
        } else {
          return Collections.emptyMap();
        }
      });
    }

    Map<Path, Footer> cache = new HashMap<Path, Footer>();
    try {
      List<Map<Path, Footer>> footersFromSummaries =
          runAllInParallel(configuration.getInt(PARQUET_READ_PARALLELISM, 5), summaries);
      for (Map<Path, Footer> footers : footersFromSummaries) {
        cache.putAll(footers);
      }
    } catch (ExecutionException e) {
      throw new IOException("Error reading summaries", e);
    }

    // keep only footers for files actually requested and read file footer if not found in summaries
    List<Footer> result = new ArrayList<Footer>(partFiles.size());
    List<FileStatus> toRead = new ArrayList<FileStatus>();
    for (FileStatus part : partFiles) {
      Footer f = cache.get(part.getPath());
      if (f != null) {
        result.add(f);
      } else {
        toRead.add(part);
      }
    }

    if (toRead.size() > 0) {
      // read the footers of the files that did not have a summary file
      LOG.info("reading another {} footers", toRead.size());
      result.addAll(readAllFootersInParallel(configuration, toRead, skipRowGroups));
    }

    return result;
  }

  private static <T> List<T> runAllInParallel(int parallelism, List<Callable<T>> toRun) throws ExecutionException {
    LOG.info("Initiating action with parallelism: {}", parallelism);
    ExecutorService threadPool = Executors.newFixedThreadPool(parallelism);
    try {
      List<Future<T>> futures = new ArrayList<Future<T>>();
      for (Callable<T> callable : toRun) {
        futures.add(threadPool.submit(callable));
      }
      List<T> result = new ArrayList<T>(toRun.size());
      for (Future<T> future : futures) {
        try {
          result.add(future.get());
        } catch (InterruptedException e) {
          throw new RuntimeException("The thread was interrupted", e);
        }
      }
      return result;
    } finally {
      threadPool.shutdownNow();
    }
  }

  /**
   * @param configuration the conf to access the File System
   * @param partFiles the files to read
   * @return the footers
   * @throws IOException if an exception was raised while reading footers
   * @deprecated metadata files are not recommended and will be removed in 2.0.0
   */
  @Deprecated
  public static List<Footer> readAllFootersInParallel(final Configuration configuration, List<FileStatus> partFiles)
      throws IOException {
    return readAllFootersInParallel(configuration, partFiles, false);
  }

  /**
   * read all the footers of the files provided
   * (not using summary files)
   *
   * @param configuration the conf to access the File System
   * @param partFiles the files to read
   * @param skipRowGroups to skip the rowGroup info
   * @return the footers
   * @throws IOException if there is an exception while reading footers
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static List<Footer> readAllFootersInParallel(
      final Configuration configuration, List<FileStatus> partFiles, final boolean skipRowGroups)
      throws IOException {
    List<Callable<Footer>> footers = new ArrayList<Callable<Footer>>();
    for (final FileStatus currentFile : partFiles) {
      footers.add(() -> {
        try {
          return new Footer(
              currentFile.getPath(), readFooter(configuration, currentFile, filter(skipRowGroups)));
        } catch (IOException e) {
          throw new IOException("Could not read footer for file " + currentFile, e);
        }
      });
    }
    try {
      return runAllInParallel(configuration.getInt(PARQUET_READ_PARALLELISM, 5), footers);
    } catch (ExecutionException e) {
      throw new IOException("Could not read footer: " + e.getMessage(), e.getCause());
    }
  }

  /**
   * Read the footers of all the files under that path (recursively)
   * not using summary files.
   *
   * @param configuration a configuration
   * @param fileStatus a file status to recursively list
   * @param skipRowGroups whether to skip reading row group metadata
   * @return a list of footers
   * @throws IOException if an exception is thrown while reading the footers
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static List<Footer> readAllFootersInParallel(
      Configuration configuration, FileStatus fileStatus, boolean skipRowGroups) throws IOException {
    List<FileStatus> statuses = listFiles(configuration, fileStatus);
    return readAllFootersInParallel(configuration, statuses, skipRowGroups);
  }

  /**
   * Read the footers of all the files under that path (recursively)
   * not using summary files.
   * rowGroups are not skipped
   *
   * @param configuration the configuration to access the FS
   * @param fileStatus the root dir
   * @return all the footers
   * @throws IOException if an exception is thrown while reading the footers
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static List<Footer> readAllFootersInParallel(Configuration configuration, FileStatus fileStatus)
      throws IOException {
    return readAllFootersInParallel(configuration, fileStatus, false);
  }

  /**
   * @param configuration a configuration
   * @param path a file path
   * @return a list of footers
   * @throws IOException if an exception is thrown while reading the footers
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static List<Footer> readFooters(Configuration configuration, Path path) throws IOException {
    return readFooters(configuration, status(configuration, path));
  }

  private static FileStatus status(Configuration configuration, Path path) throws IOException {
    return path.getFileSystem(configuration).getFileStatus(path);
  }

  /**
   * this always returns the row groups
   *
   * @param configuration a configuration
   * @param pathStatus a file status to read footers from
   * @return a list of footers
   * @throws IOException if an exception is thrown while reading the footers
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static List<Footer> readFooters(Configuration configuration, FileStatus pathStatus) throws IOException {
    return readFooters(configuration, pathStatus, false);
  }

  /**
   * Read the footers of all the files under that path (recursively)
   * using summary files if possible
   *
   * @param configuration the configuration to access the FS
   * @param pathStatus the root dir
   * @param skipRowGroups whether to skip reading row group metadata
   * @return all the footers
   * @throws IOException if an exception is thrown while reading the footers
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static List<Footer> readFooters(Configuration configuration, FileStatus pathStatus, boolean skipRowGroups)
      throws IOException {
    List<FileStatus> files = listFiles(configuration, pathStatus);
    return readAllFootersInParallelUsingSummaryFiles(configuration, files, skipRowGroups);
  }

  static boolean filterHiddenFiles(FileStatus file) {
    final char c = file.getPath().getName().charAt(0);
    return c != '.' && c != '_';
  }

  private static List<FileStatus> listFiles(Configuration conf, FileStatus fileStatus) throws IOException {
    if (fileStatus.isDir()) {
      FileSystem fs = fileStatus.getPath().getFileSystem(conf);
      return Arrays.stream(fs.listStatus(fileStatus.getPath()))
          .filter(ParquetFileReader::filterHiddenFiles)
          .flatMap(sub -> {
            try {
              return listFiles(conf, sub).stream();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .collect(Collectors.toList());
    } else {
      return Collections.singletonList(fileStatus);
    }
  }

  /**
   * Specifically reads a given summary file
   *
   * @param configuration a configuration
   * @param summaryStatus file status for a summary file
   * @return the metadata translated for each file
   * @throws IOException if an exception is thrown while reading the summary file
   * @deprecated metadata files are not recommended and will be removed in 2.0.0
   */
  @Deprecated
  public static List<Footer> readSummaryFile(Configuration configuration, FileStatus summaryStatus)
      throws IOException {
    final Path parent = summaryStatus.getPath().getParent();
    ParquetMetadata mergedFooters = readFooter(configuration, summaryStatus, filter(false));
    return footersFromSummaryFile(parent, mergedFooters);
  }

  static ParquetMetadata readSummaryMetadata(Configuration configuration, Path basePath, boolean skipRowGroups)
      throws IOException {
    Path metadataFile = new Path(basePath, PARQUET_METADATA_FILE);
    Path commonMetaDataFile = new Path(basePath, PARQUET_COMMON_METADATA_FILE);
    FileSystem fileSystem = basePath.getFileSystem(configuration);
    if (skipRowGroups && fileSystem.exists(commonMetaDataFile)) {
      // reading the summary file that does not contain the row groups
      LOG.info("reading summary file: {}", commonMetaDataFile);
      return readFooter(configuration, commonMetaDataFile, filter(skipRowGroups));
    } else if (fileSystem.exists(metadataFile)) {
      LOG.info("reading summary file: {}", metadataFile);
      return readFooter(configuration, metadataFile, filter(skipRowGroups));
    } else {
      return null;
    }
  }

  static List<Footer> footersFromSummaryFile(final Path parent, ParquetMetadata mergedFooters) {
    Map<Path, ParquetMetadata> footers = new HashMap<>();
    List<BlockMetaData> blocks = mergedFooters.getBlocks();
    for (BlockMetaData block : blocks) {
      String path = block.getPath();
      Path fullPath = new Path(parent, path);
      ParquetMetadata current = footers.get(fullPath);
      if (current == null) {
        current = new ParquetMetadata(mergedFooters.getFileMetaData(), new ArrayList<BlockMetaData>());
        footers.put(fullPath, current);
      }
      current.getBlocks().add(block);
    }
    List<Footer> result = new ArrayList<Footer>();
    for (Entry<Path, ParquetMetadata> entry : footers.entrySet()) {
      result.add(new Footer(entry.getKey(), entry.getValue()));
    }
    return result;
  }

  /**
   * Reads the meta data block in the footer of the file
   *
   * @param configuration a configuration
   * @param file the parquet File
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static final ParquetMetadata readFooter(Configuration configuration, Path file) throws IOException {
    return readFooter(configuration, file, NO_FILTER);
  }

  /**
   * Reads the meta data in the footer of the file.
   * Skipping row groups (or not) based on the provided filter
   *
   * @param configuration a configuration
   * @param file the Parquet File
   * @param filter the filter to apply to row groups
   * @return the metadata with row groups filtered.
   * @throws IOException  if an error occurs while reading the file
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static ParquetMetadata readFooter(Configuration configuration, Path file, MetadataFilter filter)
      throws IOException {
    return readFooter(HadoopInputFile.fromPath(file, configuration), filter);
  }

  /**
   * @param configuration a configuration
   * @param file the Parquet File
   * @return the metadata with row groups.
   * @throws IOException  if an error occurs while reading the file
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file) throws IOException {
    return readFooter(configuration, file, NO_FILTER);
  }

  /**
   * Reads the meta data block in the footer of the file
   *
   * @param configuration a configuration
   * @param file the parquet File
   * @param filter the filter to apply to row groups
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static final ParquetMetadata readFooter(Configuration configuration, FileStatus file, MetadataFilter filter)
      throws IOException {
    return readFooter(HadoopInputFile.fromStatus(file, configuration), filter);
  }

  /**
   * Reads the meta data block in the footer of the file using provided input stream
   *
   * @param file a {@link InputFile} to read
   * @param filter the filter to apply to row groups
   * @return the metadata blocks in the footer
   * @throws IOException if an error occurs while reading the file
   * @deprecated will be removed in 2.0.0;
   *             use {@link ParquetFileReader#open(InputFile, ParquetReadOptions)}
   */
  @Deprecated
  public static final ParquetMetadata readFooter(InputFile file, MetadataFilter filter) throws IOException {
    ParquetReadOptions options;
    if (file instanceof HadoopInputFile) {
      HadoopInputFile hadoopFile = (HadoopInputFile) file;
      options = HadoopReadOptions.builder(hadoopFile.getConfiguration(), hadoopFile.getPath())
          .withMetadataFilter(filter)
          .build();
    } else {
      options = ParquetReadOptions.builder().withMetadataFilter(filter).build();
    }

    try (SeekableInputStream in = file.newStream()) {
      return readFooter(file, options, in);
    }
  }

  public static final ParquetMetadata readFooter(InputFile file, ParquetReadOptions options, SeekableInputStream f)
      throws IOException {
    ParquetMetadataConverter converter = new ParquetMetadataConverter(options);
    return readFooter(file, options, f, converter);
  }

  private static final ParquetMetadata readFooter(
      InputFile file, ParquetReadOptions options, SeekableInputStream f, ParquetMetadataConverter converter)
      throws IOException {

    long fileLen = file.getLength();
    String filePath = file.toString();
    LOG.debug("File length {}", fileLen);

    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(filePath + " is not a Parquet file (length is too low: " + fileLen + ")");
    }

    // Read footer length and magic string - with a single seek
    byte[] magic = new byte[MAGIC.length];
    long fileMetadataLengthIndex = fileLen - magic.length - FOOTER_LENGTH_SIZE;
    LOG.debug("reading footer index at {}", fileMetadataLengthIndex);
    f.seek(fileMetadataLengthIndex);
    int fileMetadataLength = readIntLittleEndian(f);
    f.readFully(magic);

    boolean encryptedFooterMode;
    if (Arrays.equals(MAGIC, magic)) {
      encryptedFooterMode = false;
    } else if (Arrays.equals(EFMAGIC, magic)) {
      encryptedFooterMode = true;
    } else {
      throw new RuntimeException(filePath + " is not a Parquet file. Expected magic number at tail, but found "
          + Arrays.toString(magic));
    }

    long fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;
    LOG.debug("read footer length: {}, footer index: {}", fileMetadataLength, fileMetadataIndex);
    if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
      throw new RuntimeException("corrupted file: the footer index is not within the file: " + fileMetadataIndex);
    }
    f.seek(fileMetadataIndex);

    FileDecryptionProperties fileDecryptionProperties = options.getDecryptionProperties();
    InternalFileDecryptor fileDecryptor = null;
    if (null != fileDecryptionProperties) {
      fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
    }

    // Read all the footer bytes in one time to avoid multiple read operations,
    // since it can be pretty time consuming for a single read operation in HDFS.
    ByteBuffer footerBytesBuffer = options.getAllocator().allocate(fileMetadataLength);
    try {
      f.readFully(footerBytesBuffer);
      LOG.debug("Finished to read all footer bytes.");
      footerBytesBuffer.flip();
      InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);

      // Regular file, or encrypted file with plaintext footer
      if (!encryptedFooterMode) {
        return converter.readParquetMetadata(
            footerBytesStream, options.getMetadataFilter(), fileDecryptor, false, fileMetadataLength);
      }

      // Encrypted file with encrypted footer
      if (null == fileDecryptor) {
        throw new ParquetCryptoRuntimeException("Trying to read file with encrypted footer. No keys available");
      }
      FileCryptoMetaData fileCryptoMetaData = readFileCryptoMetaData(footerBytesStream);
      fileDecryptor.setFileCryptoMetaData(
          fileCryptoMetaData.getEncryption_algorithm(), true, fileCryptoMetaData.getKey_metadata());
      // footer length is required only for signed plaintext footers
      return converter.readParquetMetadata(
          footerBytesStream, options.getMetadataFilter(), fileDecryptor, true, 0);
    } finally {
      options.getAllocator().release(footerBytesBuffer);
    }
  }

  /**
   * @param conf a configuration
   * @param file a file path to open
   * @return a parquet file reader
   * @throws IOException if there is an error while opening the file
   * @deprecated will be removed in 2.0.0; use {@link #open(InputFile)}
   */
  @Deprecated
  public static ParquetFileReader open(Configuration conf, Path file) throws IOException {
    return new ParquetFileReader(
        HadoopInputFile.fromPath(file, conf),
        HadoopReadOptions.builder(conf, file).build());
  }

  /**
   * @param conf a configuration
   * @param file a file path to open
   * @param filter a metadata filter
   * @return a parquet file reader
   * @throws IOException if there is an error while opening the file
   * @deprecated will be removed in 2.0.0; use {@link #open(InputFile,ParquetReadOptions)}
   */
  @Deprecated
  public static ParquetFileReader open(Configuration conf, Path file, MetadataFilter filter) throws IOException {
    return open(
        HadoopInputFile.fromPath(file, conf),
        HadoopReadOptions.builder(conf, file).withMetadataFilter(filter).build());
  }

  /**
   * @param conf a configuration
   * @param file a file path to open
   * @param footer a footer for the file if already loaded
   * @return a parquet file reader
   * @throws IOException if there is an error while opening the file
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public static ParquetFileReader open(Configuration conf, Path file, ParquetMetadata footer) throws IOException {
    return new ParquetFileReader(conf, file, footer);
  }

  /**
   * Open a {@link InputFile file}.
   *
   * @param file an input file
   * @return an open ParquetFileReader
   * @throws IOException if there is an error while opening the file
   */
  public static ParquetFileReader open(InputFile file) throws IOException {
    return new ParquetFileReader(file, ParquetReadOptions.builder().build());
  }

  /**
   * Open a {@link InputFile file} with {@link ParquetReadOptions options}.
   *
   * @param file an input file
   * @param options parquet read options
   * @return an open ParquetFileReader
   * @throws IOException if there is an error while opening the file
   */
  public static ParquetFileReader open(InputFile file, ParquetReadOptions options) throws IOException {
    return new ParquetFileReader(file, options);
  }

  protected final SeekableInputStream f;
  // input streams opened in the async mode
  protected final List<SeekableInputStream> inputStreams = new ArrayList<>();
  protected final List<ByteBufferInputStream> bufferInputStreams = new ArrayList<>();
  private final InputFile file;
  final ParquetReadOptions options;
  private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();
  private final FileMetaData fileMetaData; // may be null
  private final List<BlockMetaData> blocks;
  private final List<ColumnIndexStore> blockIndexStores;
  private final List<RowRanges> blockRowRanges;

  // not final. in some cases, this may be lazily loaded for backward-compat.
  private ParquetMetadata footer;

  private int currentBlock = 0;
  private ColumnChunkPageReadStore currentRowGroup = null;
  private DictionaryPageReader nextDictionaryReader = null;

  private InternalFileDecryptor fileDecryptor = null;

  /**
   * @param configuration the Hadoop conf
   * @param filePath Path for the parquet file
   * @param blocks the blocks to read
   * @param columns the columns to read (their path)
   * @throws IOException if the file can not be opened
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public ParquetFileReader(
      Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns)
      throws IOException {
    this(configuration, null, filePath, blocks, columns);
  }

  /**
   * @param configuration the Hadoop conf
   * @param fileMetaData fileMetaData for parquet file
   * @param filePath Path for the parquet file
   * @param blocks the blocks to read
   * @param columns the columns to read (their path)
   * @throws IOException if the file can not be opened
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public ParquetFileReader(
      Configuration configuration,
      FileMetaData fileMetaData,
      Path filePath,
      List<BlockMetaData> blocks,
      List<ColumnDescriptor> columns)
      throws IOException {
    this.converter = new ParquetMetadataConverter(configuration);
    this.file = HadoopInputFile.fromPath(filePath, configuration);
    this.fileMetaData = fileMetaData;
    this.f = file.newStream();
    this.fileDecryptor = fileMetaData.getFileDecryptor();
    if (null == fileDecryptor) {
      this.options = HadoopReadOptions.builder(configuration).build();
    } else {
      this.options = HadoopReadOptions.builder(configuration)
          .withDecryption(fileDecryptor.getDecryptionProperties())
          .build();
    }
    try {
      this.blocks = filterRowGroups(blocks);
    } catch (Exception e) {
      // In case that filterRowGroups throws an exception in the constructor, the new stream
      // should be closed. Otherwise, there's no way to close this outside.
      f.close();
      throw e;
    }
    this.blockIndexStores = listWithNulls(this.blocks.size());
    this.blockRowRanges = listWithNulls(this.blocks.size());
    for (ColumnDescriptor col : columns) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }

    if (options.usePageChecksumVerification()) {
      this.crc = new CRC32();
      this.crcAllocator = ReusingByteBufferAllocator.strict(options.getAllocator());
    } else {
      this.crc = null;
      this.crcAllocator = null;
    }
  }

  /**
   * @param conf the Hadoop Configuration
   * @param file Path to a parquet file
   * @param filter a {@link MetadataFilter} for selecting row groups
   * @throws IOException if the file can not be opened
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public ParquetFileReader(Configuration conf, Path file, MetadataFilter filter) throws IOException {
    this(
        HadoopInputFile.fromPath(file, conf),
        HadoopReadOptions.builder(conf, file).withMetadataFilter(filter).build());
  }

  /**
   * @param conf the Hadoop Configuration
   * @param file Path to a parquet file
   * @param footer a {@link ParquetMetadata} footer already read from the file
   * @throws IOException if the file can not be opened
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public ParquetFileReader(Configuration conf, Path file, ParquetMetadata footer) throws IOException {
    this.converter = new ParquetMetadataConverter(conf);
    this.file = HadoopInputFile.fromPath(file, conf);
    this.f = this.file.newStream();
    this.fileMetaData = footer.getFileMetaData();
    this.fileDecryptor = fileMetaData.getFileDecryptor();
    if (null == fileDecryptor) {
      this.options = HadoopReadOptions.builder(conf).build();
    } else {
      this.options = HadoopReadOptions.builder(conf)
          .withDecryption(fileDecryptor.getDecryptionProperties())
          .build();
    }
    this.footer = footer;
    try {
      this.blocks = filterRowGroups(footer.getBlocks());
    } catch (Exception e) {
      // In case that filterRowGroups throws an exception in the constructor, the new stream
      // should be closed. Otherwise, there's no way to close this outside.
      f.close();
      throw e;
    }
    this.blockIndexStores = listWithNulls(this.blocks.size());
    this.blockRowRanges = listWithNulls(this.blocks.size());
    for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }

    if (options.usePageChecksumVerification()) {
      this.crc = new CRC32();
      this.crcAllocator = ReusingByteBufferAllocator.strict(options.getAllocator());
    } else {
      this.crc = null;
      this.crcAllocator = null;
    }
  }

  /**
   * @param conf   the Hadoop Configuration
   * @param file   Path to a parquet file
   * @param footer a {@link ParquetMetadata} footer already read from the file
   * @param options {@link ParquetReadOptions}
   * @throws IOException if the file can not be opened
   */
  public ParquetFileReader(Configuration conf, Path file, ParquetMetadata footer, ParquetReadOptions options)
      throws IOException {
    this.converter = new ParquetMetadataConverter(conf);
    this.file = HadoopInputFile.fromPath(file, conf);
    this.f = this.file.newStream();
    this.fileMetaData = footer.getFileMetaData();
    this.fileDecryptor = fileMetaData.getFileDecryptor();
    this.options = options;
    this.footer = footer;
    try {
      this.blocks = filterRowGroups(footer.getBlocks());
    } catch (Exception e) {
      // In case that filterRowGroups throws an exception in the constructor, the new stream
      // should be closed. Otherwise, there's no way to close this outside.
      f.close();
      throw e;
    }
    this.blockIndexStores = listWithNulls(this.blocks.size());
    this.blockRowRanges = listWithNulls(this.blocks.size());
    for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }

    if (options.usePageChecksumVerification()) {
      this.crc = new CRC32();
      this.crcAllocator = ReusingByteBufferAllocator.strict(options.getAllocator());
    } else {
      this.crc = null;
      this.crcAllocator = null;
    }
  }

  public ParquetFileReader(InputFile file, ParquetReadOptions options) throws IOException {
    this.converter = new ParquetMetadataConverter(options);
    this.file = file;
    this.f = file.newStream();
    this.options = options;
    try {
      this.footer = readFooter(file, options, f, converter);
    } catch (Exception e) {
      // In case that reading footer throws an exception in the constructor, the new stream
      // should be closed. Otherwise, there's no way to close this outside.
      f.close();
      throw e;
    }
    this.fileMetaData = footer.getFileMetaData();
    this.fileDecryptor = fileMetaData.getFileDecryptor(); // must be called before filterRowGroups!
    if (null != fileDecryptor && fileDecryptor.plaintextFile()) {
      this.fileDecryptor = null; // Plaintext file. No need in decryptor
    }

    try {
      this.blocks = filterRowGroups(footer.getBlocks());
    } catch (Exception e) {
      // In case that filterRowGroups throws an exception in the constructor, the new stream
      // should be closed. Otherwise, there's no way to close this outside.
      f.close();
      throw e;
    }
    this.blockIndexStores = listWithNulls(this.blocks.size());
    this.blockRowRanges = listWithNulls(this.blocks.size());
    for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }

    if (options.usePageChecksumVerification()) {
      this.crc = new CRC32();
      this.crcAllocator = ReusingByteBufferAllocator.strict(options.getAllocator());
    } else {
      this.crc = null;
      this.crcAllocator = null;
    }
  }

  private boolean isAsyncIOReaderEnabled() {
    if (options.isAsyncIOReaderEnabled()) {
      if (options.getIOThreadPool() != null) {
        return true;
      } else {
        LOG.debug("Parquet async IO is configured but the IO thread pool has not been "
            + "initialized. Configuration is being ignored");
      }
    }
    return false;
  }

  boolean isParallelColumnReaderEnabled() {
    if (options.isParallelColumnReaderEnabled()) {
      if (options.getProcessThreadPool() != null) {
        return true;
      } else {
        LOG.debug("Parallel column reading is configured but the process thread pool has "
            + "not been initialized. Configuration is being ignored");
      }
    }
    return false;
  }

  private static <T> List<T> listWithNulls(int size) {
    return new ArrayList<>(Collections.nCopies(size, null));
  }

  public ParquetMetadata getFooter() {
    if (footer == null) {
      try {
        // don't read the row groups because this.blocks is always set
        this.footer = readFooter(file, options, f, converter);
      } catch (IOException e) {
        throw new ParquetDecodingException("Unable to read file footer", e);
      }
    }
    return footer;
  }

  public FileMetaData getFileMetaData() {
    if (fileMetaData != null) {
      return fileMetaData;
    }
    return getFooter().getFileMetaData();
  }

  public long getRecordCount() {
    long total = 0L;
    for (BlockMetaData block : blocks) {
      total += block.getRowCount();
    }
    return total;
  }

  public long getFilteredRecordCount() {
    if (!options.useColumnIndexFilter() || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return getRecordCount();
    }
    long total = 0L;
    for (int i = 0, n = blocks.size(); i < n; ++i) {
      total += getRowRanges(i).rowCount();
    }
    return total;
  }

  /**
   * @return the path for this file
   * @deprecated will be removed in 2.0.0; use {@link #getFile()} instead
   */
  @Deprecated
  public Path getPath() {
    return new Path(file.toString());
  }

  public String getFile() {
    return file.toString();
  }

  private List<BlockMetaData> filterRowGroups(List<BlockMetaData> blocks) throws IOException {
    FilterCompat.Filter recordFilter = options.getRecordFilter();
    if (FilterCompat.isFilteringRequired(recordFilter)) {
      // set up data filters based on configured levels
      List<RowGroupFilter.FilterLevel> levels = new ArrayList<>();

      if (options.useStatsFilter()) {
        levels.add(STATISTICS);
      }

      if (options.useDictionaryFilter()) {
        levels.add(DICTIONARY);
      }

      if (options.useBloomFilter()) {
        levels.add(BLOOMFILTER);
      }
      return RowGroupFilter.filterRowGroups(levels, recordFilter, blocks, this);
    }

    return blocks;
  }

  public List<BlockMetaData> getRowGroups() {
    return blocks;
  }

  public void setRequestedSchema(MessageType projection) {
    paths.clear();
    for (ColumnDescriptor col : projection.getColumns()) {
      paths.put(ColumnPath.get(col.getPath()), col);
    }
  }

  public void appendTo(ParquetFileWriter writer) throws IOException {
    writer.appendRowGroups(f, blocks, true);
  }

  /**
   * Reads all the columns requested from the row group at the specified block.
   *
   * @param blockIndex the index of the requested block
   * @return the PageReadStore which can provide PageReaders for each column.
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readRowGroup(int blockIndex) throws IOException {
    return internalReadRowGroup(blockIndex);
  }

  /**
   * Reads all the columns requested from the row group at the current file position.
   *
   * @return the PageReadStore which can provide PageReaders for each column.
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readNextRowGroup() throws IOException {
    ColumnChunkPageReadStore rowGroup = null;
    try {
      rowGroup = internalReadRowGroup(currentBlock);
    } catch (ParquetEmptyBlockException e) {
      LOG.warn("Read empty block at index {} from {}", currentBlock, getFile());
      advanceToNextBlock();
      return readNextRowGroup();
    }

    if (rowGroup == null) {
      return null;
    }
    if (this.currentRowGroup != null) {
      this.currentRowGroup.close();
    }
    this.currentRowGroup = rowGroup;
    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return currentRowGroup;
  }

  private ColumnChunkPageReadStore internalReadRowGroup(int blockIndex) throws IOException {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(blockIndex);
    if (block.getRowCount() == 0) {
      throw new ParquetEmptyBlockException("Illegal row group of 0 rows");
    }
    ColumnChunkPageReadStore rowGroup =
        new ColumnChunkPageReadStore(block.getRowCount(), block.getRowIndexOffset());
    // prepare the list of consecutive parts to read them in one scan
    List<ConsecutivePartList> allParts = new ArrayList<>();
    ConsecutivePartList currentParts = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        // If async IO or parallel reader feature is enabled, we need a new stream for every column
        if (isAsyncIOReaderEnabled() || isParallelColumnReaderEnabled()) {
          currentParts = null;
        }
        BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
        long startingPos = mc.getStartingPos();
        // first part or not consecutive => new list
        if (currentParts == null || currentParts.endPos() != startingPos) {
          currentParts = new ConsecutivePartList(startingPos);
          allParts.add(currentParts);
        }
        currentParts.addChunk(new ChunkDescriptor(columnDescriptor, mc, startingPos, mc.getTotalSize()));
      }
    }
    // actually read all the chunks
    ChunkListBuilder builder = new ChunkListBuilder(block.getRowCount());
    if (isAsyncIOReaderEnabled()) {
      for (ConsecutivePartList consecutiveChunks : allParts) {

        SeekableInputStream is = file.newStream();
        consecutiveChunks.readAll(is, builder);
        inputStreams.add(is);
      }
    } else {
      readAllPartsVectoredOrNormal(allParts, builder);
    }
    rowGroup.setReleaser(builder.releaser);

    for (Chunk chunk : builder.build()) {
      readChunkPages(chunk, block, rowGroup);
    }

    return rowGroup;
  }

  /**
   * Reads all the columns requested from the specified row group. It may skip specific pages based on the column
   * indexes according to the actual filter. As the rows are not aligned among the pages of the different columns row
   * synchronization might be required. See the documentation of the class SynchronizingColumnReader for details.
   *
   * @param blockIndex the index of the requested block
   * @return the PageReadStore which can provide PageReaders for each column or null if there are no rows in this block
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readFilteredRowGroup(int blockIndex) throws IOException {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }

    // Filtering not required -> fall back to the non-filtering path
    if (!options.useColumnIndexFilter() || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return internalReadRowGroup(blockIndex);
    }

    BlockMetaData block = blocks.get(blockIndex);
    if (block.getRowCount() == 0) {
      throw new ParquetEmptyBlockException("Illegal row group of 0 rows");
    }

    RowRanges rowRanges = getRowRanges(blockIndex);
    return readFilteredRowGroup(blockIndex, rowRanges);
  }

  /**
   * Reads all the columns requested from the specified row group. It may skip specific pages based on the
   * {@code rowRanges} passed in. As the rows are not aligned among the pages of the different columns row
   * synchronization might be required. See the documentation of the class SynchronizingColumnReader for details.
   *
   * @param blockIndex the index of the requested block
   * @param rowRanges  the row ranges to be read from the requested block
   * @return the PageReadStore which can provide PageReaders for each column or null if there are no rows in this block
   * @throws IOException              if an error occurs while reading
   * @throws IllegalArgumentException if the {@code blockIndex} is invalid or the {@code rowRanges} is null
   */
  public ColumnChunkPageReadStore readFilteredRowGroup(int blockIndex, RowRanges rowRanges) throws IOException {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      throw new IllegalArgumentException(String.format(
          "Invalid block index %s, the valid block index range are: " + "[%s, %s]",
          blockIndex, 0, blocks.size() - 1));
    }

    if (Objects.isNull(rowRanges)) {
      throw new IllegalArgumentException("RowRanges must not be null");
    }

    BlockMetaData block = blocks.get(blockIndex);
    if (block.getRowCount() == 0L) {
      return null;
    }

    long rowCount = rowRanges.rowCount();
    if (rowCount == 0) {
      // There are no matching rows -> returning null
      return null;
    }

    if (rowCount == block.getRowCount()) {
      // All rows are matching -> fall back to the non-filtering path
      return internalReadRowGroup(blockIndex);
    }

    return internalReadFilteredRowGroup(block, rowRanges, getColumnIndexStore(blockIndex));
  }

  /**
   * Read data in all parts via either vectored IO or serial IO.
   * @param allParts all parts to be read.
   * @param builder used to build chunk list to read the pages for the different columns.
   * @throws IOException any IOE.
   */
  private void readAllPartsVectoredOrNormal(List<ConsecutivePartList> allParts, ChunkListBuilder builder)
      throws IOException {

    if (shouldUseVectoredIo(allParts)) {
      try {
        readVectored(allParts, builder);
        return;
      } catch (IllegalArgumentException | UnsupportedOperationException e) {
        // Either the arguments are wrong or somehow this is being invoked against
        // a hadoop release which doesn't have the API and yet somehow it got here.
        LOG.warn("readVectored() failed; falling back to normal IO against {}", f, e);
      }
    }
    for (ConsecutivePartList consecutiveChunks : allParts) {
      consecutiveChunks.readAll(f, builder);
    }
  }

  /**
   * Should the read use vectored IO?
   * <p>
   * This returns true if all necessary conditions are met:
   * <ol>
   *   <li> The option is enabled</li>
   *   <li> The Hadoop version supports vectored IO</li>
   *   <li> The part lengths are all valid for vectored IO</li>
   *   <li> The stream implementation explicitly supports the API; for other streams the classic
   *         API is always used.</li>
   *   <li> The allocator is not direct. This is to avoid HADOOP-19101 surfacing.
   * </ol>
   * @param allParts all parts to read.
   * @return true or false.
   */
  private boolean shouldUseVectoredIo(final List<ConsecutivePartList> allParts) {
    return options.useHadoopVectoredIo()
        && f.readVectoredAvailable(options.getAllocator())
        && arePartsValidForVectoredIo(allParts);
  }

  /**
   * Validate the parts for vectored IO.
   * Vectored IO doesn't support reading ranges of size greater than
   * Integer.MAX_VALUE.
   * @param allParts all parts to read.
   * @return true or false.
   */
  private boolean arePartsValidForVectoredIo(List<ConsecutivePartList> allParts) {
    for (ConsecutivePartList consecutivePart : allParts) {
      if (consecutivePart.length >= Integer.MAX_VALUE) {
        LOG.debug(
            "Part length {} greater than Integer.MAX_VALUE thus disabling vectored IO",
            consecutivePart.length);
        return false;
      }
    }
    return true;
  }

  /**
   * Read all parts through vectored IO.
   * <p>
   * The API is available in recent hadoop builds for all implementations of PositionedReadable;
   * the default implementation simply does a sequence of reads at different offsets.
   * <p>
   * If directly implemented by a Filesystem then it is likely to be a more efficient
   * operation such as a scatter-gather read (native IO) or set of parallel
   * GET requests against an object store.
   * @param allParts all parts to be read.
   * @param builder used to build chunk list to read the pages for the different columns.
   * @throws IOException any IOE.
   * @throws IllegalArgumentException arguments are invalid.
   * @throws UnsupportedOperationException if the filesystem does not support vectored IO.
   */
  private void readVectored(List<ConsecutivePartList> allParts, ChunkListBuilder builder) throws IOException {

    List<ParquetFileRange> ranges = new ArrayList<>(allParts.size());
    long totalSize = 0;
    for (ConsecutivePartList consecutiveChunks : allParts) {
      final long len = consecutiveChunks.length;
      Preconditions.checkArgument(
          len < Integer.MAX_VALUE,
          "Invalid length %s for vectored read operation. It must be less than max integer value.",
          len);
      ranges.add(new ParquetFileRange(consecutiveChunks.offset, (int) len));
      totalSize += len;
    }
    LOG.debug("Reading {} bytes of data with vectored IO in {} ranges", totalSize, ranges.size());
    // Request a vectored read;
    f.readVectored(ranges, options.getAllocator());
    int k = 0;
    for (ConsecutivePartList consecutivePart : allParts) {
      ParquetFileRange currRange = ranges.get(k++);
      consecutivePart.readFromVectoredRange(currRange, builder);
    }
  }

  /**
   * Reads all the columns requested from the row group at the current file position. It may skip specific pages based
   * on the column indexes according to the actual filter. As the rows are not aligned among the pages of the different
   * columns row synchronization might be required. See the documentation of the class SynchronizingColumnReader for
   * details.
   *
   * @return the PageReadStore which can provide PageReaders for each column
   * @throws IOException if an error occurs while reading
   */
  public PageReadStore readNextFilteredRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    // Filtering not required -> fall back to the non-filtering path
    if (!options.useColumnIndexFilter() || !FilterCompat.isFilteringRequired(options.getRecordFilter())) {
      return readNextRowGroup();
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0L) {
      LOG.warn("Read empty block at index {} from {}", currentBlock, getFile());
      // Skip the empty block
      advanceToNextBlock();
      return readNextFilteredRowGroup();
    }
    RowRanges rowRanges = getRowRanges(currentBlock);
    long rowCount = rowRanges.rowCount();
    if (rowCount == 0) {
      // There are no matching rows -> skipping this row-group
      advanceToNextBlock();
      return readNextFilteredRowGroup();
    }
    if (rowCount == block.getRowCount()) {
      // All rows are matching -> fall back to the non-filtering path
      return readNextRowGroup();
    }
    if (this.currentRowGroup != null) {
      this.currentRowGroup.close();
    }
    this.currentRowGroup = internalReadFilteredRowGroup(block, rowRanges, getColumnIndexStore(currentBlock));

    // avoid re-reading bytes the dictionary reader is used after this call
    if (nextDictionaryReader != null) {
      nextDictionaryReader.setRowGroup(currentRowGroup);
    }

    advanceToNextBlock();

    return this.currentRowGroup;
  }

  private ColumnChunkPageReadStore internalReadFilteredRowGroup(
      BlockMetaData block, RowRanges rowRanges, ColumnIndexStore ciStore) throws IOException {
    ColumnChunkPageReadStore rowGroup = new ColumnChunkPageReadStore(rowRanges, block.getRowIndexOffset());
    // prepare the list of consecutive parts to read them in one scan
    ChunkListBuilder builder = new ChunkListBuilder(block.getRowCount());
    List<ConsecutivePartList> allParts = new ArrayList<>();
    ConsecutivePartList currentParts = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      ColumnPath pathKey = mc.getPath();
      ColumnDescriptor columnDescriptor = paths.get(pathKey);
      if (columnDescriptor != null) {
        // If async IO or parallel reader feature is enabled, we need a new stream for every column
        if (isAsyncIOReaderEnabled() || isParallelColumnReaderEnabled()) {
          currentParts = null;
        }
        OffsetIndex offsetIndex = ciStore.getOffsetIndex(mc.getPath());

        OffsetIndex filteredOffsetIndex = filterOffsetIndex(offsetIndex, rowRanges, block.getRowCount());
        for (OffsetRange range : calculateOffsetRanges(filteredOffsetIndex, mc, offsetIndex.getOffset(0))) {
          BenchmarkCounter.incrementTotalBytes(range.getLength());
          long startingPos = range.getOffset();
          // first part or not consecutive => new list
          if (currentParts == null || currentParts.endPos() != startingPos) {
            currentParts = new ConsecutivePartList(startingPos);
            allParts.add(currentParts);
          }
          ChunkDescriptor chunkDescriptor =
              new ChunkDescriptor(columnDescriptor, mc, startingPos, range.getLength());
          currentParts.addChunk(chunkDescriptor);
          builder.setOffsetIndex(chunkDescriptor, filteredOffsetIndex);
        }
      }
    }
    // actually read all the chunks
    if (isAsyncIOReaderEnabled()) {
      for (ConsecutivePartList consecutiveChunks : allParts) {
        SeekableInputStream is = file.newStream();
        consecutiveChunks.readAll(is, builder);
        inputStreams.add(is);
      }
    } else {
      readAllPartsVectoredOrNormal(allParts, builder);
    }
    rowGroup.setReleaser(builder.releaser);
    for (Chunk chunk : builder.build()) {
      readChunkPages(chunk, block, rowGroup);
    }

    return rowGroup;
  }

  private void readChunkPages(Chunk chunk, BlockMetaData block, ColumnChunkPageReadStore rowGroup)
      throws IOException {
    if (null == fileDecryptor || fileDecryptor.plaintextFile()) {
      rowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
      return;
    }
    // Encrypted file
    ColumnPath columnPath = ColumnPath.get(chunk.descriptor.col.getPath());
    InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(columnPath);
    if (!columnDecryptionSetup.isEncrypted()) { // plaintext column
      rowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
    } else { // encrypted column
      rowGroup.addColumn(
          chunk.descriptor.col,
          chunk.readAllPages(
              columnDecryptionSetup.getMetaDataDecryptor(),
              columnDecryptionSetup.getDataDecryptor(),
              fileDecryptor.getFileAAD(),
              block.getOrdinal(),
              columnDecryptionSetup.getOrdinal()));
    }
  }

  public ColumnIndexStore getColumnIndexStore(int blockIndex) {
    ColumnIndexStore ciStore = blockIndexStores.get(blockIndex);
    if (ciStore == null) {
      ciStore = ColumnIndexStoreImpl.create(this, blocks.get(blockIndex), paths.keySet());
      blockIndexStores.set(blockIndex, ciStore);
    }
    return ciStore;
  }

  private RowRanges getRowRanges(int blockIndex) {
    assert FilterCompat.isFilteringRequired(options.getRecordFilter())
        : "Should not be invoked if filter is null or NOOP";
    RowRanges rowRanges = blockRowRanges.get(blockIndex);
    if (rowRanges == null) {
      rowRanges = ColumnIndexFilter.calculateRowRanges(
          options.getRecordFilter(),
          getColumnIndexStore(blockIndex),
          paths.keySet(),
          blocks.get(blockIndex).getRowCount());
      blockRowRanges.set(blockIndex, rowRanges);
    }
    return rowRanges;
  }

  public boolean skipNextRowGroup() {
    return advanceToNextBlock();
  }

  private boolean advanceToNextBlock() {
    if (currentBlock == blocks.size()) {
      return false;
    }

    // update the current block and instantiate a dictionary reader for it
    ++currentBlock;

    if (nextDictionaryReader != null) {
      nextDictionaryReader.close();
    }
    this.nextDictionaryReader = null;

    return true;
  }

  /**
   * Returns a {@link DictionaryPageReadStore} for the row group that would be
   * returned by calling {@link #readNextRowGroup()} or skipped by calling
   * {@link #skipNextRowGroup()}.
   *
   * @return a DictionaryPageReadStore for the next row group
   */
  public DictionaryPageReadStore getNextDictionaryReader() {
    if (nextDictionaryReader == null) {
      this.nextDictionaryReader = getDictionaryReader(currentBlock);
    }
    return nextDictionaryReader;
  }

  public DictionaryPageReader getDictionaryReader(int blockIndex) {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }
    return new DictionaryPageReader(this, blocks.get(blockIndex), options.getAllocator());
  }

  public DictionaryPageReader getDictionaryReader(BlockMetaData block) {
    return new DictionaryPageReader(this, block, options.getAllocator());
  }

  /**
   * Reads and decompresses a dictionary page for the given column chunk.
   * <p>
   * Returns null if the given column chunk has no dictionary page.
   *
   * @param meta a column's ColumnChunkMetaData to read the dictionary from
   * @return an uncompressed DictionaryPage or null
   * @throws IOException if there is an error while reading the dictionary
   */
  DictionaryPage readDictionary(ColumnChunkMetaData meta) throws IOException {
    if (!meta.hasDictionaryPage()) {
      return null;
    }

    // TODO: this should use getDictionaryPageOffset() but it isn't reliable.
    if (f.getPos() != meta.getStartingPos()) {
      f.seek(meta.getStartingPos());
    }

    boolean encryptedColumn = false;
    InternalColumnDecryptionSetup columnDecryptionSetup = null;
    byte[] dictionaryPageAAD = null;
    BlockCipher.Decryptor pageDecryptor = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      columnDecryptionSetup = fileDecryptor.getColumnSetup(meta.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        encryptedColumn = true;
      }
    }

    PageHeader pageHeader;
    if (!encryptedColumn) {
      pageHeader = Util.readPageHeader(f);
    } else {
      byte[] dictionaryPageHeaderAAD = AesCipher.createModuleAAD(
          fileDecryptor.getFileAAD(),
          ModuleType.DictionaryPageHeader,
          meta.getRowGroupOrdinal(),
          columnDecryptionSetup.getOrdinal(),
          -1);
      pageHeader = Util.readPageHeader(f, columnDecryptionSetup.getMetaDataDecryptor(), dictionaryPageHeaderAAD);
      dictionaryPageAAD = AesCipher.createModuleAAD(
          fileDecryptor.getFileAAD(),
          ModuleType.DictionaryPage,
          meta.getRowGroupOrdinal(),
          columnDecryptionSetup.getOrdinal(),
          -1);
      pageDecryptor = columnDecryptionSetup.getDataDecryptor();
    }

    if (!pageHeader.isSetDictionary_page_header()) {
      return null; // TODO: should this complain?
    }

    DictionaryPage compressedPage = readCompressedDictionary(pageHeader, f, pageDecryptor, dictionaryPageAAD);
    BytesInputDecompressor decompressor = options.getCodecFactory().getDecompressor(meta.getCodec());

    return new DictionaryPage(
        decompressor.decompress(compressedPage.getBytes(), compressedPage.getUncompressedSize()),
        compressedPage.getDictionarySize(),
        compressedPage.getEncoding());
  }

  private DictionaryPage readCompressedDictionary(
      PageHeader pageHeader,
      SeekableInputStream fin,
      BlockCipher.Decryptor pageDecryptor,
      byte[] dictionaryPageAAD)
      throws IOException {
    DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();

    int uncompressedPageSize = pageHeader.getUncompressed_page_size();
    int compressedPageSize = pageHeader.getCompressed_page_size();

    BytesInput bin = BytesInput.from(fin, compressedPageSize);

    if (null != pageDecryptor) {
      bin = BytesInput.from(pageDecryptor.decrypt(bin.toByteArray(), dictionaryPageAAD));
    }

    return new DictionaryPage(
        bin, uncompressedPageSize, dictHeader.getNum_values(), converter.getEncoding(dictHeader.getEncoding()));
  }

  public BloomFilterReader getBloomFilterDataReader(int blockIndex) {
    if (blockIndex < 0 || blockIndex >= blocks.size()) {
      return null;
    }
    return new BloomFilterReader(this, blocks.get(blockIndex));
  }

  public BloomFilterReader getBloomFilterDataReader(BlockMetaData block) {
    return new BloomFilterReader(this, block);
  }

  /**
   * Reads Bloom filter data for the given column chunk.
   *
   * @param meta a column's ColumnChunkMetaData to read the dictionary from
   * @return an BloomFilter object.
   * @throws IOException if there is an error while reading the Bloom filter.
   */
  public BloomFilter readBloomFilter(ColumnChunkMetaData meta) throws IOException {
    long bloomFilterOffset = meta.getBloomFilterOffset();
    if (bloomFilterOffset < 0) {
      return null;
    }

    // Prepare to decrypt Bloom filter (for encrypted columns)
    BlockCipher.Decryptor bloomFilterDecryptor = null;
    byte[] bloomFilterHeaderAAD = null;
    byte[] bloomFilterBitsetAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(meta.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        bloomFilterDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        bloomFilterHeaderAAD = AesCipher.createModuleAAD(
            fileDecryptor.getFileAAD(),
            ModuleType.BloomFilterHeader,
            meta.getRowGroupOrdinal(),
            columnDecryptionSetup.getOrdinal(),
            -1);
        bloomFilterBitsetAAD = AesCipher.createModuleAAD(
            fileDecryptor.getFileAAD(),
            ModuleType.BloomFilterBitset,
            meta.getRowGroupOrdinal(),
            columnDecryptionSetup.getOrdinal(),
            -1);
      }
    }

    // Seek to Bloom filter offset.
    f.seek(bloomFilterOffset);

    // Read Bloom filter length.
    int bloomFilterLength = meta.getBloomFilterLength();

    // If it is set, read Bloom filter header and bitset together.
    // Otherwise, read Bloom filter header first and then bitset.
    InputStream in = f;
    if (bloomFilterLength > 0) {
      byte[] headerAndBitSet = new byte[bloomFilterLength];
      f.readFully(headerAndBitSet);
      in = new ByteArrayInputStream(headerAndBitSet);
    }

    BloomFilterHeader bloomFilterHeader;
    try {
      bloomFilterHeader = Util.readBloomFilterHeader(in, bloomFilterDecryptor, bloomFilterHeaderAAD);
    } catch (IOException e) {
      LOG.warn("read no bloom filter");
      return null;
    }

    int numBytes = bloomFilterHeader.getNumBytes();
    if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
      LOG.warn("the read bloom filter size is wrong, size is {}", bloomFilterHeader.getNumBytes());
      return null;
    }

    if (!bloomFilterHeader.getHash().isSetXXHASH()
        || !bloomFilterHeader.getAlgorithm().isSetBLOCK()
        || !bloomFilterHeader.getCompression().isSetUNCOMPRESSED()) {
      LOG.warn(
          "the read bloom filter is not supported yet,  algorithm = {}, hash = {}, compression = {}",
          bloomFilterHeader.getAlgorithm(),
          bloomFilterHeader.getHash(),
          bloomFilterHeader.getCompression());
      return null;
    }

    byte[] bitset;
    if (null == bloomFilterDecryptor) {
      bitset = new byte[numBytes];
      in.read(bitset);
    } else {
      bitset = bloomFilterDecryptor.decrypt(in, bloomFilterBitsetAAD);
      if (bitset.length != numBytes) {
        throw new ParquetCryptoRuntimeException("Wrong length of decrypted bloom filter bitset");
      }
    }
    return new BlockSplitBloomFilter(bitset);
  }

  /**
   * Class should be considered private
   *
   * @param column the column chunk which the column index is to be returned for
   * @return the column index for the specified column chunk or {@code null} if there is no index
   * @throws IOException if any I/O error occurs during reading the file
   */
  public ColumnIndex readColumnIndex(ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getColumnIndexReference();
    if (ref == null) {
      return null;
    }
    f.seek(ref.getOffset());

    BlockCipher.Decryptor columnIndexDecryptor = null;
    byte[] columnIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        columnIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        columnIndexAAD = AesCipher.createModuleAAD(
            fileDecryptor.getFileAAD(),
            ModuleType.ColumnIndex,
            column.getRowGroupOrdinal(),
            columnDecryptionSetup.getOrdinal(),
            -1);
      }
    }
    return ParquetMetadataConverter.fromParquetColumnIndex(
        column.getPrimitiveType(), Util.readColumnIndex(f, columnIndexDecryptor, columnIndexAAD));
  }

  /**
   * Class should be considered private
   *
   * @param column the column chunk which the offset index is to be returned for
   * @return the offset index for the specified column chunk or {@code null} if there is no index
   * @throws IOException if any I/O error occurs during reading the file
   */
  public OffsetIndex readOffsetIndex(ColumnChunkMetaData column) throws IOException {
    IndexReference ref = column.getOffsetIndexReference();
    if (ref == null) {
      return null;
    }
    f.seek(ref.getOffset());

    BlockCipher.Decryptor offsetIndexDecryptor = null;
    byte[] offsetIndexAAD = null;
    if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
      InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(column.getPath());
      if (columnDecryptionSetup.isEncrypted()) {
        offsetIndexDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        offsetIndexAAD = AesCipher.createModuleAAD(
            fileDecryptor.getFileAAD(),
            ModuleType.OffsetIndex,
            column.getRowGroupOrdinal(),
            columnDecryptionSetup.getOrdinal(),
            -1);
      }
    }
    return ParquetMetadataConverter.fromParquetOffsetIndex(
        Util.readOffsetIndex(f, offsetIndexDecryptor, offsetIndexAAD));
  }

  @Override
  public void close() throws IOException {
    try {
      if (f != null) {
        f.close();
      }
      if (this.currentRowGroup != null) {
        this.currentRowGroup.close();
        this.currentRowGroup = null;
      }
      for (SeekableInputStream is : inputStreams) {
        is.close();
      }
      for (ByteBufferInputStream bufStr : bufferInputStreams) {
        bufStr.close();
      }
    } finally {
      AutoCloseables.uncheckedClose(nextDictionaryReader, crcAllocator);
      options.getCodecFactory().release();
    }
  }

  /*
   * Builder to concatenate the buffers of the discontinuous parts for the same column. These parts are generated as a
   * result of the column-index based filtering when some pages might be skipped at reading.
   */
  private class ChunkListBuilder {
    // ChunkData is backed by either a list of buffers or a list of streams
    // It's a mistake to have both lists populated
    private class ChunkData {
      // Used for synchronous reads
      final List<ByteBuffer> buffers = new ArrayList<>();
      // Used for asynchronous reads
      final List<ByteBufferInputStream> streams = new ArrayList<>();
      OffsetIndex offsetIndex;
    }

    private final Map<ChunkDescriptor, ChunkData> map = new HashMap<>();
    private ChunkDescriptor lastDescriptor;
    private final long rowCount;
    private SeekableInputStream f;
    private final ByteBufferReleaser releaser = new ByteBufferReleaser(options.getAllocator());

    public ChunkListBuilder(long rowCount) {
      this.rowCount = rowCount;
    }

    void add(ChunkDescriptor descriptor, ByteBufferInputStream stream, SeekableInputStream f) {
      map.computeIfAbsent(descriptor, d -> new ChunkData()).streams.add(stream);
      lastDescriptor = descriptor;
      this.f = f;
    }

    void add(ChunkDescriptor descriptor, List<ByteBuffer> buffers, SeekableInputStream f) {
      map.computeIfAbsent(descriptor, d -> new ChunkData()).buffers.addAll(buffers);
      lastDescriptor = descriptor;
      this.f = f;
    }

    void addBuffersToRelease(List<ByteBuffer> toRelease) {
      toRelease.forEach(releaser::releaseLater);
    }

    void setOffsetIndex(ChunkDescriptor descriptor, OffsetIndex offsetIndex) {
      map.computeIfAbsent(descriptor, d -> new ChunkData()).offsetIndex = offsetIndex;
    }

    List<Chunk> build() {
      Set<Entry<ChunkDescriptor, ChunkData>> entries = map.entrySet();
      List<Chunk> chunks = new ArrayList<>(entries.size());
      for (Entry<ChunkDescriptor, ChunkData> entry : entries) {
        ChunkDescriptor descriptor = entry.getKey();
        ChunkData data = entry.getValue();
        ByteBufferInputStream byteBufferInputStream;
        if (isAsyncIOReaderEnabled()) {
          // For async reads, we use a SequenceByeTeBufferInputStream instead of a ByteBufferInputStream
          // because calling sliceBuffers in the latter blocks until all the buffers are read (effectively
          // nullifying the async read) while the former blocks only if the next buffer is unavailable.
          byteBufferInputStream = new SequenceByteBufferInputStream(data.streams);
        } else {
          byteBufferInputStream = ByteBufferInputStream.wrap(data.buffers);
        }
        if (descriptor.equals(lastDescriptor)) {
          // because of a bug, the last chunk might be larger than descriptor.size
          chunks.add(
              new WorkaroundChunk(lastDescriptor, byteBufferInputStream, f, data.offsetIndex, rowCount));
        } else {
          chunks.add(new Chunk(descriptor, byteBufferInputStream, data.offsetIndex, rowCount));
        }
      }
      return chunks;
    }
  }

  /**
   * The data for a column chunk
   */
  class Chunk {

    protected final ChunkDescriptor descriptor;
    protected final ByteBufferInputStream stream;
    final OffsetIndex offsetIndex;
    final long rowCount;

    /**
     * @param descriptor descriptor for the chunk
     * @param stream the input stream to read from
     * @param offsetIndex the offset index for this column; might be null
     */
    public Chunk(ChunkDescriptor descriptor, ByteBufferInputStream stream, OffsetIndex offsetIndex, long rowCount) {
      this.descriptor = descriptor;
      this.stream = stream;
      this.offsetIndex = offsetIndex;
      this.rowCount = rowCount;
    }

    public ChunkDescriptor getDescriptor() {
      return descriptor;
    }

    protected PageHeader readPageHeader() throws IOException {
      return readPageHeader(null, null);
    }

    protected PageHeader readPageHeader(BlockCipher.Decryptor blockDecryptor, byte[] pageHeaderAAD)
        throws IOException {
      if (LOG.isDebugEnabled()) {
        String mode = (isAsyncIOReaderEnabled()) ? "ASYNC" : "SYNC";
        LOG.debug("{} READ HEADER: stream {}", mode, stream);
      }
      return Util.readPageHeader(stream, blockDecryptor, pageHeaderAAD);
    }

    /**
     * Calculate checksum of input bytes, throw decoding exception if it does not match the provided
     * reference crc
     */
    public void verifyCrc(int referenceCrc, BytesInput bytes, String exceptionMsg) {
      crc.reset();
      try (ByteBufferReleaser releaser = crcAllocator.getReleaser()) {
        crc.update(bytes.toByteBuffer(releaser));
      }
      if (crc.getValue() != ((long) referenceCrc & 0xffffffffL)) {
        throw new ParquetDecodingException(exceptionMsg);
      }
    }

    /**
     * Read all of the pages in a given column chunk.
     *
     * @return the list of pages
     */
    public ColumnChunkPageReader readAllPages() throws IOException {
      return readAllPages(null, null, null, -1, -1);
    }

    /*
     * If the async mode is enabled, this method will return immediately after reading the first page
     * and for subsequent pages the caller will block on the queue of pages contained in the
     * returned ColumnChunkPageReader object.
     * If the read is synchronous, this method will return only after the queue is filled and the
     * caller will not block on the queue.
     * If there is only one page, the behaviour will be identical.
     */
    public ColumnChunkPageReader readAllPages(
        BlockCipher.Decryptor headerBlockDecryptor,
        BlockCipher.Decryptor pageBlockDecryptor,
        byte[] aadPrefix,
        int rowGroupOrdinal,
        int columnOrdinal)
        throws IOException {
      BytesInputDecompressor decompressor =
          options.getCodecFactory().getDecompressor(descriptor.metadata.getCodec());
      DictionaryPage dictionaryPage;
      LinkedBlockingDeque<Optional<DataPage>> pagesInChunk;

      FilePageReader filePageReader = new FilePageReader(
          ParquetFileReader.this,
          this,
          currentBlock,
          headerBlockDecryptor,
          pageBlockDecryptor,
          aadPrefix,
          rowGroupOrdinal,
          columnOrdinal,
          decompressor);

      // Read the dictionary page;
      filePageReader.readOnePage();
      dictionaryPage = filePageReader.getDictionaryPage();
      // if reading columns in parallel, we simply submit the read page task to the thread pool
      // and continue (effectively to the next column).
      if (isParallelColumnReaderEnabled()) {
        filePageReader.readAllRemainingPagesAsync();
      } else {
        filePageReader.readAllRemainingPages();
      }
      pagesInChunk = filePageReader.getPagesInChunk();

      return new ColumnChunkPageReader(
          decompressor,
          pagesInChunk,
          dictionaryPage,
          offsetIndex,
          this.descriptor.metadata.getValueCount(),
          rowCount,
          pageBlockDecryptor,
          aadPrefix,
          rowGroupOrdinal,
          columnOrdinal,
          filePageReader,
          options);
    }

    int getPageOrdinal(int dataPageCountReadSoFar) {
      if (null == offsetIndex) {
        return dataPageCountReadSoFar;
      }

      return offsetIndex.getPageOrdinal(dataPageCountReadSoFar);
    }

    /**
     * @param size the size of the page
     * @return the page
     * @throws IOException if there is an error while reading from the file stream
     */
    public BytesInput readAsBytesInput(int size) throws IOException {
      if (LOG.isDebugEnabled()) {
        String mode = (isAsyncIOReaderEnabled()) ? "ASYNC" : "SYNC";
        LOG.debug("{} READ BYTES INPUT: stream {}", mode, stream);
      }
      return BytesInput.from(stream.sliceBuffers(size));
    }

    @Override
    public String toString() {
      return "Chunk{" + "descriptor=" + descriptor + ", stream=" + stream + ", offsetIndex=" + offsetIndex + '}';
    }
  }

  /**
   * deals with a now fixed bug where compressedLength was missing a few bytes.
   */
  private class WorkaroundChunk extends Chunk {

    private final SeekableInputStream f;

    /**
     * @param descriptor the descriptor of the chunk
     * @param f the file stream positioned at the end of this chunk
     */
    private WorkaroundChunk(
        ChunkDescriptor descriptor,
        ByteBufferInputStream stream,
        SeekableInputStream f,
        OffsetIndex offsetIndex,
        long rowCount) {
      super(descriptor, stream, offsetIndex, rowCount);
      this.f = f;
    }

    protected PageHeader readPageHeader() throws IOException {
      PageHeader pageHeader;
      stream.mark(8192); // headers should not be larger than 8k
      try {
        pageHeader = Util.readPageHeader(stream);
      } catch (IOException e) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        // if the last page is smaller than this, the page header itself is truncated in the buffer.
        stream.reset(); // resetting the buffer to the position before we got the error
        LOG.info("completing the column chunk to read the page header");
        pageHeader = Util.readPageHeader(
            new SequenceInputStream(stream, f)); // trying again from the buffer + remainder of the stream.
      }
      return pageHeader;
    }

    public BytesInput readAsBytesInput(int size) throws IOException {
      int available = stream.available();
      if (size > available) {
        // this is to workaround a bug where the compressedLength
        // of the chunk is missing the size of the header of the dictionary
        // to allow reading older files (using dictionary) we need this.
        // usually 13 to 19 bytes are missing
        int missingBytes = size - available;
        LOG.info("completed the column chunk with {} bytes", missingBytes);

        List<ByteBuffer> streamBuffers = stream.sliceBuffers(available);

        // do we make this async? If the async reader has completed reading its last
        // buffer, the file stream will be positioned at the right place and this will work just
        // fine. However this means that for some cases, the missing bytes of the last chunk will
        // be read synchronously irrespective of the async read mode.
        ByteBuffer lastBuffer = ByteBuffer.allocate(missingBytes);
        f.readFully(lastBuffer);

        List<ByteBuffer> buffers = new ArrayList<>(streamBuffers.size() + 1);
        buffers.addAll(streamBuffers);
        buffers.add(lastBuffer);

        return BytesInput.from(buffers);
      }

      return super.readAsBytesInput(size);
    }
  }

  /**
   * Information needed to read a column chunk or a part of it.
   */
  public static class ChunkDescriptor {

    private final ColumnDescriptor col;
    private final ColumnChunkMetaData metadata;
    private final long fileOffset;
    private final long size;

    /**
     * @param col column this chunk is part of
     * @param metadata metadata for the column
     * @param fileOffset offset in the file where this chunk starts
     * @param size size of the chunk
     */
    private ChunkDescriptor(ColumnDescriptor col, ColumnChunkMetaData metadata, long fileOffset, long size) {
      super();
      this.col = col;
      this.metadata = metadata;
      this.fileOffset = fileOffset;
      this.size = size;
    }

    public ColumnDescriptor getCol() {
      return col;
    }

    public ColumnChunkMetaData getMetadata() {
      return metadata;
    }

    public long getFileOffset() {
      return fileOffset;
    }

    @Override
    public int hashCode() {
      return col.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj instanceof ChunkDescriptor) {
        return col.equals(((ChunkDescriptor) obj).col);
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return "ChunkDescriptor{" + "col="
          + col + ", metadata="
          + metadata + ", fileOffset="
          + fileOffset + ", size="
          + size + '}';
    }
  }

  /**
   * Describes a list of consecutive parts to be read at once. A consecutive part may contain whole column chunks or
   * only parts of them (some pages).
   */
  private class ConsecutivePartList {

    private final long offset;
    private long length;
    private final List<ChunkDescriptor> chunks = new ArrayList<>();

    /**
     * @param offset where the first chunk starts
     */
    ConsecutivePartList(long offset) {
      this.offset = offset;
    }

    /**
     * adds a chunk to the list.
     * It must be consecutive to the previous chunk
     *
     * @param descriptor a chunk descriptor
     */
    public void addChunk(ChunkDescriptor descriptor) {
      chunks.add(descriptor);
      length += descriptor.size;
    }

    /**
     * @param builder used to build chunk list to read the pages for the different columns
     * @throws IOException if there is an error while reading from the stream
     */
    public void readAll(SeekableInputStream is, ChunkListBuilder builder) throws IOException {
      // Use a new file input stream for every set of consecutive chunks.
      // If we are reading synchronously, we don't want the input stream to seek back
      // forth in the file for every set of chunks. Seeking backwards can cause a performance drop.
      // If we read in async mode many such sets could be read in parallel, and we don't want the
      // same input stream shared between threads.
      is.seek(offset);

      int fullAllocations = Math.toIntExact(length / options.getMaxAllocationSize());
      int lastAllocationSize = Math.toIntExact(length % options.getMaxAllocationSize());

      int numAllocations = fullAllocations + (lastAllocationSize > 0 ? 1 : 0);
      List<ByteBuffer> buffers = new ArrayList<>(numAllocations);

      for (int i = 0; i < fullAllocations; i += 1) {
        buffers.add(options.getAllocator().allocate(options.getMaxAllocationSize()));
      }

      if (lastAllocationSize > 0) {
        buffers.add(options.getAllocator().allocate(lastAllocationSize));
      }
      builder.addBuffersToRelease(buffers);

      long readStart = System.nanoTime();

      ByteBufferInputStream stream;
      if (!isAsyncIOReaderEnabled()) {
        // pre-read the files into the allocated buffers
        for (ByteBuffer buffer : buffers) {
          is.readFully(buffer);
          buffer.flip();
        }
        long timeSpent = System.nanoTime() - readStart;
        LOG.debug("SYNC Stream: READ - {}", timeSpent / 1000.0);
        stream = ByteBufferInputStream.wrap(buffers);
      } else {
        // The underlying implementation will read the data from the input stream
        // asynchronously.
        stream = ByteBufferInputStream.wrapAsync(options.getIOThreadPool(), is, buffers);
      }
      setReadMetrics(readStart, length);

      bufferInputStreams.add(stream);
      // report in a counter the data we just scanned
      BenchmarkCounter.incrementBytesRead(length);
      for (final ChunkDescriptor descriptor : chunks) {
        if (!isAsyncIOReaderEnabled()) {
          // stream.sliceBuffers is a *blocking* call and assumes that all data has been read
          builder.add(descriptor, stream.sliceBuffers(descriptor.size), is);
          LOG.debug("SYNC: Added to builder -  chunk slice  {} ", descriptor);
        } else {
          // Preconditions:
          //  1) The stream is an Async stream.
          //  2) Each column chunk has an independent stream.
          builder.add(descriptor, stream, is);
          LOG.debug("ASYNC: Added to builder -  chunk slice  {} , stream {}", descriptor, stream);
        }
      }
    }

    private void setReadMetrics(long startNs, long len) {
      ParquetMetricsCallback metricsCallback = options.getMetricsCallback();
      if (metricsCallback != null) {
        long totalFileReadTimeNs = Math.max(System.nanoTime() - startNs, 0);
        double sizeInMb = ((double) len) / (1024 * 1024);
        double timeInSec = ((double) totalFileReadTimeNs) / 1000_0000_0000L;
        double throughput = sizeInMb / timeInSec;
        LOG.debug(
            "Parquet: File Read stats:  Length: {} MB, Time: {} secs, throughput: {} MB/sec ",
            sizeInMb,
            timeInSec,
            throughput);
        metricsCallback.setDuration(ParquetFileReaderMetrics.ReadTime.name(), totalFileReadTimeNs);
        metricsCallback.setValueLong(ParquetFileReaderMetrics.ReadSize.name(), length);
        metricsCallback.setValueDouble(ParquetFileReaderMetrics.ReadThroughput.name(), throughput);
      }
    }

    /**
     * Populate data in a parquet file range from a vectored range; will block for up
     * to {@link #HADOOP_VECTORED_READ_TIMEOUT_SECONDS} seconds.
     * @param currRange range to populated.
     * @param builder used to build chunk list to read the pages for the different columns.
     * @throws IOException if there is an error while reading from the stream, including a timeout.
     */
    public void readFromVectoredRange(ParquetFileRange currRange, ChunkListBuilder builder) throws IOException {
      ByteBuffer buffer;
      final long timeoutSeconds = HADOOP_VECTORED_READ_TIMEOUT_SECONDS;
      long readStart = System.nanoTime();
      try {
        LOG.debug(
            "Waiting for vectored read to finish for range {} with timeout {} seconds",
            currRange,
            timeoutSeconds);
        buffer = FutureIO.awaitFuture(currRange.getDataReadFuture(), timeoutSeconds, TimeUnit.SECONDS);
        setReadMetrics(readStart, currRange.getLength());
        // report in a counter the data we just scanned
        BenchmarkCounter.incrementBytesRead(currRange.getLength());
      } catch (TimeoutException e) {
        String error = String.format(
            "Timeout while fetching result for %s with time limit %d seconds", currRange, timeoutSeconds);
        LOG.error(error, e);
        throw new IOException(error, e);
      }
      ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffer);
      for (ChunkDescriptor descriptor : chunks) {
        builder.add(descriptor, stream.sliceBuffers(descriptor.size), f);
      }
    }

    /**
     * @return the position following the last byte of these chunks
     */
    public long endPos() {
      return offset + length;
    }

    @Override
    public String toString() {
      return "ConsecutivePartList{" + "offset=" + offset + ", length=" + length + ", chunks=" + chunks + '}';
    }
  }
}
