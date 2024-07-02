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
package org.apache.parquet.hadoop.rewrite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.IndexCache;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

/**
 * A set of options to create a ParquetRewriter.
 *
 * TODO find a place where to put a proper description of functionality as it is not trivial:
 * ParquetRewriter allows to stitch files with the same schema into a single file.
 * Note that ParquetRewriter also can be used for effectively stitch/joining multiple parquet files with
 * different schemas.
 * You can provide the main input file group and multiple right side ones. That is possible when:
 * 1) the number of rows in the main and extra input groups are the same,
 * 2) the ordering of rows in the main and extra input groups is the same.
 */
public class RewriteOptions {

  private final ParquetConfiguration conf;
  private final List<InputFile> inputFiles;
  private final List<InputFile> inputFilesToJoinColumns;
  private final OutputFile outputFile;
  private final List<String> pruneColumns;
  private final CompressionCodecName newCodecName;
  private final Map<String, MaskMode> maskColumns;
  private final List<String> encryptColumns;
  private final FileEncryptionProperties fileEncryptionProperties;
  private final IndexCache.CacheStrategy indexCacheStrategy;
  private final boolean joinColumnsOverwrite;

  private RewriteOptions(
      ParquetConfiguration conf,
      List<InputFile> inputFiles,
      List<InputFile> inputFilesToJoinColumns,
      OutputFile outputFile,
      List<String> pruneColumns,
      CompressionCodecName newCodecName,
      Map<String, MaskMode> maskColumns,
      List<String> encryptColumns,
      FileEncryptionProperties fileEncryptionProperties,
      IndexCache.CacheStrategy indexCacheStrategy,
      boolean joinColumnsOverwrite) {
    this.conf = conf;
    this.inputFiles = inputFiles;
    this.inputFilesToJoinColumns = inputFilesToJoinColumns;
    this.outputFile = outputFile;
    this.pruneColumns = pruneColumns;
    this.newCodecName = newCodecName;
    this.maskColumns = maskColumns;
    this.encryptColumns = encryptColumns;
    this.fileEncryptionProperties = fileEncryptionProperties;
    this.indexCacheStrategy = indexCacheStrategy;
    this.joinColumnsOverwrite = joinColumnsOverwrite;
  }

  /**
   * Gets the {@link Configuration} part of the rewrite options.
   *
   * @return the associated {@link Configuration}
   */
  public Configuration getConf() {
    return ConfigurationUtil.createHadoopConfiguration(conf);
  }

  /**
   * Gets the {@link ParquetConfiguration} part of the rewrite options.
   *
   * @return the associated {@link ParquetConfiguration}
   */
  public ParquetConfiguration getParquetConfiguration() {
    return conf;
  }

  /**
   * Gets the input {@link Path}s for the rewrite if they exist for all input files,
   * otherwise throws a {@link RuntimeException}.
   *
   * @return a {@link List} of the associated input {@link Path}s
   */
  public List<Path> getInputFiles() {
    return inputFiles.stream()
        .map(f -> {
          if (f instanceof HadoopOutputFile) {
            HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) f;
            return new Path(hadoopOutputFile.getPath());
          } else {
            throw new RuntimeException("The input files do not all have an associated Hadoop Path.");
          }
        })
        .collect(Collectors.toList());
  }

  /**
   * Gets the {@link InputFile}s for the rewrite.
   *
   * @return a {@link List} of the associated {@link InputFile}s
   */
  public List<InputFile> getParquetInputFiles() {
    return inputFiles;
  }

  /** TODO fix documentation after addition of InputFilesToJoinColumns
   * Gets the right {@link InputFile}s for the rewrite.
   *
   * @return a {@link List} of the associated right {@link InputFile}s
   */
  public List<InputFile> getParquetInputFilesToJoinColumns() {
    return inputFilesToJoinColumns;
  }

  /**
   * Get the {@link Path} for the rewrite if it exists, otherwise throws a {@link RuntimeException}.
   *
   * @return the associated {@link Path} if it exists
   */
  public Path getOutputFile() {
    if (outputFile instanceof HadoopOutputFile) {
      HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) outputFile;
      return new Path(hadoopOutputFile.getPath());
    } else {
      throw new RuntimeException("The output file does not have an associated Hadoop Path.");
    }
  }

  /**
   * Get the {@link OutputFile} for the rewrite.
   *
   * @return the associated {@link OutputFile}
   */
  public OutputFile getParquetOutputFile() {
    return outputFile;
  }

  public List<String> getPruneColumns() {
    return pruneColumns;
  }

  public CompressionCodecName getNewCodecName() {
    return newCodecName;
  }

  public Map<String, MaskMode> getMaskColumns() {
    return maskColumns;
  }

  public List<String> getEncryptColumns() {
    return encryptColumns;
  }

  public FileEncryptionProperties getFileEncryptionProperties() {
    return fileEncryptionProperties;
  }

  public IndexCache.CacheStrategy getIndexCacheStrategy() {
    return indexCacheStrategy;
  }

  public boolean getJoinColumnsOverwrite() {
    return joinColumnsOverwrite;
  }

  // Builder to create a RewriterOptions.
  public static class Builder {
    private final ParquetConfiguration conf;
    private final List<InputFile> inputFiles;
    private final List<InputFile> inputFilesToJoinColumns;
    private final OutputFile outputFile;
    private List<String> pruneColumns;
    private CompressionCodecName newCodecName;
    private Map<String, MaskMode> maskColumns;
    private List<String> encryptColumns;
    private FileEncryptionProperties fileEncryptionProperties;
    private IndexCache.CacheStrategy indexCacheStrategy = IndexCache.CacheStrategy.NONE;
    private boolean joinColumnsOverwrite = false;

    /**
     * Create a builder to create a RewriterOptions.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFile  input file path to read from
     * @param outputFile output file path to rewrite to
     */
    public Builder(Configuration conf, Path inputFile, Path outputFile) {
      this(
          new HadoopParquetConfiguration(conf),
          HadoopInputFile.fromPathUnchecked(inputFile, conf),
          HadoopOutputFile.fromPathUnchecked(outputFile, conf));
    }

    /**
     * Create a builder to create a RewriterOptions.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFile  input file to read from
     * @param outputFile output file to rewrite to
     */
    public Builder(ParquetConfiguration conf, InputFile inputFile, OutputFile outputFile) {
      this(conf, Collections.singletonList(inputFile), outputFile);
    }

    /**
     * Create a builder to create a RewriterOptions.
     * <p>
     * Please note that if merging more than one file, the schema of all files must be the same.
     * Otherwise, the rewrite will fail.
     * <p>
     * The rewrite will keep original row groups from all input files. This may not be optimal
     * if row groups are very small and will not solve small file problems. Instead, it will
     * make it worse to have a large file footer in the output file.
     * TODO: support rewrite by record to break the original row groups into reasonable ones.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFiles list of input file paths to read from
     * @param outputFile output file path to rewrite to
     */
    public Builder(Configuration conf, List<Path> inputFiles, Path outputFile) {
      this.conf = new HadoopParquetConfiguration(conf);
      this.inputFiles = new ArrayList<>(inputFiles.size());
      for (Path inputFile : inputFiles) {
        this.inputFiles.add(HadoopInputFile.fromPathUnchecked(inputFile, conf));
      }
      this.inputFilesToJoinColumns = new ArrayList<>();
      this.outputFile = HadoopOutputFile.fromPathUnchecked(outputFile, conf);
    }

    /**
     * Create a builder to create a RewriterOptions.
     * <p>
     * Please note that if merging more than one file, the schema of all files must be the same.
     * Otherwise, the rewrite will fail.
     * <p>
     * The rewrite will keep original row groups from all input files. This may not be optimal
     * if row groups are very small and will not solve small file problems. Instead, it will
     * make it worse to have a large file footer in the output file.
     * TODO: support rewrite by record to break the original row groups into reasonable ones.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFiles list of input file paths to read from
     * @param outputFile output file path to rewrite to
     */
    public Builder(ParquetConfiguration conf, List<InputFile> inputFiles, OutputFile outputFile) {
      this.conf = conf;
      this.inputFiles = inputFiles;
      this.inputFilesToJoinColumns = new ArrayList<>();
      this.outputFile = outputFile;
    }

    public Builder(Configuration conf, List<Path> inputFiles, List<Path> inputFilesToJoinColumns, Path outputFile) {
      this.conf = new HadoopParquetConfiguration(conf);
      this.inputFiles = new ArrayList<>(inputFiles.size());
      for (Path inputFile : inputFiles) {
        this.inputFiles.add(HadoopInputFile.fromPathUnchecked(inputFile, conf));
      }
      this.inputFilesToJoinColumns = new ArrayList<>(inputFilesToJoinColumns.size());
      for (Path inputFile : inputFilesToJoinColumns) {
        this.inputFilesToJoinColumns.add(HadoopInputFile.fromPathUnchecked(inputFile, conf));
      }
      this.outputFile = HadoopOutputFile.fromPathUnchecked(outputFile, conf);
    }

    public Builder(
        ParquetConfiguration conf,
        List<InputFile> inputFiles,
        List<InputFile> inputFilesToJoinColumns,
        OutputFile outputFile) {
      this.conf = conf;
      this.inputFiles = inputFiles;
      this.inputFilesToJoinColumns = inputFilesToJoinColumns;
      this.outputFile = outputFile;
    }

    /**
     * Set the columns to prune.
     * <p>
     * By default, all columns are kept.
     *
     * @param columns list of columns to prune
     * @return self
     */
    public Builder prune(List<String> columns) {
      this.pruneColumns = columns;
      return this;
    }

    /**
     * Set the compression codec to use for the output file.
     * <p>
     * By default, the codec is the same as the input file.
     *
     * @param newCodecName compression codec to use
     * @return self
     */
    public Builder transform(CompressionCodecName newCodecName) {
      this.newCodecName = newCodecName;
      return this;
    }

    /**
     * Set the columns to mask.
     * <p>
     * By default, no columns are masked.
     *
     * @param maskColumns map of columns to mask to the masking mode
     * @return self
     */
    public Builder mask(Map<String, MaskMode> maskColumns) {
      this.maskColumns = maskColumns;
      return this;
    }

    /**
     * Set the columns to encrypt.
     * <p>
     * By default, no columns are encrypted.
     *
     * @param encryptColumns list of columns to encrypt
     * @return self
     */
    public Builder encrypt(List<String> encryptColumns) {
      this.encryptColumns = encryptColumns;
      return this;
    }

    /**
     * Set the encryption properties to use for the output file.
     * <p>
     * This is required if encrypting columns are not empty.
     *
     * @param fileEncryptionProperties encryption properties to use
     * @return self
     */
    public Builder encryptionProperties(FileEncryptionProperties fileEncryptionProperties) {
      this.fileEncryptionProperties = fileEncryptionProperties;
      return this;
    }

    /**
     * Add an input file to read from.
     *
     * @param path input file path to read from
     * @return self
     */
    public Builder addInputFile(Path path) {
      this.inputFiles.add(
          HadoopInputFile.fromPathUnchecked(path, ConfigurationUtil.createHadoopConfiguration(conf)));
      return this;
    }

    /** TODO fix documentation after addition of InputFilesToJoinColumns
     * Add an input file to read from.
     *
     * @param path input file path to read from
     * @return self
     */
    public Builder addInputFileToJoinColumns(Path path) {
      this.inputFilesToJoinColumns.add(
          HadoopInputFile.fromPathUnchecked(path, ConfigurationUtil.createHadoopConfiguration(conf)));
      return this;
    }

    /**
     * Add an input file to read from.
     *
     * @param inputFile input file to read from
     * @return self
     */
    public Builder addInputFile(InputFile inputFile) {
      this.inputFiles.add(inputFile);
      return this;
    }

    /** TODO fix documentation after addition of InputFilesToJoinColumns
     * Add an input file to read from.
     *
     * @param inputFile input file to read from
     * @return self
     */
    public Builder addInputFilesToJoinColumns(InputFile inputFile) {
      this.inputFilesToJoinColumns.add(inputFile);
      return this;
    }

    /**
     * Set the index(ColumnIndex, Offset and BloomFilter) cache strategy.
     * <p>
     * This could reduce the random seek while rewriting with PREFETCH_BLOCK strategy, NONE by default.
     *
     * @param cacheStrategy the index cache strategy, supports: {@link IndexCache.CacheStrategy#NONE} or
     *                      {@link IndexCache.CacheStrategy#PREFETCH_BLOCK}
     * @return self
     */
    public Builder indexCacheStrategy(IndexCache.CacheStrategy cacheStrategy) {
      this.indexCacheStrategy = cacheStrategy;
      return this;
    }

    /** TODO fix documentation after addition of InputFilesToJoinColumns
     */
    public Builder joinColumnsOverwrite(boolean joinColumnsOverwrite) {
      this.joinColumnsOverwrite = joinColumnsOverwrite;
      return this;
    }

    /**
     * Build the RewriterOptions.
     *
     * @return a RewriterOptions
     */
    public RewriteOptions build() {
      Preconditions.checkArgument(inputFiles != null && !inputFiles.isEmpty(), "Input file is required");
      Preconditions.checkArgument(outputFile != null, "Output file is required");
      Preconditions.checkArgument(
          inputFiles != null && !inputFiles.isEmpty(),
          "Right side Input files can't be empty, if you don't need a join functionality then use other builders");

      if (pruneColumns != null) {
        if (maskColumns != null) {
          for (String pruneColumn : pruneColumns) {
            Preconditions.checkArgument(
                !maskColumns.containsKey(pruneColumn), "Cannot prune and mask same column");
          }
        }

        if (encryptColumns != null) {
          for (String pruneColumn : pruneColumns) {
            Preconditions.checkArgument(
                !encryptColumns.contains(pruneColumn), "Cannot prune and encrypt same column");
          }
        }
      }

      if (encryptColumns != null && !encryptColumns.isEmpty()) {
        Preconditions.checkArgument(
            fileEncryptionProperties != null,
            "FileEncryptionProperties is required when encrypting columns");
      }

      if (fileEncryptionProperties != null) {
        Preconditions.checkArgument(
            encryptColumns != null && !encryptColumns.isEmpty(),
            "Encrypt columns is required when FileEncryptionProperties is set");
      }

      return new RewriteOptions(
          conf,
          inputFiles,
          inputFilesToJoinColumns,
          outputFile,
          pruneColumns,
          newCodecName,
          maskColumns,
          encryptColumns,
          fileEncryptionProperties,
          indexCacheStrategy,
          joinColumnsOverwrite);
    }
  }
}
