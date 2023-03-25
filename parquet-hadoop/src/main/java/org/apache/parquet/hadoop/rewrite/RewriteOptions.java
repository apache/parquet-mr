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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A set of options to create a ParquetRewriter.
 */
public class RewriteOptions {

  final Configuration conf;
  final List<Path> inputFiles;
  final Path outputFile;
  final List<String> pruneColumns;
  final CompressionCodecName newCodecName;
  final Map<String, MaskMode> maskColumns;
  final List<String> encryptColumns;
  final FileEncryptionProperties fileEncryptionProperties;

  private RewriteOptions(Configuration conf,
                         List<Path> inputFiles,
                         Path outputFile,
                         List<String> pruneColumns,
                         CompressionCodecName newCodecName,
                         Map<String, MaskMode> maskColumns,
                         List<String> encryptColumns,
                         FileEncryptionProperties fileEncryptionProperties) {
    this.conf = conf;
    this.inputFiles = inputFiles;
    this.outputFile = outputFile;
    this.pruneColumns = pruneColumns;
    this.newCodecName = newCodecName;
    this.maskColumns = maskColumns;
    this.encryptColumns = encryptColumns;
    this.fileEncryptionProperties = fileEncryptionProperties;
  }

  public Configuration getConf() {
    return conf;
  }

  public List<Path> getInputFiles() {
    return inputFiles;
  }

  public Path getOutputFile() {
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

  // Builder to create a RewriterOptions.
  public static class Builder {
    private Configuration conf;
    private List<Path> inputFiles;
    private Path outputFile;
    private List<String> pruneColumns;
    private CompressionCodecName newCodecName;
    private Map<String, MaskMode> maskColumns;
    private List<String> encryptColumns;
    private FileEncryptionProperties fileEncryptionProperties;

    /**
     * Create a builder to create a RewriterOptions.
     *
     * @param conf       configuration for reading from input files and writing to output file
     * @param inputFile  input file path to read from
     * @param outputFile output file path to rewrite to
     */
    public Builder(Configuration conf, Path inputFile, Path outputFile) {
      this.conf = conf;
      this.inputFiles = Arrays.asList(inputFile);
      this.outputFile = outputFile;
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
      this.conf = conf;
      this.inputFiles = inputFiles;
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
      this.inputFiles.add(path);
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

      if (pruneColumns != null) {
        if (maskColumns != null) {
          for (String pruneColumn : pruneColumns) {
            Preconditions.checkArgument(!maskColumns.containsKey(pruneColumn),
                    "Cannot prune and mask same column");
          }
        }

        if (encryptColumns != null) {
          for (String pruneColumn : pruneColumns) {
            Preconditions.checkArgument(!encryptColumns.contains(pruneColumn),
                    "Cannot prune and encrypt same column");
          }
        }
      }

      if (encryptColumns != null && !encryptColumns.isEmpty()) {
        Preconditions.checkArgument(fileEncryptionProperties != null,
                "FileEncryptionProperties is required when encrypting columns");
      }

      if (fileEncryptionProperties != null) {
        Preconditions.checkArgument(encryptColumns != null && !encryptColumns.isEmpty(),
                "Encrypt columns is required when FileEncryptionProperties is set");
      }

      return new RewriteOptions(conf,
              inputFiles,
              outputFile,
              pruneColumns,
              newCodecName,
              maskColumns,
              encryptColumns,
              fileEncryptionProperties);
    }
  }

}
