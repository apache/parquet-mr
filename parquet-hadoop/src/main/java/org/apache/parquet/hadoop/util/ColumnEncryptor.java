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
package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is for fast rewriting existing file with column encryption
 *
 * For columns to be encrypted, all the pages of those columns are read, but decompression/decoding,
 * it is encrypted immediately and write back.
 *
 * For columns not to be encrypted, the whole column chunk will be appended directly to writer.
 */
@Deprecated
public class ColumnEncryptor {

  private Configuration conf;

  public ColumnEncryptor(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Given the input file, encrypt the columns specified by paths, and output the file.
   * The encryption settings can be specified in the parameter of fileEncryptionProperties
   * @param inputFile Input file
   * @param outputFile Output file
   * @param paths columns to be encrypted
   * @param fileEncryptionProperties FileEncryptionProperties of the file
   * @throws IOException
   */
  public void encryptColumns(String inputFile, String outputFile, List<String> paths, FileEncryptionProperties fileEncryptionProperties) throws IOException {
    Path inPath = new Path(inputFile);
    Path outPath = new Path(outputFile);
    RewriteOptions options = new RewriteOptions.Builder(conf, inPath, outPath).
            encrypt(paths).encryptionProperties(fileEncryptionProperties).build();
    ParquetRewriter rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();
  }

  public byte[] readBlock(int length, TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return data;
  }

  public BytesInput readBlockAllocate(int length, TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  public static Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }
}
