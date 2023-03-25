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

import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Deprecated
public class ColumnMasker {
  /**
   *
   * @param reader Reader of source file
   * @param writer Writer of destination file
   * @param meta Metadata of source file
   * @param schema Schema of source file
   * @param paths Column Paths need to be masked
   * @param maskMode Mode to mask
   * @throws IOException
   */
  public void processBlocks(TransParquetFileReader reader, ParquetFileWriter writer, ParquetMetadata meta,
                            MessageType schema, List<String> paths, MaskMode maskMode) throws IOException {
    ParquetRewriter rewriter = new ParquetRewriter(
            reader, writer, meta, schema, null, null,  paths, convertMaskMode(maskMode));
    rewriter.processBlocks();
  }

  org.apache.parquet.hadoop.rewrite.MaskMode convertMaskMode(MaskMode maskMode) {
    switch (maskMode) {
      case NULLIFY:
        return org.apache.parquet.hadoop.rewrite.MaskMode.NULLIFY;
      case HASH:
        return org.apache.parquet.hadoop.rewrite.MaskMode.HASH;
      case REDACT:
        return org.apache.parquet.hadoop.rewrite.MaskMode.REDACT;
      default:
        return null;
    }
  }

  public static Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }

  public enum MaskMode {
    NULLIFY("nullify"),
    HASH("hash"),
    REDACT("redact");

    private String mode;

    MaskMode(String text) {
      this.mode = text;
    }

    public String getMode() {
      return this.mode;
    }

    public static MaskMode fromString(String mode) {
      for (MaskMode b : MaskMode.values()) {
        if (b.mode.equalsIgnoreCase(mode)) {
          return b;
        }
      }
      return null;
    }
  }

}
