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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class ColumnPruner {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnPruner.class);

  public void pruneColumns(Configuration conf, Path inputFile, Path outputFile, List<String> cols)
      throws IOException {
    RewriteOptions options = new RewriteOptions.Builder(conf, inputFile, outputFile)
        .prune(cols)
        .build();
    ParquetRewriter rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();
  }
}
