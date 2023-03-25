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
package org.apache.parquet.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.rewrite.MaskMode;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription = "Rewrite one or more Parquet files to a new Parquet file")
public class RewriteCommand extends BaseCommand {

  @Parameter(
          names = {"-i", "--input"},
          description = "<comma-separated text of input parquet file paths>",
          required = true)
  List<String> inputs;
  @Parameter(
          names = {"-o", "--output"},
          description = "<output parquet file path>",
          required = true)
  String output;
  @Parameter(
          names = {"--mask-mode"},
          description = "<mask mode: nullify>",
          required = false)
  String maskMode;
  @Parameter(
          names = {"--mask-columns"},
          description = "<columns to be replaced with masked value>",
          required = false)
  List<String> maskColumns;

  @Parameter(
          names = {"--prune-columns"},
          description = "<columns to be replaced with masked value>",
          required = false)
  List<String> pruneColumns;

  @Parameter(
          names = {"-c", "--compression-codec"},
          description = "<new compression codec>",
          required = false)
  String codec;

  public RewriteCommand(Logger console) {
    super(console);
  }

  private RewriteOptions buildOptionsOrFail() {
    Preconditions.checkArgument(inputs != null && !inputs.isEmpty() && output != null,
            "Both input and output parquet file paths are required.");

    List<Path> inputPaths = new ArrayList<>();
    for (String input : inputs) {
      inputPaths.add(new Path(input));
    }
    Path outputPath = new Path(output);

    // The builder below takes the job to validate all input parameters.
    RewriteOptions.Builder builder = new RewriteOptions.Builder(getConf(), inputPaths, outputPath);

    // Mask columns if specified.
    if (maskMode != null && maskMode.equals("nullify") && maskColumns != null && !maskColumns.isEmpty()) {
      Map<String, MaskMode> maskModeMap = new HashMap<>();
      for (String maskColumn : maskColumns) {
        maskModeMap.put(maskColumn, MaskMode.NULLIFY);
      }
      builder.mask(maskModeMap);
    }

    // Prune columns if specified.
    if (pruneColumns != null && !pruneColumns.isEmpty()) {
      builder.prune(pruneColumns);
    }

    if (codec != null) {
      CompressionCodecName codecName = CompressionCodecName.valueOf(codec);
      builder.transform(codecName);
    }

    return builder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    RewriteOptions options = buildOptionsOrFail();
    ParquetRewriter rewriter = new ParquetRewriter(options);
    rewriter.processBlocks();
    rewriter.close();
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
            "# Rewrite one or more Parquet files to a new Parquet file",
            "-i input.parquet -o output.parquet --mask-mode nullify --mask-columns col1_name"
    );
  }
}
