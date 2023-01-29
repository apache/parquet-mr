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
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ColumnMasker;
import org.apache.parquet.hadoop.util.ColumnMasker.MaskMode;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

@Parameters(commandDescription="Replace columns with masked values and write to a new Parquet file")
public class ColumnMaskingCommand extends BaseCommand {

  private ColumnMasker masker;

  public ColumnMaskingCommand(Logger console) {
    super(console);
    masker = new ColumnMasker();
  }

  @Parameter(
    names = {"-m", "--mode"},
    description = "<mask mode: nullify>",
    required = true)
  String mode;

  @Parameter(
    names = {"-i", "--input"},
    description = "<input parquet file path>",
    required = true)
  String input;

  @Parameter(
    names = {"-o", "--output"},
    description = "<output parquet file path>",
    required = true)
  String output;

  @Parameter(
    names = {"-c", "--columns"},
    description = "<columns to be replaced with masked value>",
    required = true)
  List<String> cols;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(mode != null && (mode.equals("nullify")),
      "mask mode cannot be null and can be only nullify");

    Preconditions.checkArgument(input != null && output != null,
      "Both input and output parquet file paths are required.");

    Preconditions.checkArgument(cols != null && cols.size() > 0,
      "columns cannot be null or empty");

    MaskMode maskMode = MaskMode.fromString(mode);
    Path inPath = new Path(input);
    Path outPath = new Path(output);

    ParquetMetadata metaData = ParquetFileReader.readFooter(getConf(), inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    ParquetFileWriter writer = new ParquetFileWriter(getConf(), schema, outPath, ParquetFileWriter.Mode.CREATE);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(HadoopInputFile.fromPath(inPath, getConf()), HadoopReadOptions.builder(getConf()).build())) {
      masker.processBlocks(reader, writer, metaData, schema, cols, maskMode);
    } finally {
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Replace columns with masked values and write to a new Parquet file",
        "-m nullify -i input.parquet -o output.parquet -c col1_name"
    );
  }
}
