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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

@Parameters(commandDescription="(Deprecated: will be removed in 2.0.0, use rewrite command instead) " +
        "Translate the compression from one to another (It doesn't support bloom filter feature yet).")
public class TransCompressionCommand extends BaseCommand {

  private CompressionConverter compressionConverter;

  public TransCompressionCommand(Logger console) {
    super(console);
    compressionConverter = new CompressionConverter();
  }

  @Parameter(description = "<input parquet file path>")
  String input;

  @Parameter(
    names={"-o", "--output"},
    description = "<output parquet file path>")
  String output;

  @Parameter(
    names = {"-c", "--compression-codec"},
    description = "<new compression codec>")
  String codec;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(input != null && output != null,
      "Both input and output parquet file paths are required.");

    Preconditions.checkArgument(codec != null,
      "The codec cannot be null");

    Path inPath = new Path(input);
    Path outPath = new Path(output);
    CompressionCodecName codecName = CompressionCodecName.valueOf(codec);

    ParquetMetadata metaData = ParquetFileReader.readFooter(getConf(), inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    ParquetFileWriter writer = new ParquetFileWriter(getConf(), schema, outPath, ParquetFileWriter.Mode.CREATE);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(HadoopInputFile.fromPath(inPath, getConf()), HadoopReadOptions.builder(getConf()).build())) {
      compressionConverter.processBlocks(reader, writer, metaData, schema, metaData.getFileMetaData().getCreatedBy(), codecName);
    } finally {
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Translate the compression from one to another",
        " input.parquet -o output.parquet -c ZSTD"
    );
  }
}
