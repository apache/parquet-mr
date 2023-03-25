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
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Formats;
import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.slf4j.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Parameters(commandDescription="Print the Avro schema for a file")
public class SchemaCommand extends BaseCommand {

  public SchemaCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  List<String> targets;

  @Parameter(
      names={"-o", "--output"},
      description="Output file path")
  String outputPath = null;

  @Parameter(
      names={"--overwrite"},
      description="Overwrite the output file if it exists")
  boolean overwrite = false;

  @Parameter(
      names={"--parquet"},
      description="Print a Parquet schema, without converting to Avro",
      hidden=true)
  boolean parquetSchema = false;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 1,
        "Parquet file is required.");

    if (targets.size() > 1) {
      Preconditions.checkArgument(outputPath == null,
          "Cannot output multiple schemas to file %s", outputPath);
      for (String source : targets) {
        console.info("{}: {}", source, getSchema(source));
      }

    } else {
      String source = targets.get(0);

      if (outputPath != null) {
        try (OutputStream out = overwrite ?
          create(outputPath) : createWithNoOverwrite(outputPath)) {
          out.write(getSchema(source).getBytes(StandardCharsets.UTF_8));
        }
      } else {
        console.info(getSchema(source));
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print the Avro schema for a Parquet file",
        "sample.parquet",
        "# Print the Avro schema for an Avro file",
        "sample.avro",
        "# Print the Avro schema for a JSON file",
        "sample.json"
    );
  }

  private String getSchema(String source) throws IOException {
    if (parquetSchema) {
      return getParquetSchema(source);
    } else {
      return getAvroSchema(source).toString(true);
    }
  }

  private String getParquetSchema(String source) throws IOException {
    Formats.Format format;
    try (SeekableInput in = openSeekable(source)) {
      format = Formats.detectFormat((InputStream) in);
      in.seek(0);

      switch (format) {
        case PARQUET:
          try (ParquetFileReader reader = new ParquetFileReader(
            getConf(), qualifiedPath(source), ParquetMetadataConverter.NO_FILTER)) {
            return reader.getFileMetaData().getSchema().toString();
          }
        default:
          throw new IllegalArgumentException(String.format(
              "Could not get a Parquet schema for format %s: %s", format, source));
      }
    }
  }
}
