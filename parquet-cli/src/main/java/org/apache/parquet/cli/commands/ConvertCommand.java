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
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Codecs;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static org.apache.avro.generic.GenericData.Record;
import static org.apache.parquet.cli.util.Expressions.filterSchema;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

@Parameters(commandDescription="Create a Parquet file from a data file")
public class ConvertCommand extends BaseCommand {

  public ConvertCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<file>")
  List<String> targets;

  @Parameter(
      names={"-o", "--output"},
      description="Output file path",
      required=true)
  String outputPath = null;

  @Parameter(names = {"-s", "--schema"},
      description = "The file containing the Avro schema.")
  String avroSchemaFile;

  @Parameter(
      names = {"-c", "--column", "--columns"},
      description = "List of columns")
  List<String> columns;

  @Parameter(names = {"--compression-codec"},
      description = "A compression codec name.")
  String compressionCodecName = "GZIP";

  @Parameter(
      names={"--overwrite"},
      description="Overwrite the output file if it exists")
  boolean overwrite = false;

  @Parameter(
      names={"-2", "--format-version-2", "--writer-version-2"},
      description="Use Parquet format version 2",
      hidden = true)
  boolean v2 = false;

  @Parameter(names="--row-group-size", description="Target row group size")
  int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

  @Parameter(names="--page-size", description="Target page size")
  int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

  @Parameter(names="--dictionary-size", description="Max dictionary page size")
  int dictionaryPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 1,
        "A data file is required.");

    String source = targets.get(0);

    CompressionCodecName codec = Codecs.parquetCodec(compressionCodecName);

    Schema schema;
    if (avroSchemaFile != null) {
      schema = Schemas.fromAvsc(open(avroSchemaFile));
    } else {
      schema = getAvroSchema(source);
    }
    Schema projection = filterSchema(schema, columns);

    Path outPath = qualifiedPath(outputPath);
    FileSystem outFS = outPath.getFileSystem(getConf());
    if (overwrite && outFS.exists(outPath)) {
      console.debug("Deleting output file {} (already exists)", outPath);
      outFS.delete(outPath);
    }

    Iterable<Record> reader = openDataFile(source, projection);
    boolean threw = true;
    long count = 0;
    try {
      try (ParquetWriter<Record> writer = AvroParquetWriter
          .<Record>builder(qualifiedPath(outputPath))
          .withWriterVersion(v2 ? PARQUET_2_0 : PARQUET_1_0)
          .withConf(getConf())
          .withCompressionCodec(codec)
          .withRowGroupSize(rowGroupSize)
          .withDictionaryPageSize(dictionaryPageSize < 64 ? 64 : dictionaryPageSize)
          .withDictionaryEncoding(dictionaryPageSize != 0)
          .withPageSize(pageSize)
          .withDataModel(GenericData.get())
          .withSchema(projection)
          .build()) {
        for (Record record : reader) {
          writer.write(record);
          count += 1;
        }
      }
      threw = false;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed on record " + count, e);
    } finally {
      if (reader instanceof Closeable) {
        Closeables.close((Closeable) reader, threw);
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Create a Parquet file from an Avro file",
        "sample.avro -o sample.parquet",
        "# Create a Parquet file in S3 from a local Avro file",
        "path/to/sample.avro -o s3a://user/me/sample.parquet",
        "# Create a Parquet file from Avro data in S3",
        "s3a://data/path/sample.avro -o sample.parquet"
    );
  }
}
