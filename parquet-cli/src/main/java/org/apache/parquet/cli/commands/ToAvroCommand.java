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
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Codecs;
import org.apache.parquet.cli.util.Schemas;
import org.slf4j.Logger;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.apache.avro.generic.GenericData.Record;
import static org.apache.parquet.cli.util.Expressions.filterSchema;

@Parameters(commandDescription="Create an Avro file from a data file")
public class ToAvroCommand extends BaseCommand {

  public ToAvroCommand(Logger console) {
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
      description = "The file containing an Avro schema for the output file")
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

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 1,
        "A data file is required.");

    String source = targets.get(0);

    CodecFactory codecFactory = Codecs.avroCodec(compressionCodecName);

    final Schema schema;
    if (avroSchemaFile != null) {
      schema = Schemas.fromAvsc(open(avroSchemaFile));
    } else {
      schema = getAvroSchema(source);
    }

    final Schema projection = filterSchema(schema, columns);
    Iterable<Record> reader = openDataFile(source, projection);
    boolean threw = true;
    long count = 0;

    DatumWriter<Record> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<Record> fileWriter = new DataFileWriter<>(datumWriter)) {
      fileWriter.setCodec(codecFactory);
      try (OutputStream os = overwrite ? create(outputPath) : createWithNoOverwrite(outputPath);
           DataFileWriter<Record> writer = fileWriter.create(projection, os)) {
        for (Record record : reader) {
          writer.append(record);
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
        "# Create an Avro file from a Parquet file",
        "sample.parquet -o sample.avro",
        "# Create an Avro file in HDFS from a local JSON file",
        "path/to/sample.json -o hdfs://user/me/sample.avro",
        "# Create an Avro file from data in S3",
        "s3a://data/path/sample.parquet -o sample.avro"
    );
  }
}
