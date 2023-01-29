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

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.hadoop.ParquetFileWriter.EFMAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.format.CliUtils;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@Parameters(commandDescription = "Print the Parquet file footer in json format")
public class ShowFooterCommand extends BaseCommand {

  public ShowFooterCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>", required = true)
  String target;

  @Parameter(names = { "-r", "--raw" }, description = "Print the raw thrift object of the footer")
  boolean raw = false;

  @Override
  public int run() throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(qualifiedPath(target), getConf());

    console.info(raw ? readRawFooter(inputFile) : readFooter(inputFile));

    return 0;
  }

  private String readFooter(InputFile inputFile) throws JsonProcessingException, IOException {
    String json;
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata footer = reader.getFooter();
      ObjectMapper mapper = createObjectMapper();
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
      mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
      json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(footer);
    }
    return json;
  }

  private ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }

  private String readRawFooter(InputFile file) throws IOException {
    long fileLen = file.getLength();

    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException("Not a Parquet file (length is too low: " + fileLen + ")");
    }

    try (SeekableInputStream f = file.newStream()) {
      // Read footer length and magic string - with a single seek
      byte[] magic = new byte[MAGIC.length];
      long fileMetadataLengthIndex = fileLen - magic.length - FOOTER_LENGTH_SIZE;
      f.seek(fileMetadataLengthIndex);
      int fileMetadataLength = readIntLittleEndian(f);
      f.readFully(magic);

      if (Arrays.equals(EFMAGIC, magic)) {
        throw new RuntimeException("Parquet files with encrypted footers are not supported.");
      } else if (!Arrays.equals(MAGIC, magic)) {
        throw new RuntimeException(
            "Not a Parquet file (expected magic number at tail, but found " + Arrays.toString(magic) + ')');
      }

      long fileMetadataIndex = fileMetadataLengthIndex - fileMetadataLength;
      if (fileMetadataIndex < magic.length || fileMetadataIndex >= fileMetadataLengthIndex) {
        throw new RuntimeException("Corrupted file: the footer index is not within the file: " + fileMetadataIndex);
      }
      f.seek(fileMetadataIndex);

      ByteBuffer footerBytesBuffer = ByteBuffer.allocate(fileMetadataLength);
      f.readFully(footerBytesBuffer);
      footerBytesBuffer.flip();
      InputStream footerBytesStream = ByteBufferInputStream.wrap(footerBytesBuffer);
      return prettify(CliUtils.toJson(Util.readFileMetaData(footerBytesStream)));
    }
  }

  private String prettify(String json) throws JsonProcessingException {
    ObjectMapper mapper = createObjectMapper();
    Object obj = mapper.readValue(json, Object.class);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
  }

  @Override
  public List<String> getExamples() {
    return Arrays.asList(
        "# Print the parquet-mr interpreted footer of the specified Parquet file in json format",
        "sample.parquet",
        "# Print the raw thrift footer object of the specified Parquet file in json format",
        "sample.parquet --raw");
  }

}
