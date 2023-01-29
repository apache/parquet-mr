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
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.commons.text.TextStringBuilder;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.Util;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;

@Parameters(commandDescription="Check bloom filters for a Parquet column")
public class ShowBloomFilterCommand extends BaseCommand {

  public ShowBloomFilterCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  String file;

  @Parameter(
    names = { "-c", "--column" },
    description = "Check the bloom filter indexes for the given column",
    required = true)
  String columnPath;

  @Parameter(
    names = { "-v", "--values" },
    description = "Check if the given values match bloom filter",
    required = true)
  List<String> testValues;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(file != null,
      "A Parquet file is required.");

    InputFile in = HadoopInputFile.fromPath(qualifiedPath(file), getConf());

    try (ParquetFileReader reader = ParquetFileReader.open(in)) {
      MessageType schema = reader.getFileMetaData().getSchema();
      PrimitiveType type = Util.primitive(columnPath, schema);

      int rowGroupIndex = 0;
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        console.info(String.format("\nRow group %d: \n%s",
          rowGroupIndex,
          new TextStringBuilder(80).appendPadding(80, '-')));

        Optional<ColumnChunkMetaData> maybeColumnMeta = block.getColumns().stream()
          .filter(c -> columnPath.equals(c.getPath().toDotString())).findFirst();
        if (!maybeColumnMeta.isPresent()) {
          console.info("column {} doesn't exist.", columnPath);
        } else {
          BloomFilterReader bloomFilterReader = reader.getBloomFilterDataReader(block);
          BloomFilter bloomFilter = bloomFilterReader.readBloomFilter(maybeColumnMeta.get());
          if (bloomFilter == null) {
            console.info("column {} has no bloom filter", columnPath);
          } else {
            for (String value: testValues) {
              if (bloomFilter.findHash(bloomFilter.hash(getOriginalType(value, type)))) {
                console.info("value {} maybe exists.", value);
              } else {
                console.info("value {} NOT exists.", value);
              }
            }
          }
        }
        ++rowGroupIndex;
      }
    }
    return 0;
  }

  private Object getOriginalType(String value, PrimitiveType type) {
    switch(type.getPrimitiveTypeName()) {
      case BINARY:
        return Binary.fromString(value);
      case INT32:
        return Integer.valueOf(value);
      case INT64:
        return Long.valueOf(value);
      case FLOAT:
        return Float.valueOf(value);
      case DOUBLE:
        return Double.valueOf(value);
      default:
        throw new IllegalArgumentException(
          "Unknown type: " + type.getPrimitiveTypeName());
    }
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
      "# Show bloom filter for column 'col' from a Parquet file",
      "-c col -v 1,2,3 -i sample.parquet");
  }
}
