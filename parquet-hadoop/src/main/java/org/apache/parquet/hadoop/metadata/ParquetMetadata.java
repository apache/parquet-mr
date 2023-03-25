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
package org.apache.parquet.hadoop.metadata;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 * Meta Data block stored in the footer of the file
 * contains file level (Codec, Schema, ...) and block level (location, columns, record count, ...) meta data
 */
public class ParquetMetadata {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  // Enable FAIL_ON_EMPTY_BEANS on objectmapper. Without this feature parquet-casdacing tests fail,
  // because LogicalTypeAnnotation implementations are classes without any property.
  static {
    objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
  }

  /**
   * @param parquetMetaData an instance of parquet metadata to convert
   * @return the json representation
   */
  public static String toJSON(ParquetMetadata parquetMetaData) {
    return toJSON(parquetMetaData, false);
  }

  /**
   *
   * @param parquetMetaData an instance of parquet metadata to convert
   * @return the pretty printed json representation
   */
  public static String toPrettyJSON(ParquetMetadata parquetMetaData) {
    return toJSON(parquetMetaData, true);
  }

  private static String toJSON(ParquetMetadata parquetMetaData, boolean isPrettyPrint) {
    StringWriter stringWriter = new StringWriter();

    try {
      if (isPrettyPrint) {
        Object objectToPrint;
        if (parquetMetaData.getFileMetaData() == null ||
            parquetMetaData.getFileMetaData().getEncryptionType() == FileMetaData.EncryptionType.UNENCRYPTED) {
          objectToPrint = parquetMetaData;
        } else {
          objectToPrint = parquetMetaData.getFileMetaData();
        }
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(stringWriter, objectToPrint);
      } else {
        objectMapper.writeValue(stringWriter, parquetMetaData);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return stringWriter.toString();
  }

  /**
   *
   * @param json the json representation
   * @return the parsed object
   */
  public static ParquetMetadata fromJSON(String json) {
    try {
      return objectMapper.readValue(new StringReader(json), ParquetMetadata.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final FileMetaData fileMetaData;
  private final List<BlockMetaData> blocks;

  /**
   *
   * @param fileMetaData file level metadata
   * @param blocks block level metadata
   */
  public ParquetMetadata(FileMetaData fileMetaData, List<BlockMetaData> blocks) {
    this.fileMetaData = fileMetaData;
    this.blocks = blocks;
  }

  /**
   *
   * @return block level metadata
   */
  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  /**
   *
   * @return file level meta data
   */
  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }


  @Override
  public String toString() {
    return "ParquetMetaData{"+fileMetaData+", blocks: "+blocks+"}";
  }

}
