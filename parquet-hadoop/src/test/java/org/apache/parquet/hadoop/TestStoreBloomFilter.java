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

package org.apache.parquet.hadoop;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.TestBloomFiltering.generateDictionaryData;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

@RunWith(Parameterized.class)
public class TestStoreBloomFilter {
  private static final Path FILE_V1 = createTempFile("v1");
  private static final Path FILE_V2 = createTempFile("v2");
  private static final List<PhoneBookWriter.User> DATA = Collections.unmodifiableList(generateDictionaryData(10000));
  private final Path file;
  private final String version;

  public TestStoreBloomFilter(Path file, String version) {
    this.file = file;
    this.version = version;
  }

  @Parameterized.Parameters(name = "Run {index}: parquet {1}")
  public static Collection<Object[]> params() {
    return Arrays.asList(
      new Object[]{FILE_V1, "v1"},
      new Object[]{FILE_V2, "v2"});
  }

  @BeforeClass
  public static void createFiles() throws IOException {
    writePhoneBookToFile(FILE_V1, ParquetProperties.WriterVersion.PARQUET_1_0);
    writePhoneBookToFile(FILE_V2, ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  @AfterClass
  public static void deleteFiles() throws IOException {
    deleteFile(FILE_V1);
    deleteFile(FILE_V2);
  }

  @Test
  public void testStoreBloomFilter() throws IOException {
    ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(file, new Configuration()),
      ParquetReadOptions.builder().build());
    List<BlockMetaData> blocks = reader.getRowGroups();
    blocks.forEach(block -> {
      try {
        // column `id` isn't fully encoded in dictionary, it will generate `BloomFilter`
        ColumnChunkMetaData idMeta = block.getColumns().get(0);
        EncodingStats idEncoding = idMeta.getEncodingStats();
        Assert.assertTrue(idEncoding.hasNonDictionaryEncodedPages());
        Assert.assertNotNull(reader.readBloomFilter(idMeta));

        // column `name` is fully encoded in dictionary, it won't generate `BloomFilter`
        ColumnChunkMetaData nameMeta = block.getColumns().get(1);
        EncodingStats nameEncoding = nameMeta.getEncodingStats();
        Assert.assertFalse(nameEncoding.hasNonDictionaryEncodedPages());
        Assert.assertNull(reader.readBloomFilter(nameMeta));
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  private static Path createTempFile(String version) {
    try {
      return new Path(Files.createTempFile("test-store-bloom-filter-" + version, ".parquet")
        .toAbsolutePath().toString());
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private static void deleteFile(Path file) throws IOException {
    file.getFileSystem(new Configuration()).delete(file, false);
  }

  private static void writePhoneBookToFile(Path file,
    ParquetProperties.WriterVersion parquetVersion) throws IOException {
    int pageSize = DATA.size() / 100;     // Ensure that several pages will be created
    int rowGroupSize = pageSize * 4;    // Ensure that there are more row-groups created
    PhoneBookWriter.write(ExampleParquetWriter.builder(file)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withBloomFilterNDV("id", 10000L)
        .withBloomFilterNDV("name", 10000L)
        .withWriterVersion(parquetVersion),
      DATA);
  }
}
