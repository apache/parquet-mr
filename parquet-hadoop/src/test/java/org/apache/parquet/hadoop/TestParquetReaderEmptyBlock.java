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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;

public class TestParquetReaderEmptyBlock {

  // The parquet file contains only one empty row group
  private static final Path EMPTY_BLOCK_FILE_1 = createPathFromCP("/test-empty-row-group_1.parquet");

  // The parquet file contains three row groups, the second one is empty
  private static final Path EMPTY_BLOCK_FILE_2 = createPathFromCP("/test-empty-row-group_2.parquet");

  // The parquet file contains four row groups, the second one and third one are empty
  private static final Path EMPTY_BLOCK_FILE_3 = createPathFromCP("/test-empty-row-group_3.parquet");

  private static Path createPathFromCP(String path) {
    try {
      return new Path(TestParquetReaderEmptyBlock.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testReadOnlyEmptyBlock() throws IOException {
    Configuration conf = new Configuration();
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    InputFile inputFile = HadoopInputFile.fromPath(EMPTY_BLOCK_FILE_1, conf);

    // The parquet file contains only one empty row group
    ParquetMetadata readFooter = ParquetFileReader.readFooter(inputFile, options, inputFile.newStream());
    Assert.assertEquals(1, readFooter.getBlocks().size());

    // The empty block is skipped via readNextRowGroup()
    try (ParquetFileReader r = new ParquetFileReader(inputFile, options)) {
      Assert.assertNull(r.readNextRowGroup());
    }

    // The empty block is skipped via readNextFilteredRowGroup()
    FilterCompat.Filter filter = FilterCompat.get(gt(intColumn("a"), 1));
    ParquetReadOptions filterOptions = ParquetReadOptions.builder()
      .copy(options)
      .withRecordFilter(filter)
      .useStatsFilter(true)
      .build();
    try (ParquetFileReader r = new ParquetFileReader(inputFile, filterOptions)) {
      Assert.assertNull(r.readNextFilteredRowGroup());
    }
  }

  @Test
  public void testSkipEmptyBlock() throws IOException {
    Configuration conf = new Configuration();
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    InputFile inputFile = HadoopInputFile.fromPath(EMPTY_BLOCK_FILE_2, conf);

    // The parquet file contains three row groups, the second one is empty
    ParquetMetadata readFooter = ParquetFileReader.readFooter(inputFile, options, inputFile.newStream());
    Assert.assertEquals(3, readFooter.getBlocks().size());

    // Second row group is empty and skipped via readNextRowGroup()
    try (ParquetFileReader r = new ParquetFileReader(inputFile, options)) {
      PageReadStore pages = null;
      pages = r.readNextRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(1, pages.getRowCount());

      pages = r.readNextRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(3, pages.getRowCount());

      pages = r.readNextRowGroup();
      Assert.assertNull(pages);
    }

    // Only the last row group is read via readNextFilteredRowGroup()
    FilterCompat.Filter filter = FilterCompat.get(gt(intColumn("a"), 1));
    ParquetReadOptions filterOptions = ParquetReadOptions.builder()
      .copy(options)
      .withRecordFilter(filter)
      .useStatsFilter(true)
      .build();
    try (ParquetFileReader r = new ParquetFileReader(inputFile, filterOptions)) {
      PageReadStore pages = null;
      pages = r.readNextFilteredRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(3, pages.getRowCount());

      pages = r.readNextFilteredRowGroup();
      Assert.assertNull(pages);
    }
  }

  @Test
  public void testSkipEmptyBlocksNextToEachOther() throws IOException {
    Configuration conf = new Configuration();
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    InputFile inputFile = HadoopInputFile.fromPath(EMPTY_BLOCK_FILE_3, conf);

    // The parquet file contains four row groups, the second one and third one are empty
    ParquetMetadata readFooter = ParquetFileReader.readFooter(inputFile, options, inputFile.newStream());
    Assert.assertEquals(4, readFooter.getBlocks().size());

    // Second and third row groups are empty and skipped via readNextRowGroup()
    try (ParquetFileReader r = new ParquetFileReader(inputFile, options)) {
      PageReadStore pages = null;
      pages = r.readNextRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(1, pages.getRowCount());

      pages = r.readNextRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(4, pages.getRowCount());

      pages = r.readNextRowGroup();
      Assert.assertNull(pages);
    }

    // Only the last row group is read via readNextFilteredRowGroup()
    FilterCompat.Filter filter = FilterCompat.get(gt(intColumn("a"), 1));
    ParquetReadOptions filterOptions = ParquetReadOptions.builder()
      .copy(options)
      .withRecordFilter(filter)
      .useStatsFilter(true)
      .build();
    try (ParquetFileReader r = new ParquetFileReader(inputFile, filterOptions)) {
      PageReadStore pages = null;
      pages = r.readNextFilteredRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(4, pages.getRowCount());

      pages = r.readNextFilteredRowGroup();
      Assert.assertNull(pages);
    }
  }

}
