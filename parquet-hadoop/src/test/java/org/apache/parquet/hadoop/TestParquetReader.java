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

import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestParquetReader {

  private static final Path FILE_V1 = createTempFile();
  private static final Path FILE_V2 = createTempFile();
  private static final Path STATIC_FILE_WITHOUT_COL_INDEXES =
      createPathFromCP("/test-file-with-no-column-indexes-1.parquet");
  private static final List<PhoneBookWriter.User> DATA = Collections.unmodifiableList(makeUsers(1000));

  private final Path file;
  private final long fileSize;
  private TrackingByteBufferAllocator allocator;

  private static Path createPathFromCP(String path) {
    try {
      return new Path(TestParquetReader.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public TestParquetReader(Path file) throws IOException {
    this.file = file;
    this.fileSize =
        file.getFileSystem(new Configuration()).getFileStatus(file).getLen();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {{FILE_V1}, {FILE_V2}, {STATIC_FILE_WITHOUT_COL_INDEXES}};
    return Arrays.asList(data);
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

  private static void deleteFile(Path file) throws IOException {
    file.getFileSystem(new Configuration()).delete(file, false);
  }

  public static List<PhoneBookWriter.User> makeUsers(int rowCount) {
    List<PhoneBookWriter.User> users = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      PhoneBookWriter.Location location = null;
      if (i % 3 == 1) {
        location = new PhoneBookWriter.Location((double) i, (double) i * 2);
      }
      if (i % 3 == 2) {
        location = new PhoneBookWriter.Location((double) i, null);
      }
      // row index of each row in the file is same as the user id.
      users.add(new PhoneBookWriter.User(
          i, "p" + i, Arrays.asList(new PhoneBookWriter.PhoneNumber(i, "cell")), location));
    }
    return users;
  }

  private static Path createTempFile() {
    try {
      return new Path(Files.createTempFile("test-ci_", ".parquet")
          .toAbsolutePath()
          .toString());
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private static void writePhoneBookToFile(Path file, ParquetProperties.WriterVersion parquetVersion)
      throws IOException {
    int pageSize = DATA.size() / 10; // Ensure that several pages will be created
    int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created

    PhoneBookWriter.write(
        ExampleParquetWriter.builder(file)
            .withWriteMode(OVERWRITE)
            .withRowGroupSize(rowGroupSize)
            .withPageSize(pageSize)
            .withWriterVersion(parquetVersion),
        DATA);
  }

  private List<PhoneBookWriter.User> readUsers(
      FilterCompat.Filter filter,
      boolean useOtherFiltering,
      boolean useColumnIndexFilter,
      boolean materializeRowGroup)
      throws IOException {
    return readUsers(filter, useOtherFiltering, useColumnIndexFilter, materializeRowGroup, 0, this.fileSize);
  }

  private List<PhoneBookWriter.User> readUsers(
      FilterCompat.Filter filter,
      boolean useOtherFiltering,
      boolean useColumnIndexFilter,
      boolean materializeRowGroup,
      long rangeStart,
      long rangeEnd)
      throws IOException {
    Configuration conf = new Configuration();
    if (!materializeRowGroup) {
      // @Todo parameterize this value?
      conf.setInt(ParquetInputFormat.COLUMN_CHUNK_BUFFER_SIZE, 25);
    }
    return PhoneBookWriter.readUsers(
        ParquetReader.builder(new GroupReadSupport(), file)
            .withAllocator(allocator)
            .withFilter(filter)
            .withConf(conf)
            .useDictionaryFilter(useOtherFiltering)
            .useStatsFilter(useOtherFiltering)
            .useRecordFilter(useOtherFiltering)
            .useColumnIndexFilter(useColumnIndexFilter)
            .withFileRange(rangeStart, rangeEnd),
        true);
  }

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void testCurrentRowIndex() throws Exception {
    ParquetReader<Group> reader = PhoneBookWriter.createReader(file, FilterCompat.NOOP, allocator);
    // Fetch row index without processing any row.
    assertEquals(reader.getCurrentRowIndex(), -1);
    reader.read();
    assertEquals(reader.getCurrentRowIndex(), 0);
    // calling the same API again and again should return same result.
    assertEquals(reader.getCurrentRowIndex(), 0);

    reader.read();
    assertEquals(reader.getCurrentRowIndex(), 1);
    assertEquals(reader.getCurrentRowIndex(), 1);
    long expectedCurrentRowIndex = 2L;
    while (reader.read() != null) {
      assertEquals(reader.getCurrentRowIndex(), expectedCurrentRowIndex);
      expectedCurrentRowIndex++;
    }
    // reader.read() returned null and so reader doesn't have any more rows.
    assertEquals(reader.getCurrentRowIndex(), -1);
  }

  @Test
  public void testRangeFiltering() throws Exception {
    // The readUsers also validates the rowIndex for each returned row.
    readUsers(FilterCompat.NOOP, false, false, true, this.fileSize / 2, this.fileSize);
    readUsers(FilterCompat.NOOP, true, false, true, this.fileSize / 3, this.fileSize * 3 / 4);
    readUsers(FilterCompat.NOOP, false, true, true, this.fileSize / 4, this.fileSize / 2);
    readUsers(FilterCompat.NOOP, true, true, true, this.fileSize * 3 / 4, this.fileSize);
  }

  @Test
  public void testRangeFilteringLazy() throws Exception {
    // The readUsers also validates the rowIndex for each returned row.
    readUsers(FilterCompat.NOOP, false, false, false, this.fileSize / 2, this.fileSize);
    readUsers(FilterCompat.NOOP, true, false, false, this.fileSize / 3, this.fileSize * 3 / 4);
    readUsers(FilterCompat.NOOP, false, true, false, this.fileSize / 4, this.fileSize / 2);
    readUsers(FilterCompat.NOOP, true, true, false, this.fileSize * 3 / 4, this.fileSize);
  }

  @Test
  public void testSimpleFiltering() throws Exception {
    Set<Long> idSet = new HashSet<>();
    idSet.add(123l);
    idSet.add(567l);
    // The readUsers also validates the rowIndex for each returned row.
    List<PhoneBookWriter.User> filteredUsers1 =
        readUsers(FilterCompat.get(in(longColumn("id"), idSet)), true, true, true);
    assertEquals(filteredUsers1.size(), 2L);
    List<PhoneBookWriter.User> filteredUsers2 =
        readUsers(FilterCompat.get(in(longColumn("id"), idSet)), true, false, true);
    assertEquals(filteredUsers2.size(), 2L);
    List<PhoneBookWriter.User> filteredUsers3 =
        readUsers(FilterCompat.get(in(longColumn("id"), idSet)), false, false, true);
    assertEquals(filteredUsers3.size(), 1000L);
  }

  @Test
  public void testSimpleFilteringLazy() throws Exception {
    Set<Long> idSet = new HashSet<>();
    idSet.add(123l);
    idSet.add(567l);
    // The readUsers also validates the rowIndex for each returned row.
    List<PhoneBookWriter.User> filteredUsers1 =
        readUsers(FilterCompat.get(in(longColumn("id"), idSet)), true, true, false);
    assertEquals(filteredUsers1.size(), 2L);
    List<PhoneBookWriter.User> filteredUsers2 =
        readUsers(FilterCompat.get(in(longColumn("id"), idSet)), true, false, false);
    assertEquals(filteredUsers2.size(), 2L);
    List<PhoneBookWriter.User> filteredUsers3 =
        readUsers(FilterCompat.get(in(longColumn("id"), idSet)), false, false, false);
    assertEquals(filteredUsers3.size(), 1000L);
  }

  @Test
  public void testNoFiltering() throws Exception {
    assertEquals(DATA, readUsers(FilterCompat.NOOP, false, false, true));
    assertEquals(DATA, readUsers(FilterCompat.NOOP, true, false, true));
    assertEquals(DATA, readUsers(FilterCompat.NOOP, false, true, true));
    assertEquals(DATA, readUsers(FilterCompat.NOOP, true, true, true));
  }

  @Test
  public void testNoFilteringLazy() throws Exception {
    assertEquals(DATA, readUsers(FilterCompat.NOOP, false, false, false));
    assertEquals(DATA, readUsers(FilterCompat.NOOP, true, false, false));
    assertEquals(DATA, readUsers(FilterCompat.NOOP, false, true, false));
    assertEquals(DATA, readUsers(FilterCompat.NOOP, true, true, false));
  }
}
