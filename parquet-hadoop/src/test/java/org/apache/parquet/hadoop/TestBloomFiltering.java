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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetrieverMock;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestBloomFiltering {
  private static final Path FILE_V1 = createTempFile(false);
  private static final Path FILE_V2 = createTempFile(false);
  private static final Path FILE_V1_E = createTempFile(true);
  private static final Path FILE_V2_E = createTempFile(true);
  private static final Logger LOGGER = LoggerFactory.getLogger(TestBloomFiltering.class);
  private static final Random RANDOM = new Random(42);
  private static final String[] PHONE_KINDS = { null, "mobile", "home", "work" };
  private static final List<PhoneBookWriter.User> DATA = Collections.unmodifiableList(generateData(10000));

  private static final byte[] FOOTER_ENCRYPTION_KEY = "0123456789012345".getBytes();
  private static final byte[] COLUMN_ENCRYPTION_KEY1 = "1234567890123450".getBytes();
  private static final byte[] COLUMN_ENCRYPTION_KEY2 = "1234567890123451".getBytes();
  private static final String FOOTER_ENCRYPTION_KEY_ID = "kf";
  private static final String COLUMN_ENCRYPTION_KEY1_ID = "kc1";
  private static final String COLUMN_ENCRYPTION_KEY2_ID = "kc2";

  private final Path file;
  private final boolean isEncrypted;

  public TestBloomFiltering(Path file, boolean isEncrypted) {
    this.file = file;
    this.isEncrypted = isEncrypted;
  }

  private static Path createTempFile(boolean encrypted) {
    String suffix = encrypted ? ".parquet.encrypted" : ".parquet";
    try {
      return new Path(Files.createTempFile("test-bloom-filter_", suffix).toAbsolutePath().toString());
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  @Parameterized.Parameters(name = "Run {index}: isEncrypted={1}")
  public static Collection<Object[]> params() {
    return Arrays.asList(
      new Object[] { FILE_V1, false /*isEncrypted*/ },
      new Object[] { FILE_V2, false /*isEncrypted*/ },
      new Object[] { FILE_V1_E, true /*isEncrypted*/ },
      new Object[] { FILE_V2_E, true /*isEncrypted*/ });
  }

  private static List<PhoneBookWriter.User> generateData(int rowCount) {
    List<PhoneBookWriter.User> users = new ArrayList<>();
    List<String> names = generateNames(rowCount);
    for (int i = 0; i < rowCount; ++i) {
      users.add(new PhoneBookWriter.User(i, names.get(i), generatePhoneNumbers(), generateLocation(i, rowCount)));
    }
    return users;
  }

  private static List<String> generateNames(int rowCount) {
    List<String> list = new ArrayList<>();

    // Adding fix values for filtering
    list.add("anderson");
    list.add("anderson");
    list.add("miller");
    list.add("miller");
    list.add("miller");
    list.add("thomas");
    list.add("thomas");
    list.add("williams");

    int nullCount = rowCount / 100;

    String alphabet = "aabcdeefghiijklmnoopqrstuuvwxyz";
    int maxLength = 8;
    for (int i = rowCount - list.size() - nullCount; i >= 0; --i) {
      int l = RANDOM.nextInt(maxLength);
      StringBuilder builder = new StringBuilder(l);
      for (int j = 0; j < l; ++j) {
        builder.append(alphabet.charAt(RANDOM.nextInt(alphabet.length())));
      }
      list.add(builder.toString());
    }
    list.sort((str1, str2) -> -str1.compareTo(str2));

    // Adding nulls to random places
    for (int i = 0; i < nullCount; ++i) {
      list.add(RANDOM.nextInt(list.size()), null);
    }

    return list;
  }

  protected static List<PhoneBookWriter.User> generateDictionaryData(int rowCount) {
    List<PhoneBookWriter.User> users = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (int i = 0; i < rowCount / 5; i++) {
      names.add("miller");
      names.add("anderson");
      names.add("thomas");
      names.add("chenLiang");
      names.add("len");
    }
    for (int i = 0; i < rowCount; ++i) {
      users.add(new PhoneBookWriter.User(i, names.get(i), generatePhoneNumbers(), generateLocation(i, rowCount)));
    }
    return users;
  }

  private static List<PhoneBookWriter.PhoneNumber> generatePhoneNumbers() {
    int length = RANDOM.nextInt(5) - 1;
    if (length < 0) {
      return null;
    }
    List<PhoneBookWriter.PhoneNumber> phoneNumbers = new ArrayList<>(length);
    for (int i = 0; i < length; ++i) {
      // 6 digits numbers
      long number = Math.abs(RANDOM.nextLong() % 900000) + 100000;
      phoneNumbers.add(new PhoneBookWriter.PhoneNumber(number, PHONE_KINDS[RANDOM.nextInt(PHONE_KINDS.length)]));
    }
    return phoneNumbers;
  }

  private static PhoneBookWriter.Location generateLocation(int id, int rowCount) {
    if (RANDOM.nextDouble() < 0.01) {
      return null;
    }

    if (RANDOM.nextDouble() < 0.001) {
      return new PhoneBookWriter.Location(99.9, 99.9);
    }

    double lat = RANDOM.nextDouble() * 90.0 - (id < rowCount / 2 ? 90.0 : 0.0);
    double lon = RANDOM.nextDouble() * 90.0 - (id < rowCount / 4 || id >= 3 * rowCount / 4 ? 90.0 : 0.0);

    return new PhoneBookWriter.Location(RANDOM.nextDouble() < 0.01 ? null : lat, RANDOM.nextDouble() < 0.01 ? null : lon);
  }

  private List<PhoneBookWriter.User> readUsers(FilterPredicate filter, boolean useOtherFiltering,
                                               boolean useBloomFilter) throws IOException {
    FileDecryptionProperties fileDecryptionProperties = null;
    if (isEncrypted) {
      DecryptionKeyRetrieverMock decryptionKeyRetrieverMock = new DecryptionKeyRetrieverMock()
        .putKey(FOOTER_ENCRYPTION_KEY_ID, FOOTER_ENCRYPTION_KEY)
        .putKey(COLUMN_ENCRYPTION_KEY1_ID, COLUMN_ENCRYPTION_KEY1)
        .putKey(COLUMN_ENCRYPTION_KEY2_ID, COLUMN_ENCRYPTION_KEY2);

      fileDecryptionProperties = FileDecryptionProperties.builder()
        .withKeyRetriever(decryptionKeyRetrieverMock)
        .build();
    }

    return PhoneBookWriter.readUsers(ParquetReader.builder(new GroupReadSupport(), file)
      .withFilter(FilterCompat.get(filter))
      .withDecryption(fileDecryptionProperties)
      .useDictionaryFilter(useOtherFiltering)
      .useStatsFilter(useOtherFiltering)
      .useRecordFilter(useOtherFiltering)
      .useBloomFilter(useBloomFilter)
      .useColumnIndexFilter(useOtherFiltering), true);
  }

  // Assumes that both lists are in the same order
  private static void assertContains(Stream<PhoneBookWriter.User> expected, List<PhoneBookWriter.User> actual) {
    Iterator<PhoneBookWriter.User> expIt = expected.iterator();
    if (!expIt.hasNext()) {
      return;
    }
    PhoneBookWriter.User exp = expIt.next();
    for (PhoneBookWriter.User act : actual) {
      if (act.equals(exp)) {
        if (!expIt.hasNext()) {
          break;
        }
        exp = expIt.next();
      }
    }
    assertFalse("Not all expected elements are in the actual list. E.g.: " + exp, expIt.hasNext());
  }

  private void assertCorrectFiltering(Predicate<PhoneBookWriter.User> expectedFilter, FilterPredicate actualFilter)
    throws IOException {
    // Check with only bloom filter based filtering
    List<PhoneBookWriter.User> result = readUsers(actualFilter, false, true);

    assertTrue("Bloom filtering should drop some row groups", result.size() < DATA.size());
    LOGGER.info("{}/{} records read; filtering ratio: {}%", result.size(), DATA.size(),
      100 * result.size() / DATA.size());
    // Asserts that all the required records are in the result
    assertContains(DATA.stream().filter(expectedFilter), result);
    // Asserts that all the retrieved records are in the file (validating non-matching records)
    assertContains(result.stream(), DATA);

    // Check with all the filtering filtering to ensure the result contains exactly the required values
    result = readUsers(actualFilter, true, false);
    assertEquals(DATA.stream().filter(expectedFilter).collect(Collectors.toList()), result);
  }

  protected static FileEncryptionProperties getFileEncryptionProperties() {
    ColumnEncryptionProperties columnProperties1 = ColumnEncryptionProperties
      .builder("id")
      .withKey(COLUMN_ENCRYPTION_KEY1)
      .withKeyID(COLUMN_ENCRYPTION_KEY1_ID)
      .build();

    ColumnEncryptionProperties columnProperties2 = ColumnEncryptionProperties
      .builder("name")
      .withKey(COLUMN_ENCRYPTION_KEY2)
      .withKeyID(COLUMN_ENCRYPTION_KEY2_ID)
      .build();
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = new HashMap<>();

    columnPropertiesMap.put(columnProperties1.getPath(), columnProperties1);
    columnPropertiesMap.put(columnProperties2.getPath(), columnProperties2);

    FileEncryptionProperties encryptionProperties = FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyID(FOOTER_ENCRYPTION_KEY_ID)
      .withEncryptedColumns(columnPropertiesMap)
      .build();

    return encryptionProperties;
  }

  private static void writePhoneBookToFile(Path file,
                                           ParquetProperties.WriterVersion parquetVersion,
                                           FileEncryptionProperties encryptionProperties) throws IOException {
    int pageSize = DATA.size() / 100;     // Ensure that several pages will be created
    int rowGroupSize = pageSize * 4;    // Ensure that there are more row-groups created
    PhoneBookWriter.write(ExampleParquetWriter.builder(file)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withBloomFilterNDV("location.lat", 10000L)
        .withBloomFilterNDV("name", 10000L)
        .withBloomFilterNDV("id", 10000L)
        .withEncryption(encryptionProperties)
        .withWriterVersion(parquetVersion),
      DATA);
  }

  private static void deleteFile(Path file) throws IOException {
    file.getFileSystem(new Configuration()).delete(file, false);
  }

  @BeforeClass
  public static void createFiles() throws IOException {
    writePhoneBookToFile(FILE_V1, ParquetProperties.WriterVersion.PARQUET_1_0, null);
    writePhoneBookToFile(FILE_V2, ParquetProperties.WriterVersion.PARQUET_2_0, null);

    FileEncryptionProperties encryptionProperties = getFileEncryptionProperties();
    writePhoneBookToFile(FILE_V1_E, ParquetProperties.WriterVersion.PARQUET_1_0, encryptionProperties);
    writePhoneBookToFile(FILE_V2_E, ParquetProperties.WriterVersion.PARQUET_2_0, encryptionProperties);
  }

  @AfterClass
  public static void deleteFiles() throws IOException {
    deleteFile(FILE_V1);
    deleteFile(FILE_V2);
    deleteFile(FILE_V1_E);
    deleteFile(FILE_V2_E);
  }


  @Test
  public void testSimpleFiltering() throws IOException {
    assertCorrectFiltering(
      record -> record.getId() == 1234L,
      eq(longColumn("id"), 1234L));

    assertCorrectFiltering(
      record -> "miller".equals(record.getName()),
      eq(binaryColumn("name"), Binary.fromString("miller")));

    Set<Binary> values1 = new HashSet<>();
    values1.add(Binary.fromString("miller"));
    values1.add(Binary.fromString("anderson"));

    assertCorrectFiltering(
      record -> "miller".equals(record.getName()) || "anderson".equals(record.getName()),
      in(binaryColumn("name"), values1));

    Set<Binary> values2 = new HashSet<>();
    values2.add(Binary.fromString("miller"));
    values2.add(Binary.fromString("alien"));

    assertCorrectFiltering(
      record -> "miller".equals(record.getName()),
      in(binaryColumn("name"), values2));

    Set<Binary> values3 = new HashSet<>();
    values3.add(Binary.fromString("alien"));
    values3.add(Binary.fromString("predator"));

    assertCorrectFiltering(
      record -> "dummy".equals(record.getName()),
      in(binaryColumn("name"), values3));
  }

  @Test
  public void testNestedFiltering() throws IOException {
    assertCorrectFiltering(
      record -> {
        PhoneBookWriter.Location location = record.getLocation();
        return location != null && location.getLat() != null && location.getLat() == 99.9;
      },
      eq(doubleColumn("location.lat"), 99.9));
  }
}
