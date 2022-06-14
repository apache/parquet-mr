/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.filter2;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notIn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.io.api.Binary.fromString;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;

public class TestFiltersWithMissingColumns {
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  public Path path;

  @Before
  public void createDataFile() throws Exception {
    File file = temp.newFile("test.parquet");
    this.path = new Path(file.toString());

    MessageType type = Types.buildMessage()
        .addField(Types.required(INT64).id(1).named("id"))
        .addField(Types.required(BINARY).as(UTF8).named("data"))
        .named("test");

    SimpleGroupFactory factory = new SimpleGroupFactory(type);

    ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(type)
        .build();

    try {
      for (long i = 0; i < 1000; i += 1) {
        Group g = factory.newGroup();
        g.add(0, i);
        g.add(1, "data-" + i);
        writer.write(g);
      }
    } finally {
      writer.close();
    }
  }

  @Test
  public void testNormalFilter() throws Exception {
    assertEquals(500, countFilteredRecords(path, lt(longColumn("id"), 500L)));
    assertEquals(500, countFilteredRecords(path, lt(longColumn(new Type.ID(1)), 500L)));
  }

  @Test
  public void testSimpleMissingColumnFilter() throws Exception {
    assertEquals(0, countFilteredRecords(path, lt(longColumn("missing"), 500L)));
    Set<Long> values = new HashSet<>();
    values.add(1L);
    values.add(2L);
    values.add(5L);
    assertEquals(0, countFilteredRecords(path, in(longColumn("missing"), values)));
    assertEquals(1000, countFilteredRecords(path, notIn(longColumn("missing"), values)));
    assertEquals(0, countFilteredRecords(path, in(longColumn(new Type.ID(11)), values)));
    assertEquals(1000, countFilteredRecords(path, notIn(longColumn(new Type.ID(11)), values)));
  }

  @Test
  public void testAndMissingColumnFilter() throws Exception {
    // missing column filter is true
    assertEquals(500, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        eq(binaryColumn("missing"), null)
    )));
    assertEquals(500, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        notEq(binaryColumn("missing"), fromString("any"))
    )));

    assertEquals(500, countFilteredRecords(path, and(
        eq(binaryColumn("missing"), null),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, and(
        notEq(binaryColumn("missing"), fromString("any")),
        lt(longColumn("id"), 500L)
    )));

    // missing column filter is false
    assertEquals(0, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        eq(binaryColumn("missing"), fromString("any"))
    )));
    assertEquals(0, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        notEq(binaryColumn("missing"), null)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        lt(doubleColumn("missing"), 33.33)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        ltEq(doubleColumn("missing"), 33.33)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        gt(doubleColumn("missing"), 33.33)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        lt(longColumn("id"), 500L),
        gtEq(doubleColumn("missing"), 33.33)
    )));

    assertEquals(0, countFilteredRecords(path, and(
        eq(binaryColumn("missing"), fromString("any")),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        notEq(binaryColumn("missing"), null),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        lt(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        ltEq(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        gt(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
        gtEq(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));
  }

  @Test
  public void testAndMissingColumnFilterForID() throws Exception {
    // missing column filter is true
    assertEquals(500, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      eq(binaryColumn(new Type.ID(11)), null)
    )));
    assertEquals(500, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      notEq(binaryColumn(new Type.ID(11)), fromString("any"))
    )));

    assertEquals(500, countFilteredRecords(path, and(
      eq(binaryColumn(new Type.ID(11)), null),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, and(
      notEq(binaryColumn(new Type.ID(11)), fromString("any")),
      lt(longColumn(new Type.ID(1)), 500L)
    )));

    // missing column filter is false
    assertEquals(0, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      eq(binaryColumn(new Type.ID(11)), fromString("any"))
    )));
    assertEquals(0, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      notEq(binaryColumn(new Type.ID(11)), null)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      lt(doubleColumn(new Type.ID(11)), 33.33)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      ltEq(doubleColumn(new Type.ID(11)), 33.33)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      gt(doubleColumn(new Type.ID(11)), 33.33)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      lt(longColumn(new Type.ID(1)), 500L),
      gtEq(doubleColumn(new Type.ID(11)), 33.33)
    )));

    assertEquals(0, countFilteredRecords(path, and(
      eq(binaryColumn(new Type.ID(11)), fromString("any")),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      notEq(binaryColumn(new Type.ID(11)), null),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      lt(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      ltEq(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      gt(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(0, countFilteredRecords(path, and(
      gtEq(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
  }

  @Test
  public void testOrMissingColumnFilter() throws Exception {
    // missing column filter is false
    assertEquals(500, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        eq(binaryColumn("missing"), fromString("any"))
    )));
    assertEquals(500, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        notEq(binaryColumn("missing"), null)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        lt(doubleColumn("missing"), 33.33)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        ltEq(doubleColumn("missing"), 33.33)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        gt(doubleColumn("missing"), 33.33)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        gtEq(doubleColumn("missing"), 33.33)
    )));

    assertEquals(500, countFilteredRecords(path, or(
        eq(binaryColumn("missing"), fromString("any")),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        notEq(binaryColumn("missing"), null),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        lt(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        ltEq(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        gt(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
        gtEq(doubleColumn("missing"), 33.33),
        lt(longColumn("id"), 500L)
    )));

    // missing column filter is false
    assertEquals(1000, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        eq(binaryColumn("missing"), null)
    )));
    assertEquals(1000, countFilteredRecords(path, or(
        lt(longColumn("id"), 500L),
        notEq(binaryColumn("missing"), fromString("any"))
    )));

    assertEquals(1000, countFilteredRecords(path, or(
        eq(binaryColumn("missing"), null),
        lt(longColumn("id"), 500L)
    )));
    assertEquals(1000, countFilteredRecords(path, or(
        notEq(binaryColumn("missing"), fromString("any")),
        lt(longColumn("id"), 500L)
    )));
  }

  @Test
  public void testOrMissingColumnFilterForID() throws Exception {
    // missing column filter is false
    assertEquals(500, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      eq(binaryColumn(new Type.ID(11)), fromString("any"))
    )));
    assertEquals(500, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      notEq(binaryColumn(new Type.ID(11)), null)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      lt(doubleColumn(new Type.ID(11)), 33.33)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      ltEq(doubleColumn(new Type.ID(11)), 33.33)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      gt(doubleColumn(new Type.ID(11)), 33.33)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      gtEq(doubleColumn(new Type.ID(11)), 33.33)
    )));

    assertEquals(500, countFilteredRecords(path, or(
      eq(binaryColumn(new Type.ID(11)), fromString("any")),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      notEq(binaryColumn(new Type.ID(11)), null),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      lt(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      ltEq(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      gt(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(500, countFilteredRecords(path, or(
      gtEq(doubleColumn(new Type.ID(11)), 33.33),
      lt(longColumn(new Type.ID(1)), 500L)
    )));

    // missing column filter is false
    assertEquals(1000, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      eq(binaryColumn(new Type.ID(11)), null)
    )));
    assertEquals(1000, countFilteredRecords(path, or(
      lt(longColumn(new Type.ID(1)), 500L),
      notEq(binaryColumn(new Type.ID(11)), fromString("any"))
    )));

    assertEquals(1000, countFilteredRecords(path, or(
      eq(binaryColumn(new Type.ID(11)), null),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
    assertEquals(1000, countFilteredRecords(path, or(
      notEq(binaryColumn(new Type.ID(11)), fromString("any")),
      lt(longColumn(new Type.ID(1)), 500L)
    )));
  }

  public static long countFilteredRecords(Path path, FilterPredicate pred) throws IOException{
    ParquetReader<Group> reader = ParquetReader
        .builder(new GroupReadSupport(), path)
        .withFilter(FilterCompat.get(pred))
        .build();

    long count = 0;
    try {
      while (reader.read() != null) {
        count += 1;
      }
    } finally {
      reader.close();
    }
    return count;
  }
}
