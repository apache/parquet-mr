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
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.LogicalTypeAnnotation.geometryType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.geometry.GeometryStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.Edges;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeometryEncoding;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

public class TestGeometryTypeRoundTrip {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return file.toPath();
  }

  @Test
  public void testBasicReadWriteGeometryValue() throws IOException {
    GeometryFactory geomFactory = new GeometryFactory();
    WKBWriter wkbWriter = new WKBWriter();
    Binary[] points = {
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(1.0, 1.0)))),
      Binary.fromConstantByteArray(wkbWriter.write(geomFactory.createPoint(new Coordinate(2.0, 2.0))))
    };

    MessageType schema = Types.buildMessage()
        .required(BINARY)
        .as(geometryType(GeometryEncoding.WKB, Edges.PLANAR, "EPSG:4326", null))
        .named("col_geom")
        .named("msg");

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);
    GroupFactory factory = new SimpleGroupFactory(schema);
    Path path = newTempPath();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(path))
        .withConf(conf)
        .withDictionaryEncoding(false)
        .build()) {
      for (Binary value : points) {
        writer.write(factory.newGroup().append("col_geom", value));
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      Assert.assertEquals(2, reader.getRecordCount());

      System.out.println("Footer");
      System.out.println(reader.getFooter().toString());
      System.out.println();

      ColumnChunkMetaData columnChunkMetaData =
          reader.getRowGroups().get(0).getColumns().get(0);
      System.out.println("Statistics");
      System.out.println(columnChunkMetaData.getStatistics().toString());
      System.out.println();

      GeometryStatistics geometryStatistics =
          ((BinaryStatistics) columnChunkMetaData.getStatistics()).getGeometryStatistics();
      Assert.assertNotNull(geometryStatistics);
      System.out.println("GeometryStatistics");
      System.out.println(geometryStatistics);
      System.out.println();

      ColumnIndex columnIndex = reader.readColumnIndex(columnChunkMetaData);
      System.out.println("ColumnIndex");
      System.out.println(columnIndex);

      List<GeometryStatistics> pageGeometryStatistics = columnIndex.getGeometryStatistics();
      Assert.assertNotNull(pageGeometryStatistics);
      System.out.println("Page GeometryStatistics");
      System.out.println(pageGeometryStatistics);
    }
  }
}
