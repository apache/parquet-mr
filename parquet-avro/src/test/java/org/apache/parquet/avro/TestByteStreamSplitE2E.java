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
package org.apache.parquet.avro;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TestByteStreamSplitE2E {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private void testWriteReadFloatingNumbers(boolean isDouble, boolean hasNull) throws IOException {
    Schema schema = Schema.createRecord("myrecord", null, null, false);
    Schema field = Schema.create(isDouble ? Schema.Type.DOUBLE : Schema.Type.FLOAT);
    schema.setFields(Collections.singletonList(new Schema.Field("a",
            Schema.createUnion(Schema.create(Schema.Type.NULL), field), null, null)));

    File file = temp.newFile((isDouble ? "double_" : "float_") + hasNull + ".parquet");
    Path path = new Path(file.toString());
    List<GenericRecord> expected = Lists.newArrayList();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
            .<GenericRecord>builder(path)
            .withDataModel(new GenericData())
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withSchema(schema)
            .withByteStreamSplitEncoding(true)
            .build()) {
      GenericRecordBuilder builder = new GenericRecordBuilder(schema);
      Random rnd = new Random();
      for (int i = 0; i < 1000; i++) {
        if (isDouble) {
          builder.set("a", rnd.nextDouble());
        } else {
          builder.set("a", rnd.nextFloat());
        }
        if (hasNull && i % 2 == 0) {
          builder.set("a", null);
        }
        GenericRecord rec = builder.build();
        expected.add(rec);
        writer.write(rec);
      }
    }

    List<GenericRecord> records = Lists.newArrayList();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader
            .<GenericRecord>builder(path)
            .withDataModel(new GenericData())
            .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        records.add(rec);
      }
    }

    Assert.assertEquals("Content should match", expected, records);
  }

  @Test
  public void testWriteReadFloatWithoutNull() throws IOException {
    testWriteReadFloatingNumbers(false, false);
  }

  @Test
  public void testWriteReadDoubleWithoutNull() throws IOException {
    testWriteReadFloatingNumbers(true, false);
  }

  @Test
  public void testWriteReadFloatWithNull() throws IOException {
    testWriteReadFloatingNumbers(false, true);
  }

  @Test
  public void testWriteReadDoubleWithNull() throws IOException {
    testWriteReadFloatingNumbers(true, true);
  }

}
