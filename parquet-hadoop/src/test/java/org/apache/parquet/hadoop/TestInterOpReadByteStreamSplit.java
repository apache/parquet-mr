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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.Test;

public class TestInterOpReadByteStreamSplit {
  private static final String FLOATS_FILE = "byte_stream_split.zstd.parquet";
  private static final String EXTENDED_FILE = "byte_stream_split_extended.gzip.parquet";
  // FIXME after https://github.com/apache/parquet-testing/pull/46 is merged
  private static final String CHANGESET = "74278bc";
  private final InterOpTester interop = new InterOpTester();

  @Test
  public void testReadFloats() throws IOException {
    Path floatsFile = interop.GetInterOpFile(FLOATS_FILE, CHANGESET);
    final int expectRows = 300;

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), floatsFile).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        assertNotNull(group);
        float fval = group.getFloat("f32", 0);
        double dval = group.getDouble("f64", 0);
        // Values are from the normal distribution
        assertTrue(Math.abs(fval) < 4.0);
        assertTrue(Math.abs(dval) < 4.0);
        switch (i) {
          case 0:
            assertEquals(1.7640524f, fval, 0.0);
            assertEquals(-1.3065268517353166, dval, 0.0);
            break;
          case 1:
            assertEquals(0.4001572f, fval, 0.0);
            assertEquals(1.658130679618188, dval, 0.0);
            break;
          case expectRows - 2:
            assertEquals(-0.39944902f, fval, 0.0);
            assertEquals(-0.9301565025243212, dval, 0.0);
            break;
          case expectRows - 1:
            assertEquals(0.37005588f, fval, 0.0);
            assertEquals(-0.17858909208732915, dval, 0.0);
            break;
        }
      }
      assertNull(reader.read());
    }
  }

  private void compareColumnValues(Path path, int expectRows, String leftCol, String rightCol) throws IOException {
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), path).build()) {
      for (int i = 0; i < expectRows; ++i) {
        SimpleGroup group = (SimpleGroup) reader.read();
        assertNotNull(group);
        Object left = group.getObject(leftCol, 0);
        Object right = group.getObject(rightCol, 0);
        assertEquals(left, right);
      }
      assertNull(reader.read());
    }
  }

  @Test
  public void testReadAllSupportedTypes() throws IOException {
    Path extendedFile = interop.GetInterOpFile(EXTENDED_FILE, CHANGESET);
    final int expectRows = 200;
    compareColumnValues(extendedFile, expectRows, "float_plain", "float_byte_stream_split");
    compareColumnValues(extendedFile, expectRows, "double_plain", "double_byte_stream_split");
    compareColumnValues(extendedFile, expectRows, "int32_plain", "int32_byte_stream_split");
    compareColumnValues(extendedFile, expectRows, "int64_plain", "int64_byte_stream_split");
    compareColumnValues(extendedFile, expectRows, "float16_plain", "float16_byte_stream_split");
    compareColumnValues(extendedFile, expectRows, "flba5_plain", "flba5_byte_stream_split");
    compareColumnValues(extendedFile, expectRows, "decimal_plain", "decimal_byte_stream_split");
  }
}
