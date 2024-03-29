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
package org.apache.parquet.thrift.struct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.parquet.thrift.test.compat.AddRequiredStructV1;
import org.apache.parquet.thrift.test.compat.DefaultStructV1;
import org.apache.parquet.thrift.test.compat.ListStructV1;
import org.apache.parquet.thrift.test.compat.ListStructV2;
import org.apache.parquet.thrift.test.compat.MapAddRequiredStructV1;
import org.apache.parquet.thrift.test.compat.MapStructV1;
import org.apache.parquet.thrift.test.compat.MapStructV2;
import org.apache.parquet.thrift.test.compat.MapValueStructV1;
import org.apache.parquet.thrift.test.compat.MapValueStructV2;
import org.apache.parquet.thrift.test.compat.NestedEmptyStruct;
import org.apache.parquet.thrift.test.compat.OptionalStructV1;
import org.apache.parquet.thrift.test.compat.RenameStructV1;
import org.apache.parquet.thrift.test.compat.SetStructV1;
import org.apache.parquet.thrift.test.compat.SetStructV2;
import org.apache.parquet.thrift.test.compat.StructV1;
import org.apache.parquet.thrift.test.compat.StructV2;
import org.apache.parquet.thrift.test.compat.TypeChangeStructV1;
import org.junit.Test;

public class CompatibilityCheckerTest {

  /**
   * Adding optional field is compatible
   */
  @Test
  public void testAddOptionalField() {
    verifyCompatible(StructV1.class, StructV2.class, true);
  }

  /**
   * removing field is incompatible
   */
  @Test
  public void testRemoveOptionalField() {
    verifyCompatible(StructV2.class, StructV1.class, false);
  }

  /**
   * renaming field is incompatible
   */
  @Test
  public void testRenameField() {
    verifyCompatible(StructV1.class, RenameStructV1.class, false);
  }

  /**
   * changing field type is incompatible
   */
  @Test
  public void testTypeChange() {
    verifyCompatible(StructV1.class, TypeChangeStructV1.class, false);
  }

  /**
   * Making requirement more restrictive is incompatible
   */
  @Test
  public void testReuirementChange() {
    // required can become optional or default
    verifyCompatible(StructV1.class, OptionalStructV1.class, true);
    verifyCompatible(StructV1.class, DefaultStructV1.class, true);

    // optional/deafult can not become required
    verifyCompatible(OptionalStructV1.class, StructV1.class, false);
    verifyCompatible(DefaultStructV1.class, StructV1.class, false);
  }

  /**
   * Adding required field is incompatible
   */
  @Test
  public void testAddRequiredField() {
    verifyCompatible(StructV1.class, AddRequiredStructV1.class, false);
  }

  @Test
  public void testMap() {
    // can add optional field
    verifyCompatible(MapStructV1.class, MapStructV2.class, true);
    verifyCompatible(MapValueStructV1.class, MapValueStructV2.class, true);

    // should not delete field
    verifyCompatible(MapStructV2.class, MapStructV1.class, false);
    verifyCompatible(MapValueStructV2.class, MapValueStructV1.class, false);

    // should not add required field
    verifyCompatible(MapStructV2.class, MapAddRequiredStructV1.class, false);
  }

  @Test
  public void testSet() {
    verifyCompatible(SetStructV2.class, SetStructV1.class, false);
    verifyCompatible(SetStructV1.class, SetStructV2.class, true);
  }

  @Test
  public void testList() {
    verifyCompatible(ListStructV2.class, ListStructV1.class, false);
    verifyCompatible(ListStructV1.class, ListStructV2.class, true);
  }

  @Test
  public void testEmptyStruct() {
    CompatibilityReport report = getCompatibilityReport(NestedEmptyStruct.class, NestedEmptyStruct.class);
    assertEquals(
        "encountered an empty struct: required_empty\nencountered an empty struct: optional_empty",
        report.prettyMessages());
    assertTrue(report.hasEmptyStruct());
  }

  private ThriftType.StructType struct(Class thriftClass) {
    return ThriftSchemaConverter.toStructType(thriftClass);
  }

  private CompatibilityReport getCompatibilityReport(Class oldClass, Class newClass) {
    CompatibilityChecker checker = new CompatibilityChecker();
    CompatibilityReport report = checker.checkCompatibility(struct(oldClass), struct(newClass));
    return report;
  }

  private void verifyCompatible(Class oldClass, Class newClass, boolean expectCompatible) {
    CompatibilityReport report = getCompatibilityReport(oldClass, newClass);
    assertEquals(expectCompatible, report.isCompatible());
  }
}
