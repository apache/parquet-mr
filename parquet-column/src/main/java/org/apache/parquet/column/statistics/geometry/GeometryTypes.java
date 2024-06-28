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
package org.apache.parquet.column.statistics.geometry;

import java.util.HashSet;
import java.util.Set;
import org.apache.parquet.Preconditions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class GeometryTypes {

  private static final int UNKNOWN_TYPE_ID = -1;
  private Set<Integer> types = new HashSet<>();
  private boolean valid = true;

  public GeometryTypes(Set<Integer> types) {
    this.types = types;
  }

  public GeometryTypes() {}

  public Set<Integer> getTypes() {
    return types;
  }

  void update(Geometry geometry) {
    if (!valid) {
      return;
    }
    int code = getGeometryTypeCode(geometry);
    if (code != UNKNOWN_TYPE_ID) {
      types.add(code);
    } else {
      valid = false;
      types.clear();
    }
  }

  public void merge(GeometryTypes other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null GeometryTypes");
    if (!valid) {
      return;
    }
    if (!other.valid) {
      valid = false;
      types.clear();
      return;
    }
    types.addAll(other.types);
  }

  public void reset() {
    types.clear();
    valid = true;
  }

  public void abort() {
    valid = false;
    types.clear();
  }

  public GeometryTypes copy() {
    return new GeometryTypes(new HashSet<>(types));
  }

  @Override
  public String toString() {
    // TODO: Print the geometry types as strings
    return "GeometryTypes{" + "types=" + types + '}';
  }

  private int getGeometryTypeId(Geometry geometry) {
    switch (geometry.getGeometryType()) {
      case Geometry.TYPENAME_POINT:
        return 1;
      case Geometry.TYPENAME_LINESTRING:
        return 2;
      case Geometry.TYPENAME_POLYGON:
        return 3;
      case Geometry.TYPENAME_MULTIPOINT:
        return 4;
      case Geometry.TYPENAME_MULTILINESTRING:
        return 5;
      case Geometry.TYPENAME_MULTIPOLYGON:
        return 6;
      case Geometry.TYPENAME_GEOMETRYCOLLECTION:
        return 7;
      default:
        return UNKNOWN_TYPE_ID;
    }
  }

  private int getGeometryTypeCode(Geometry geometry) {
    int typeId = getGeometryTypeId(geometry);
    if (typeId == UNKNOWN_TYPE_ID) {
      return UNKNOWN_TYPE_ID;
    }
    Coordinate coordinate = geometry.getCoordinate();
    if (!Double.isNaN(coordinate.getZ())) {
      typeId += 1000;
    }
    if (!Double.isNaN(coordinate.getM())) {
      typeId += 2000;
    }
    return typeId;
  }
}
