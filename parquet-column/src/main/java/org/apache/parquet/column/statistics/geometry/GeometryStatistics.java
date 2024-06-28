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

import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class GeometryStatistics {

  private final BoundingBox boundingBox;
  private final Covering covering;
  private final GeometryTypes geometryTypes;
  private final WKBReader reader = new WKBReader();

  public GeometryStatistics(BoundingBox boundingBox, Covering covering, GeometryTypes geometryTypes) {
    this.boundingBox = boundingBox;
    this.covering = covering;
    this.geometryTypes = geometryTypes;
  }

  public GeometryStatistics() {
    this(new BoundingBox(), new EnvelopeCovering(), new GeometryTypes());
  }

  public BoundingBox getBoundingBox() {
    return boundingBox;
  }

  public Covering getCovering() {
    return covering;
  }

  public GeometryTypes getGeometryTypes() {
    return geometryTypes;
  }

  public void update(Binary value) {
    if (value == null) {
      return;
    }
    try {
      Geometry geom = reader.read(value.getBytes());
      update(geom);
    } catch (ParseException e) {
      abort();
    }
  }

  private void update(Geometry geom) {
    boundingBox.update(geom);
    covering.update(geom);
    geometryTypes.update(geom);
  }

  public void merge(GeometryStatistics other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null GeometryStatistics");
    boundingBox.merge(other.boundingBox);
    covering.merge(other.covering);
    geometryTypes.merge(other.geometryTypes);
  }

  public void reset() {
    boundingBox.reset();
    covering.reset();
    geometryTypes.reset();
  }

  public void abort() {
    boundingBox.abort();
    covering.abort();
    geometryTypes.abort();
  }

  public GeometryStatistics copy() {
    return new GeometryStatistics(boundingBox.copy(), covering.copy(), geometryTypes.copy());
  }

  @Override
  public String toString() {
    return "GeometryStatistics{" + "boundingBox="
        + boundingBox + ", covering="
        + covering + ", geometryTypes="
        + geometryTypes + '}';
  }
}
