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

import java.nio.ByteBuffer;
import org.apache.parquet.Preconditions;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class Covering {

  protected final LogicalTypeAnnotation.Edges edges;
  protected ByteBuffer geometry;

  public Covering(ByteBuffer geometry, LogicalTypeAnnotation.Edges edges) {
    Preconditions.checkArgument(geometry != null, "Geometry cannot be null");
    Preconditions.checkArgument(edges != null, "Edges cannot be null");
    this.geometry = geometry;
    this.edges = edges;
  }

  public ByteBuffer getGeometry() {
    return geometry;
  }

  public LogicalTypeAnnotation.Edges getEdges() {
    return edges;
  }

  void update(Geometry geom) {
    throw new UnsupportedOperationException(
        "Update is not supported for " + this.getClass().getSimpleName());
  }

  public void merge(Covering other) {
    throw new UnsupportedOperationException(
        "Merge is not supported for " + this.getClass().getSimpleName());
  }

  public void reset() {
    throw new UnsupportedOperationException(
        "Reset is not supported for " + this.getClass().getSimpleName());
  }

  public void abort() {
    throw new UnsupportedOperationException(
        "Abort is not supported for " + this.getClass().getSimpleName());
  }

  public Covering copy() {
    return new Covering(geometry.duplicate(), edges);
  }

  @Override
  public String toString() {
    String geomText;
    try {
      geomText = new WKBReader().read(geometry.array()).toText();
    } catch (ParseException e) {
      geomText = "Invalid Geometry";
    }

    return "Covering{" + "geometry=" + geomText + ", edges=" + edges + '}';
  }
}
