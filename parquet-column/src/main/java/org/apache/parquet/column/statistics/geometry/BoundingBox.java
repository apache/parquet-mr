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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class BoundingBox {

  private double xMin = Double.MAX_VALUE;
  private double xMax = Double.MIN_VALUE;
  private double yMin = Double.MAX_VALUE;
  private double yMax = Double.MIN_VALUE;
  private double zMin = Double.MAX_VALUE;
  private double zMax = Double.MIN_VALUE;
  private double mMin = Double.MAX_VALUE;
  private double mMax = Double.MIN_VALUE;

  public BoundingBox(
      double xMin, double xMax, double yMin, double yMax, double zMin, double zMax, double mMin, double mMax) {
    this.xMin = xMin;
    this.xMax = xMax;
    this.yMin = yMin;
    this.yMax = yMax;
    this.zMin = zMin;
    this.zMax = zMax;
    this.mMin = mMin;
    this.mMax = mMax;
  }

  public BoundingBox() {}

  public double getXMin() {
    return xMin;
  }

  public double getXMax() {
    return xMax;
  }

  public double getYMin() {
    return yMin;
  }

  public double getYMax() {
    return yMax;
  }

  public double getZMin() {
    return zMin;
  }

  public double getZMax() {
    return zMax;
  }

  public double getMMin() {
    return mMin;
  }

  public double getMMax() {
    return mMax;
  }

  void update(Geometry geom) {
    if (geom == null || geom.isEmpty()) {
      return;
    }
    Coordinate[] coordinates = geom.getCoordinates();
    for (Coordinate coordinate : coordinates) {
      update(coordinate.getX(), coordinate.getY(), coordinate.getZ(), coordinate.getM());
    }
  }

  public void update(double x, double y, double z, double m) {
    xMin = Math.min(xMin, x);
    xMax = Math.max(xMax, x);
    yMin = Math.min(yMin, y);
    yMax = Math.max(yMax, y);
    zMin = Math.min(zMin, z);
    zMax = Math.max(zMax, z);
    mMin = Math.min(mMin, m);
    mMax = Math.max(mMax, m);
  }

  public void merge(BoundingBox other) {
    Preconditions.checkArgument(other != null, "Cannot merge with null bounding box");
    xMin = Math.min(xMin, other.xMin);
    xMax = Math.max(xMax, other.xMax);
    yMin = Math.min(yMin, other.yMin);
    yMax = Math.max(yMax, other.yMax);
    zMin = Math.min(zMin, other.zMin);
    zMax = Math.max(zMax, other.zMax);
    mMin = Math.min(mMin, other.mMin);
    mMax = Math.max(mMax, other.mMax);
  }

  public void reset() {
    xMin = Double.MAX_VALUE;
    xMax = Double.MIN_VALUE;
    yMin = Double.MAX_VALUE;
    yMax = Double.MIN_VALUE;
    zMin = Double.MAX_VALUE;
    zMax = Double.MIN_VALUE;
    mMin = Double.MAX_VALUE;
    mMax = Double.MIN_VALUE;
  }

  public void abort() {
    xMin = Double.NaN;
    xMax = Double.NaN;
    yMin = Double.NaN;
    yMax = Double.NaN;
    zMin = Double.NaN;
    zMax = Double.NaN;
    mMin = Double.NaN;
    mMax = Double.NaN;
  }

  public BoundingBox copy() {
    return new BoundingBox(xMin, xMax, yMin, yMax, zMin, zMax, mMin, mMax);
  }

  @Override
  public String toString() {
    return "BoundingBox{" + "xMin="
        + xMin + ", xMax="
        + xMax + ", yMin="
        + yMin + ", yMax="
        + yMax + ", zMin="
        + zMin + ", zMax="
        + zMax + ", mMin="
        + mMin + ", mMax="
        + mMax + '}';
  }
}
