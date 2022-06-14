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
package org.apache.parquet.column;

import java.util.Arrays;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/**
 * Describes a column's type as well as its position in its containing schema.
 */
public class ColumnDescriptor implements Comparable<ColumnDescriptor> {

  private final String[] path;
  private final Type.ID id;
  private final PrimitiveType type;
  private final int maxRep;
  private final int maxDef;

  /**
   *
   * @param path the path to the leaf field in the schema
   * @param type the type of the field
   * @param maxRep the maximum repetition level for that path
   * @param maxDef the maximum definition level for that path
   * @deprecated will be removed in 2.0.0; Use {@link #ColumnDescriptor(String[], PrimitiveType, int, int)}
   */
  @Deprecated
  public ColumnDescriptor(String[] path, PrimitiveTypeName type, int maxRep,
                          int maxDef) {
    this(path, type, 0, maxRep, maxDef);
  }

  /**
   *
   * @param path the path to the leaf field in the schema
   * @param type the type of the field
   * @param typeLength the length of the type, if type is a fixed-length byte array
   * @param maxRep the maximum repetition level for that path
   * @param maxDef the maximum definition level for that path
   * @deprecated will be removed in 2.0.0; Use {@link #ColumnDescriptor(String[], PrimitiveType, int, int)}
   */
  @Deprecated
  public ColumnDescriptor(String[] path, PrimitiveTypeName type,
                          int typeLength, int maxRep, int maxDef) {
    this(path, new PrimitiveType(Type.Repetition.OPTIONAL, type, typeLength,""), maxRep, maxDef);
  }

  /**
   * @param path the path to the leaf field in the schema
   * @param type the type of the field
   * @param maxRep the maximum repetition level for that path
   * @param maxDef the maximum definition level for that path
   */
  public ColumnDescriptor(String[] path, PrimitiveType type, int maxRep, int maxDef) {
    this(path, null, type, maxRep, maxDef);
  }

  /**
   * @param path the path to the leaf field in the schema
   * @param id the id to the leaf field in the schema
   * @param type the type of the field
   * @param maxRep the maximum repetition level for that path
   * @param maxDef the maximum definition level for that path
   * @deprecated will be removed in 2.0.0; Use {@link #ColumnDescriptor(String[], PrimitiveType, int, int)}
   */
  public ColumnDescriptor(String[] path, Type.ID id, PrimitiveType type, int maxRep, int maxDef) {
    this.path = path;
    this.id = id;
    this.type = type;
    this.maxRep = maxRep;
    this.maxDef = maxDef;
  }

  /**
   * @return the path to the leaf field in the schema
   */
  public String[] getPath() {
    return path;
  }

  public Type.ID getId() {
    return id;
  }

  /**
   * @return the maximum repetition level for that path
   */
  public int getMaxRepetitionLevel() {
    return maxRep;
  }

  /**
   * @return  the maximum definition level for that path
   */
  public int getMaxDefinitionLevel() {
    return maxDef;
  }

  /**
   * @return the type of that column
   * @deprecated will removed in 2.0.0. Use {@link #getPrimitiveType()} instead.
   */
  @Deprecated
  public PrimitiveTypeName getType() {
    return type.getPrimitiveTypeName();
  }

  /**
   * @return the size of the type
   * @deprecated will removed in 2.0.0. Use {@link #getPrimitiveType()} instead.
   **/
  @Deprecated
  public int getTypeLength() {
    return type.getTypeLength();
  }

  /**
   * @return the primitive type object of the column
   */
  public PrimitiveType getPrimitiveType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(path);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (!(other instanceof ColumnDescriptor))
      return false;
    ColumnDescriptor descriptor = (ColumnDescriptor) other;
    return Arrays.equals(path, descriptor.path);
  }

  @Override
  public int compareTo(ColumnDescriptor o) {
    int length = path.length < o.path.length ? path.length : o.path.length;
    for (int i = 0; i < length; i++) {
      int compareTo = path[i].compareTo(o.path[i]);
      if (compareTo != 0) {
        return compareTo;
      }
    }
    return path.length - o.path.length;
  }

  @Override
  public String toString() {
    return Arrays.toString(path) + " " + type;
  }
}
