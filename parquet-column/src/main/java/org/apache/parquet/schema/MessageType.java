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
package org.apache.parquet.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.InvalidRecordException;

/**
 * The root of a schema
 */
public final class MessageType extends GroupType {

  /**
   *
   * @param name the name of the type
   * @param fields the fields contained by this message
   */
  public MessageType(String name, Type... fields) {
    super(Repetition.REPEATED, name, fields);
  }

 /**
  *
  * @param name the name of the type
  * @param fields the fields contained by this message
  */
 public MessageType(String name, List<Type> fields) {
   super(Repetition.REPEATED, name, fields);
 }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeToStringBuilder(StringBuilder sb, String indent) {
    sb.append("message ")
        .append(getName())
        .append(getLogicalTypeAnnotation() == null ? "" : " (" + getLogicalTypeAnnotation().toString() +")")
        .append(" {\n");
    membersDisplayString(sb, "  ");
    sb.append("}\n");
  }

  /**
   * @param path an array of strings representing the name path in this type
   * @return the max repetition level that might be needed to encode the
   * type at 'path'.
   */
  public int getMaxRepetitionLevel(String ... path) {
    return getMaxRepetitionLevel(path, 0) - 1;
  }

  /**
   * @param path an array of strings representing the name path in this type
   * @return the max repetition level that might be needed to encode the
   * type at 'path'.
   */
  public int getMaxDefinitionLevel(String ... path) {
    return getMaxDefinitionLevel(path, 0) - 1;
  }

  public Type getType(String ... path) {
    return getType(path, 0);
  }

  public ColumnDescriptor getColumnDescription(String[] path) {
    int maxRep = getMaxRepetitionLevel(path);
    int maxDef = getMaxDefinitionLevel(path);
    PrimitiveType type = getType(path).asPrimitiveType();
    return new ColumnDescriptor(path, type, maxRep, maxDef);
  }

  public List<String[]> getPaths() {
    return this.getPaths(0);
  }

  public List<ColumnDescriptor> getColumns() {
    List<String[]> paths = this.getPaths(0);
    List<ColumnDescriptor> columns = new ArrayList<>(paths.size());
    for (String[] path : paths) {
      // TODO: optimize this
      PrimitiveType primitiveType = getType(path).asPrimitiveType();
      columns.add(new ColumnDescriptor(
                      path,
                      primitiveType,
                      getMaxRepetitionLevel(path),
                      getMaxDefinitionLevel(path)));
    }
    return columns;
  }

  @Override
  public void checkContains(Type subType) {
    if (!(subType instanceof MessageType)) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
    checkGroupContains(subType);
  }

  @Override
  public void checkCompatibility(Type subType) {
    if (!(subType instanceof MessageType)) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
    checkGroupCompatibility(subType);
  }


  public <T> T convertWith(TypeConverter<T> converter) {
    final ArrayList<GroupType> path = new ArrayList<>();
    path.add(this);
    return converter.convertMessageType(this, convertChildren(path, converter));
  }

  public boolean containsPath(String[] path) {
    return containsPath(path, 0);
  }

  public MessageType union(MessageType toMerge) {
    return union(toMerge, true);
  }

  public MessageType union(MessageType toMerge, boolean strict) {
    return new MessageType(this.getName(), mergeFields(toMerge, strict));
  }

}
