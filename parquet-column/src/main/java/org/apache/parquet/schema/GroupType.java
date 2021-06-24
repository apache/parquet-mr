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

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.parquet.io.InvalidRecordException;

/**
 * Represents a group type: a list of fields
 */
public class GroupType extends Type {

  private final List<Type> fields;
  private final Map<String, Integer> indexByName;

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param fields the contained fields
   */
  public GroupType(Repetition repetition, String name, List<Type> fields) {
    this(repetition, name, (LogicalTypeAnnotation) null, fields, null);
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param fields the contained fields
   */
  public GroupType(Repetition repetition, String name, Type... fields) {
    this(repetition, name, Arrays.asList(fields));
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   * @param fields the contained fields
   */
  @Deprecated
  public GroupType(Repetition repetition, String name, OriginalType originalType, Type... fields) {
    this(repetition, name, originalType, Arrays.asList(fields));
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param logicalTypeAnnotation (optional) the logical type to help with cross schema conversion (LIST, MAP, ...)
   * @param fields the contained fields
   */
  GroupType(Repetition repetition, String name, LogicalTypeAnnotation logicalTypeAnnotation, Type... fields) {
    this(repetition, name, logicalTypeAnnotation, Arrays.asList(fields));
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   * @param fields the contained fields
   */
  @Deprecated
  public GroupType(Repetition repetition, String name, OriginalType originalType, List<Type> fields) {
    this(repetition, name, originalType, fields, null);
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param logicalTypeAnnotation (optional) the logical type to help with cross schema conversion (LIST, MAP, ...)
   * @param fields the contained fields
   */
  GroupType(Repetition repetition, String name, LogicalTypeAnnotation logicalTypeAnnotation, List<Type> fields) {
    this(repetition, name, logicalTypeAnnotation, fields, null);
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   * @param fields the contained fields
   * @param id the id of the field
   */
  GroupType(Repetition repetition, String name, OriginalType originalType, List<Type> fields, ID id) {
    super(name, repetition, originalType, id);
    this.fields = fields;
    this.indexByName = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      indexByName.put(fields.get(i).getName(), i);
    }
  }

  GroupType(Repetition repetition, String name, LogicalTypeAnnotation logicalTypeAnnotation, List<Type> fields, ID id) {
    super(name, repetition, logicalTypeAnnotation, id);
    this.fields = fields;
    this.indexByName = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      indexByName.put(fields.get(i).getName(), i);
    }
  }

  /**
   * @param id the field id
   * @return a new GroupType with the same fields and a new id
   */
  @Override
  public GroupType withId(int id) {
    return new GroupType(getRepetition(), getName(), getLogicalTypeAnnotation(), fields, new ID(id));
  }

  /**
   * @param newFields a list of types to use as fields in a copy of this group
   * @return a group with the same attributes and new fields.
   */
  public GroupType withNewFields(List<Type> newFields) {
    return new GroupType(getRepetition(), getName(), getLogicalTypeAnnotation(), newFields, getId());
  }

  /**
   * @param newFields an array of types to use as fields in a copy of this group
   * @return a group with the same attributes and new fields.
   */
  public GroupType withNewFields(Type... newFields) {
    return withNewFields(asList(newFields));
  }

  /**
   * returns the name of the corresponding field
   * @param index the index of the desired field in this type
   * @return the name of the field at this index
   */
  public String getFieldName(int index) {
    return fields.get(index).getName();
  }

  /**
   * @param name the requested name
   * @return whether this type contains a field with that name
   */
  public boolean containsField(String name) {
    return indexByName.containsKey(name);
  }

  /**
   *
   * @param name string name of a field
   * @return the index of the field with that name
   */
  public int getFieldIndex(String name) {
    Integer i = indexByName.get(name);
    if (i == null) {
      throw new InvalidRecordException(name + " not found in " + this);
    }
    return i.intValue();
  }

  /**
   * @return the fields contained in this type
   */
  public List<Type> getFields() {
    return fields;
  }

  /**
   * @return the number of fields in this type
   */
  public int getFieldCount() {
    return fields.size();
  }

  /**
   * @return false
   */
  @Override
  public boolean isPrimitive() {
    return false;
  }

  /**
   * @param fieldName string name of a field
   * @return the type of this field by name
   */
  public Type getType(String fieldName) {
    return getType(getFieldIndex(fieldName));
  }

  /**
   * @param index integer index of a field
   * @return the type of this field by index
   */
  public Type getType(int index) {
    return fields.get(index);
  }

  /**
   * appends a display string for of the members of this group to sb
   * @param sb where to append
   * @param indent the indentation level
   */
  void membersDisplayString(StringBuilder sb, String indent) {
    for (Type field : fields) {
      field.writeToStringBuilder(sb, indent);
      if (field.isPrimitive()) {
        sb.append(";");
      }
      sb.append("\n");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeToStringBuilder(StringBuilder sb, String indent) {
    sb.append(indent)
        .append(getRepetition().name().toLowerCase(Locale.ENGLISH))
        .append(" group ")
        .append(getName())
        .append(getLogicalTypeAnnotation() == null ? "" : " (" + getLogicalTypeAnnotation().toString() +")")
        .append(getId() == null ? "" : " = " + getId())
        .append(" {\n");
    membersDisplayString(sb, indent + "  ");
    sb.append(indent)
        .append("}");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

  @Override @Deprecated
  protected int typeHashCode() {
    return hashCode();
  }

  @Override @Deprecated
  protected boolean typeEquals(Type other) {
    return equals(other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return Objects.hash(getLogicalTypeAnnotation(), getFields());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean equals(Type otherType) {
    return
        !otherType.isPrimitive()
        && super.equals(otherType)
        && Objects.equals(getLogicalTypeAnnotation(),otherType.getLogicalTypeAnnotation())
        && getFields().equals(otherType.asGroupType().getFields());
  }

  @Override
  protected int getMaxRepetitionLevel(String[] path, int depth) {
    int myVal = isRepetition(Repetition.REPEATED) ? 1 : 0;
    if (depth == path.length) {
      return myVal;
    }
    return myVal + getType(path[depth]).getMaxRepetitionLevel(path, depth + 1);
  }

  @Override
  protected int getMaxDefinitionLevel(String[] path, int depth) {
    int myVal = !isRepetition(Repetition.REQUIRED) ? 1 : 0;
    if (depth == path.length) {
      return myVal;
    }
    return myVal + getType(path[depth]).getMaxDefinitionLevel(path, depth + 1);
  }

  @Override
  protected Type getType(String[] path, int depth) {
    if (depth == path.length) {
      return this;
    }
    return getType(path[depth]).getType(path, depth + 1);
  }

  @Override
  protected boolean containsPath(String[] path, int depth) {
    if (depth == path.length) {
      return false;
    }
    return containsField(path[depth]) && getType(path[depth]).containsPath(path, depth + 1);
  }

  @Override
  protected List<String[]> getPaths(int depth) {
    List<String[]> result = new ArrayList<>();
    for (Type field : fields) {
      List<String[]> paths = field.getPaths(depth + 1);
      for (String[] path : paths) {
        path[depth] = field.getName();
        result.add(path);
      }
    }
    return result;
  }

  @Override
  void checkContains(Type subType) {
    super.checkContains(subType);
    checkGroupContains(subType);
  }

  @Override
  void checkCompatibility(Type subType) {
    super.checkCompatibility(subType);
    checkGroupCompatibility(subType);
  }

  void checkGroupContains(Type subType) {
    if (subType.isPrimitive()) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
    List<Type> fields = subType.asGroupType().getFields();
    for (Type otherType : fields) {
      Type thisType = this.getType(otherType.getName());
      thisType.checkContains(otherType);
    }
  }

  /**
   * Checks compatibility with given subType
   * Allows subType to have extra optional fields
   *
   * @param subType
   */
  void checkGroupCompatibility(Type subType) {
    if (subType.isPrimitive()) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
    List<Type> fields = subType.asGroupType().getFields();
    for (Type otherType : fields) {
      if(this.containsField(otherType.getName())) {
        Type thisType = this.getType(otherType.getName());
        thisType.checkCompatibility(otherType);
      } else if (otherType.getRepetition() == Repetition.REQUIRED) {
        throw new InvalidRecordException(otherType.getName() + " not found in " + this);
      }
    }
  }


  @Override
  <T> T convert(List<GroupType> path, TypeConverter<T> converter) {
    List<GroupType> childrenPath = new ArrayList<>(path);
    childrenPath.add(this);
    final List<T> children = convertChildren(childrenPath, converter);
    return converter.convertGroupType(path, this, children);
  }

  protected <T> List<T> convertChildren(List<GroupType> path, TypeConverter<T> converter) {
    List<T> children = new ArrayList<>(fields.size());
    for (Type field : fields) {
      children.add(field.convert(path, converter));
    }
    return children;
  }

  @Override
  protected Type union(Type toMerge) {
    return union(toMerge, true);
  }

  @Override
  protected Type union(Type toMerge, boolean strict) {
    if (toMerge.isPrimitive()) {
      throw new IncompatibleSchemaModificationException("can not merge primitive type " + toMerge + " into group type " + this);
    }
    return new GroupType(toMerge.getRepetition(), getName(), toMerge.getLogicalTypeAnnotation(), mergeFields(toMerge.asGroupType()), getId());
  }

  /**
   * produces the list of fields resulting from merging toMerge into the fields of this
   * @param toMerge the group containing the fields to merge
   * @return the merged list
   */
  List<Type> mergeFields(GroupType toMerge) {
    return mergeFields(toMerge, true);
  }

  /**
   * produces the list of fields resulting from merging toMerge into the fields of this
   * @param toMerge the group containing the fields to merge
   * @param strict should schema primitive types match
   * @return the merged list
   */
  List<Type> mergeFields(GroupType toMerge, boolean strict) {
    List<Type> newFields = new ArrayList<>();
    // merge existing fields
    for (Type type : this.getFields()) {
      Type merged;
      if (toMerge.containsField(type.getName())) {
        Type fieldToMerge = toMerge.getType(type.getName());
        if (type.getLogicalTypeAnnotation() != null && !type.getLogicalTypeAnnotation().equals(fieldToMerge.getLogicalTypeAnnotation())) {
          throw new IncompatibleSchemaModificationException("cannot merge logical type " + fieldToMerge.getLogicalTypeAnnotation() + " into " + type.getLogicalTypeAnnotation());
        }
        merged = type.union(fieldToMerge, strict);
      } else {
        merged = type;
      }
      newFields.add(merged);
    }
    // add new fields
    for (Type type : toMerge.getFields()) {
      if (!this.containsField(type.getName())) {
        newFields.add(type);
      }
    }
    return newFields;
  }
}
