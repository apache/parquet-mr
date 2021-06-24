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

import java.util.List;
import java.util.Objects;

import org.apache.parquet.io.InvalidRecordException;

/**
 * Represents the declared type for a field in a schema.
 * The Type object represents both the actual underlying type of the object
 * (eg a primitive or group) as well as its attributes such as whether it is
 * repeated, required, or optional.
 */
abstract public class Type {

  /**
   * represents a field ID
   */
  public static final class ID {
    private final int id;

    public ID(int id) {
      this.id = id;
    }

    /**
     * For bean serialization, used by Cascading 3.
     * @return this type's id
     * @deprecated use {@link #intValue()} instead.
     */
    @Deprecated
    public int getId() {
      return id;
    }

    public int intValue() {
      return id;
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof ID) && ((ID)obj).id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public String toString() {
      return String.valueOf(id);
    }
  }

  /**
   * Constraint on the repetition of a field
   */
  public static enum Repetition {
    /**
     * exactly 1
     */
    REQUIRED {
      @Override
      public boolean isMoreRestrictiveThan(Repetition other) {
        return other != REQUIRED;
      }
    },
    /**
     * 0 or 1
     */
    OPTIONAL {
      @Override
      public boolean isMoreRestrictiveThan(Repetition other) {
        return other == REPEATED;
      }
    },
    /**
     * 0 or more
     */
    REPEATED {
      @Override
      public boolean isMoreRestrictiveThan(Repetition other) {
        return false;
      }
    }
    ;

    /**
     * @param other a repetition to test
     * @return true if it is strictly more restrictive than other
     */
    abstract public boolean isMoreRestrictiveThan(Repetition other);


    /**
     * @param repetitions repetitions to traverse
     * @return the least restrictive repetition of all repetitions provided
     */
    public static Repetition leastRestrictive(Repetition... repetitions) {
      boolean hasOptional = false;

      for (Repetition repetition : repetitions) {
        if (repetition == REPEATED) {
          return REPEATED;
        } else if (repetition == OPTIONAL) {
          hasOptional = true;
        }
      }

      if (hasOptional) {
        return OPTIONAL;
      }

      return REQUIRED;
    }
  }

  private final String name;
  private final Repetition repetition;
  private final LogicalTypeAnnotation logicalTypeAnnotation;
  private final ID id;

  /**
   * @param name the name of the type
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   */
  @Deprecated
  public Type(String name, Repetition repetition) {
    this(name, repetition, (LogicalTypeAnnotation) null, null);
  }

  /**
   * @param name the name of the type
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   */
  @Deprecated
  public Type(String name, Repetition repetition, OriginalType originalType) {
    this(name, repetition, originalType, null);
  }

  /**
   * @param name the name of the type
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   * @param id (optional) the id of the fields.
   */
  Type(String name, Repetition repetition, OriginalType originalType, ID id) {
    this(name, repetition, originalType, null, id);
  }

  Type(String name, Repetition repetition, OriginalType originalType, DecimalMetadata decimalMetadata, ID id) {
    super();
    this.name = Objects.requireNonNull(name, "name cannot be null");
    this.repetition = Objects.requireNonNull(repetition, "repetition cannot be null");
    this.logicalTypeAnnotation = originalType == null ? null : LogicalTypeAnnotation.fromOriginalType(originalType, decimalMetadata);
    this.id = id;
  }

  Type(String name, Repetition repetition, LogicalTypeAnnotation logicalTypeAnnotation) {
    this(name, repetition, logicalTypeAnnotation, null);
  }

  Type(String name, Repetition repetition, LogicalTypeAnnotation logicalTypeAnnotation, ID id) {
    super();
    this.name = Objects.requireNonNull(name, "name cannot be null");
    this.repetition = Objects.requireNonNull(repetition, "repetition cannot be null");
    this.logicalTypeAnnotation = logicalTypeAnnotation;
    this.id = id;
  }

  /**
   * @param id an integer id
   * @return the same type with the id field set
   */
  public abstract Type withId(int id);

  /**
   * @return the name of the type
   */
  public String getName() {
    return name;
  }

  /**
   * @param rep repetition level to test
   * @return if repetition of the type is rep
   */
  public boolean isRepetition(Repetition rep) {
    return repetition == rep;
  }

  /**
   * @return the repetition constraint
   */
  public Repetition getRepetition() {
    return repetition;
  }

  /**
   * @return the id of the field (if defined)
   */
  public ID getId() {
    return id;
  }

  public LogicalTypeAnnotation getLogicalTypeAnnotation() {
    return logicalTypeAnnotation;
  }

  /**
   * @return the original type (LIST, MAP, ...)
   */
  public OriginalType getOriginalType() {
    return logicalTypeAnnotation == null ? null : logicalTypeAnnotation.toOriginalType();
  }

  /**
   * @return if this is a primitive type
   */
  abstract public boolean isPrimitive();

  /**
   * @return this if it's a group type
   * @throws ClassCastException if not
   */
  public GroupType asGroupType() {
    if (isPrimitive()) {
      throw new ClassCastException(this + " is not a group");
    }
    return (GroupType)this;
  }

  /**
   * @return this if it's a primitive type
   * @throws ClassCastException if not
   */
  public PrimitiveType asPrimitiveType() {
    if (!isPrimitive()) {
      throw new ClassCastException(this + " is not primitive");
    }
    return (PrimitiveType)this;
  }

  /**
   * Writes a string representation to the provided StringBuilder
   * @param sb the StringBuilder to write itself to
   * @param indent indentation level
   */
  abstract public void writeToStringBuilder(StringBuilder sb, String indent);

  /**
   * Visits this type with the given visitor
   * @param visitor the visitor to visit this type
   */
  abstract public void accept(TypeVisitor visitor);

  @Deprecated
  abstract protected int typeHashCode();

  @Deprecated
  abstract protected boolean typeEquals(Type other);

  @Override
  public int hashCode() {
    int c = repetition.hashCode();
    c = 31 * c + name.hashCode();
    if (logicalTypeAnnotation != null) {
      c = 31 * c +  logicalTypeAnnotation.hashCode();
    }
    if (id != null) {
      c = 31 * c + id.hashCode();
    }
    return c;
  }

  protected boolean equals(Type other) {
    return
        name.equals(other.name)
        && repetition == other.repetition
        && eqOrBothNull(repetition, other.repetition)
        && eqOrBothNull(id, other.id)
        && eqOrBothNull(logicalTypeAnnotation, other.logicalTypeAnnotation);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Type) || other == null) {
      return false;
    }
    return equals((Type)other);
  }

  protected boolean eqOrBothNull(Object o1, Object o2) {
    return (o1 == null && o2 == null) || (o1 != null && o1.equals(o2));
  }

  protected abstract int getMaxRepetitionLevel(String[] path, int i);

  protected abstract int getMaxDefinitionLevel(String[] path, int i);

  protected abstract Type getType(String[] path, int i);

  protected abstract List<String[]> getPaths(int depth);

  protected abstract boolean containsPath(String[] path, int depth);

  /**
   * @param toMerge the type to merge into this one
   * @return the union result of merging toMerge into this
   */
  protected abstract Type union(Type toMerge);

  /**
   * @param toMerge the type to merge into this one
   * @param strict should schema primitive types match
   * @return the union result of merging toMerge into this
   */
  protected abstract Type union(Type toMerge, boolean strict);

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    writeToStringBuilder(sb, "");
    return sb.toString();
  }

  void checkContains(Type subType) {
    if (!this.name.equals(subType.name)
        || this.repetition != subType.repetition) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
  }

  /**
   * Checks compatibility with subType
   * Allows subtype to have a less strict repetition
   * For eg: a REQUIRED type is compatible with an OPTIONAL subType
   *
   * @param subType
   */
  void checkCompatibility(Type subType) {
    if (!this.name.equals(subType.name)
      || (this.repetition != subType.repetition && !this.repetition.isMoreRestrictiveThan(subType.repetition))) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
  }


  /**
   *
   * @param path a list of groups to convert
   * @param converter logic to convert the tree
   * @param <T> the type returned by the converter
   * @return the converted tree
   */
   abstract <T> T convert(List<GroupType> path, TypeConverter<T> converter);

}
