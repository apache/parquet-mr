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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import org.apache.parquet.Preconditions;
import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;


/**
 * Representation of a Primitive type
 */
public final class PrimitiveType extends Type {

  public static interface PrimitiveTypeNameConverter<T, E extends Exception> {

    T convertFLOAT(PrimitiveTypeName primitiveTypeName) throws E;

    T convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws E;

    T convertINT32(PrimitiveTypeName primitiveTypeName) throws E;

    T convertINT64(PrimitiveTypeName primitiveTypeName) throws E;

    T convertINT96(PrimitiveTypeName primitiveTypeName) throws E;

    T convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) throws E;

    T convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws E;

    T convertBINARY(PrimitiveTypeName primitiveTypeName) throws E;

  }

  /**
   * Supported Primitive types
   */
  public static enum PrimitiveTypeName {
    INT64("getLong", Long.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getLong());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addLong(columnReader.getLong());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addLong(columnReader.getLong());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertINT64(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        if (logicalType == null) {
          return PrimitiveComparator.SIGNED_INT64_COMPARATOR;
        }
        return logicalType.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<PrimitiveComparator>() {
          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            return intLogicalType.isSigned() ?
              of(PrimitiveComparator.SIGNED_INT64_COMPARATOR) : of(PrimitiveComparator.UNSIGNED_INT64_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return of(PrimitiveComparator.SIGNED_INT64_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            return of(PrimitiveComparator.SIGNED_INT64_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            return of(PrimitiveComparator.SIGNED_INT64_COMPARATOR);
          }
        }).orElseThrow(() -> new ShouldNeverHappenException("No comparator logic implemented for INT64 logical type: " + logicalType));
      }
    },
    INT32("getInteger", Integer.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getInteger());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addInteger(columnReader.getInteger());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addInt(columnReader.getInteger());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertINT32(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        if (logicalType == null) {
          return PrimitiveComparator.SIGNED_INT32_COMPARATOR;
        }
        return logicalType.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<PrimitiveComparator>() {
          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
            if (intLogicalType.getBitWidth() == 64) {
              return empty();
            }
            return intLogicalType.isSigned() ?
              of(PrimitiveComparator.SIGNED_INT32_COMPARATOR) : of(PrimitiveComparator.UNSIGNED_INT32_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return of(PrimitiveComparator.SIGNED_INT32_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return of(PrimitiveComparator.SIGNED_INT32_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            if (timeLogicalType.getUnit() == MILLIS) {
              return of(PrimitiveComparator.SIGNED_INT32_COMPARATOR);
            }
            return empty();
          }
        }).orElseThrow(
          () -> new ShouldNeverHappenException("No comparator logic implemented for INT32 logical type: " + logicalType));
      }
    },
    BOOLEAN("getBoolean", Boolean.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBoolean());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBoolean(columnReader.getBoolean());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addBoolean(columnReader.getBoolean());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertBOOLEAN(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        return PrimitiveComparator.BOOLEAN_COMPARATOR;
      }
    },
    BINARY("getBinary", Binary.class) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBinary());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBinary(columnReader.getBinary());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addBinary(columnReader.getBinary());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertBINARY(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        if (logicalType == null) {
          return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        }
        return logicalType.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<PrimitiveComparator>() {
          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return of(PrimitiveComparator.BINARY_AS_SIGNED_INTEGER_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
            return of(PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return of(PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
            return of(PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
            return of(PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR);
          }
        }).orElseThrow(() -> new ShouldNeverHappenException("No comparator logic implemented for BINARY logical type: " + logicalType));
      }
    },
    FLOAT("getFloat", Float.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getFloat());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addFloat(columnReader.getFloat());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addFloat(columnReader.getFloat());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertFLOAT(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        return PrimitiveComparator.FLOAT_COMPARATOR;
      }
    },
    DOUBLE("getDouble", Double.TYPE) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getDouble());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addDouble(columnReader.getDouble());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addDouble(columnReader.getDouble());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertDOUBLE(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        return PrimitiveComparator.DOUBLE_COMPARATOR;
      }
    },
    INT96("getBinary", Binary.class) {
      @Override
      public String toString(ColumnReader columnReader) {
        return Arrays.toString(columnReader.getBinary().getBytesUnsafe());
      }
      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBinary(columnReader.getBinary());
      }
      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addBinary(columnReader.getBinary());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertINT96(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        return PrimitiveComparator.BINARY_AS_SIGNED_INTEGER_COMPARATOR;
      }
    },
    FIXED_LEN_BYTE_ARRAY("getBinary", Binary.class) {
      @Override
      public String toString(ColumnReader columnReader) {
        return String.valueOf(columnReader.getBinary());
      }

      @Override
      public void addValueToRecordConsumer(RecordConsumer recordConsumer,
          ColumnReader columnReader) {
        recordConsumer.addBinary(columnReader.getBinary());
      }

      @Override
      public void addValueToPrimitiveConverter(
          PrimitiveConverter primitiveConverter, ColumnReader columnReader) {
        primitiveConverter.addBinary(columnReader.getBinary());
      }

      @Override
      public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E {
        return converter.convertFIXED_LEN_BYTE_ARRAY(this);
      }

      @Override
      PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType) {
        if (logicalType == null) {
          return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        }

        return logicalType.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<PrimitiveComparator>() {
          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return of(PrimitiveComparator.BINARY_AS_SIGNED_INTEGER_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
            return of(PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR);
          }

          @Override
          public Optional<PrimitiveComparator> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
            return of(PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR);
          }
        }).orElseThrow(() -> new ShouldNeverHappenException(
          "No comparator logic implemented for FIXED_LEN_BYTE_ARRAY logical type: " + logicalType));
      }
    };

    public final String getMethod;
    public final Class<?> javaType;

    private PrimitiveTypeName(String getMethod, Class<?> javaType) {
      this.getMethod = getMethod;
      this.javaType = javaType;
    }

    /**
     * reads the value from the columnReader with the appropriate accessor and returns a String representation
     * @param columnReader where to read
     * @return a string
     */
    abstract public String toString(ColumnReader columnReader);

    /**
     * reads the value from the columnReader with the appropriate accessor and writes it to the recordConsumer
     * @param recordConsumer where to write
     * @param columnReader where to read from
     */
    abstract public void addValueToRecordConsumer(RecordConsumer recordConsumer,
        ColumnReader columnReader);

    abstract public void addValueToPrimitiveConverter(
        PrimitiveConverter primitiveConverter, ColumnReader columnReader);

    abstract public <T, E extends Exception> T convert(PrimitiveTypeNameConverter<T, E> converter) throws E;

    abstract PrimitiveComparator<?> comparator(LogicalTypeAnnotation logicalType);
  }

  private final PrimitiveTypeName primitive;
  private final int length;
  private final DecimalMetadata decimalMeta;
  private final ColumnOrder columnOrder;

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param primitive STRING, INT64, ...
   * @param name the name of the type
   */
  public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive, String name) {
    this(repetition, primitive, 0, name, (LogicalTypeAnnotation) null, null, null);
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param primitive STRING, INT64, ...
   * @param length the length if the type is FIXED_LEN_BYTE_ARRAY, 0 otherwise (XXX)
   * @param name the name of the type
   */
  public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive, int length, String name) {
    this(repetition, primitive, length, name, (LogicalTypeAnnotation) null, null, null);
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param primitive STRING, INT64, ...
   * @param name the name of the type
   * @param originalType (optional) the original type to help with cross schema convertion (LIST, MAP, ...)
   *
   * @deprecated will be removed in 2.0.0; use builders in {@link Types} instead
   */
  @Deprecated
  public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                       String name, OriginalType originalType) {
    this(repetition, primitive, 0, name, originalType, null, null);
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param primitive STRING, INT64, ...
   * @param name the name of the type
   * @param length the length if the type is FIXED_LEN_BYTE_ARRAY, 0 otherwise (XXX)
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   */
  @Deprecated
  public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                       int length, String name, OriginalType originalType) {
    this(repetition, primitive, length, name, originalType, null, null);
  }

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param primitive STRING, INT64, ...
   * @param name the name of the type
   * @param length the length if the type is FIXED_LEN_BYTE_ARRAY, 0 otherwise
   * @param originalType (optional) the original type (MAP, DECIMAL, UTF8, ...)
   * @param decimalMeta (optional) metadata about the decimal type
   * @param id the id of the field
   *
   * @deprecated will be removed in 2.0.0; use builders in {@link Types} instead
   */
  @Deprecated
  public PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                       int length, String name, OriginalType originalType,
                       DecimalMetadata decimalMeta, ID id) {
    this(repetition, primitive, length, name, originalType, decimalMeta, id, null);
  }

  PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
      int length, String name, OriginalType originalType,
      DecimalMetadata decimalMeta, ID id, ColumnOrder columnOrder) {
    super(name, repetition, originalType, decimalMeta, id);
    this.primitive = primitive;
    this.length = length;
    this.decimalMeta = decimalMeta;

    if (columnOrder == null) {
      columnOrder = primitive == PrimitiveTypeName.INT96 || originalType == OriginalType.INTERVAL
          ? ColumnOrder.undefined()
          : ColumnOrder.typeDefined();
    }
    this.columnOrder = requireValidColumnOrder(columnOrder);
  }

  PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                       String name, LogicalTypeAnnotation logicalTypeAnnotation) {
    this(repetition, primitive, 0, name, logicalTypeAnnotation, null, null);
  }

  PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                       int length, String name, LogicalTypeAnnotation logicalTypeAnnotation, ID id) {
    this(repetition, primitive, length, name, logicalTypeAnnotation, id, null);
  }

  PrimitiveType(Repetition repetition, PrimitiveTypeName primitive,
                int length, String name, LogicalTypeAnnotation logicalTypeAnnotation,
                ID id, ColumnOrder columnOrder) {
    super(name, repetition, logicalTypeAnnotation, id);
    this.primitive = primitive;
    this.length = length;
    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
      this.decimalMeta = new DecimalMetadata(decimal.getPrecision(), decimal.getScale());
    } else {
      this.decimalMeta = null;
    }

    if (columnOrder == null) {
      columnOrder = primitive == PrimitiveTypeName.INT96 || logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation
        ? ColumnOrder.undefined()
        : ColumnOrder.typeDefined();
    }
    this.columnOrder = requireValidColumnOrder(columnOrder);
  }

  private ColumnOrder requireValidColumnOrder(ColumnOrder columnOrder) {
    if (primitive == PrimitiveTypeName.INT96) {
      Preconditions.checkArgument(columnOrder.getColumnOrderName() == ColumnOrderName.UNDEFINED,
          "The column order %s is not supported by INT96", columnOrder);
    }
    if (getLogicalTypeAnnotation() != null) {
      Preconditions.checkArgument(getLogicalTypeAnnotation().isValidColumnOrder(columnOrder),
        "The column order %s is not supported by %s (%s)", columnOrder, primitive, getLogicalTypeAnnotation());
    }
    return columnOrder;
  }

  /**
   * @param id the field id
   * @return a new PrimitiveType with the same fields and a new id
   */
  @Override
  public PrimitiveType withId(int id) {
    return new PrimitiveType(getRepetition(), primitive, length, getName(), getLogicalTypeAnnotation(), new ID(id),
        columnOrder);
  }

  /**
   * @return the primitive type
   */
  public PrimitiveTypeName getPrimitiveTypeName() {
    return primitive;
  }

  /**
   * @return the type length
   */
  public int getTypeLength() {
    return length;
  }

  /**
   * @return the decimal type metadata
   */
  @Deprecated
  public DecimalMetadata getDecimalMetadata() {
    return decimalMeta;
  }

  /**
   * @return true
   */
  @Override
  public boolean isPrimitive() {
    return true;
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
    sb.append(indent)
        .append(getRepetition().name().toLowerCase(Locale.ENGLISH))
        .append(" ")
        .append(primitive.name().toLowerCase());
    if (primitive == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      sb.append("(" + length + ")");
    }
    sb.append(" ").append(getName());
    if (getLogicalTypeAnnotation() != null) {
      // TODO: should we print decimal metadata too?
      sb.append(" (").append(getLogicalTypeAnnotation().toString()).append(")");
    }
    if (getId() != null) {
      sb.append(" = ").append(getId());
    }
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
  protected boolean equals(Type other) {
    if (!other.isPrimitive()) {
      return false;
    }
    PrimitiveType otherPrimitive = other.asPrimitiveType();
    return super.equals(other)
        && primitive == otherPrimitive.getPrimitiveTypeName()
        && length == otherPrimitive.length
        && columnOrder.equals(otherPrimitive.columnOrder)
        && eqOrBothNull(decimalMeta, otherPrimitive.decimalMeta);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + primitive.hashCode();
    hash = hash * 31 + length;
    hash = hash * 31 + columnOrder.hashCode();
    if (decimalMeta != null) {
      hash = hash * 31 + decimalMeta.hashCode();
    }
    return hash;
  }

  @Override
  public int getMaxRepetitionLevel(String[] path, int i) {
    if (path.length != i) {
      throw new InvalidRecordException("Arrived at primitive node, path invalid");
    }
    return isRepetition(Repetition.REPEATED)? 1 : 0;
  }

  @Override
  public int getMaxDefinitionLevel(String[] path, int i) {
    if (path.length != i) {
      throw new InvalidRecordException("Arrived at primitive node, path invalid");
    }
    return isRepetition(Repetition.REQUIRED) ? 0 : 1;
  }

  @Override
  public Type getType(String[] path, int i) {
    if (path.length != i) {
      throw new InvalidRecordException("Arrived at primitive node at index " + i + " , path invalid: " + Arrays.toString(path));
    }
    return this;
  }

  @Override
  protected List<String[]> getPaths(int depth) {
    return Arrays.<String[]>asList(new String[depth]);
  }

  @Override
  void checkContains(Type subType) {
    super.checkContains(subType);
    if (!subType.isPrimitive()) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
    PrimitiveType primitiveType = subType.asPrimitiveType();
    if (this.primitive != primitiveType.primitive) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }

  }

  @Override
  public <T> T convert(List<GroupType> path, TypeConverter<T> converter) {
    return converter.convertPrimitiveType(path, this);
  }

  @Override
  protected boolean containsPath(String[] path, int depth) {
    return path.length == depth;
  }

  @Override
  protected Type union(Type toMerge) {
    return union(toMerge, true);
  }

  private void reportSchemaMergeError(Type toMerge) {
    throw new IncompatibleSchemaModificationException("can not merge type " + toMerge + " into " + this);
  }

  private void reportSchemaMergeErrorWithColumnOrder(Type toMerge) {
    throw new IncompatibleSchemaModificationException("can not merge type " + toMerge + " with column order "
        + toMerge.asPrimitiveType().columnOrder() + " into " + this + " with column order " + columnOrder());
  }

  @Override
  protected Type union(Type toMerge, boolean strict) {
    if (!toMerge.isPrimitive()) {
      reportSchemaMergeError(toMerge);
    }

    if (strict) {
      // Can't merge primitive fields of different type names or different original types
      if (!primitive.equals(toMerge.asPrimitiveType().getPrimitiveTypeName()) ||
        !Objects.equals(getLogicalTypeAnnotation(), toMerge.getLogicalTypeAnnotation())) {
        reportSchemaMergeError(toMerge);
      }

      // Can't merge FIXED_LEN_BYTE_ARRAY fields of different lengths
      int toMergeLength = toMerge.asPrimitiveType().getTypeLength();
      if (primitive == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && length != toMergeLength) {
        reportSchemaMergeError(toMerge);
      }

      // Can't merge primitive fields with different column orders
      if (!columnOrder().equals(toMerge.asPrimitiveType().columnOrder())) {
        reportSchemaMergeErrorWithColumnOrder(toMerge);
      }
    }

    Repetition repetition = Repetition.leastRestrictive(this.getRepetition(), toMerge.getRepetition());
    Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(primitive, repetition);

    if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == primitive) {
      builder.length(length);
    }

    return builder.as(getLogicalTypeAnnotation()).named(getName());
  }

  /**
   * Returns the {@link Type} specific comparator for properly comparing values. The natural ordering of the values
   * might not proper in certain cases (e.g. {@code UINT_32} requires unsigned comparison of {@code int} values while
   * the natural ordering is signed.)
   *
   * @param <T> the type of values compared by the returned PrimitiveComparator
   * @return a PrimitiveComparator for values of this type
   */
  @SuppressWarnings("unchecked")
  public <T> PrimitiveComparator<T> comparator() {
    return (PrimitiveComparator<T>) getPrimitiveTypeName().comparator(getLogicalTypeAnnotation());
  }

  /**
   * @return the column order for this type
   */
  public ColumnOrder columnOrder() {
    return columnOrder;
  }

  /**
   * @return the {@link Type} specific stringifier for generating the proper string representation of the values.
   */
  @SuppressWarnings("unchecked")
  public PrimitiveStringifier stringifier() {
    LogicalTypeAnnotation logicalTypeAnnotation = getLogicalTypeAnnotation();
    return logicalTypeAnnotation == null ? PrimitiveStringifier.DEFAULT_STRINGIFIER : logicalTypeAnnotation.valueStringifier(this);
  }
}
