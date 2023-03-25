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
package org.apache.parquet.avro;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.Preconditions;

/**
 * Avro implementation of {@link WriteSupport} for generic, specific, and
 * reflect models. Use {@link AvroParquetWriter} or
 * {@link AvroParquetOutputFormat} rather than using this class directly.
 */
public class AvroWriteSupport<T> extends WriteSupport<T> {

  public static final String AVRO_DATA_SUPPLIER = "parquet.avro.write.data.supplier";

  public static void setAvroDataSupplier(
      Configuration configuration, Class<? extends AvroDataSupplier> suppClass) {
    configuration.set(AVRO_DATA_SUPPLIER, suppClass.getName());
  }

  static final String AVRO_SCHEMA = "parquet.avro.schema";
  private static final Schema MAP_KEY_SCHEMA = Schema.create(Schema.Type.STRING);

  public static final String WRITE_OLD_LIST_STRUCTURE =
      "parquet.avro.write-old-list-structure";
  static final boolean WRITE_OLD_LIST_STRUCTURE_DEFAULT = true;
  public static final String WRITE_PARQUET_UUID = "parquet.avro.write-parquet-uuid";
  static final boolean WRITE_PARQUET_UUID_DEFAULT = false;

  // Support writing Parquet INT96 from a 12-byte Avro fixed.
  public static final String WRITE_FIXED_AS_INT96 = "parquet.avro.writeFixedAsInt96";

  private static final String MAP_REPEATED_NAME = "key_value";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";
  private static final String LIST_REPEATED_NAME = "list";
  private static final String OLD_LIST_REPEATED_NAME = "array";
  static final String LIST_ELEMENT_NAME = "element";

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootAvroSchema;
  private LogicalType rootLogicalType;
  private Conversion<?> rootConversion;
  private GenericData model;
  private ListWriter listWriter;

  public AvroWriteSupport() {
  }

  /**
   * @param schema the write parquet schema
   * @param avroSchema the write avro schema
   * @deprecated will be removed in 2.0.0
   */
  @Deprecated
  public AvroWriteSupport(MessageType schema, Schema avroSchema) {
    this.rootSchema = schema;
    this.rootAvroSchema = avroSchema;
    this.rootLogicalType = rootAvroSchema.getLogicalType();
    this.model = null;
  }

  public AvroWriteSupport(MessageType schema, Schema avroSchema,
                          GenericData model) {
    this.rootSchema = schema;
    this.rootAvroSchema = avroSchema;
    this.rootLogicalType = rootAvroSchema.getLogicalType();
    this.model = model;
  }

  @Override
  public String getName() {
    return "avro";
  }

  /**
   * @param configuration a configuration
   * @param schema the write schema
   * @see org.apache.parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setSchema(Configuration configuration, Schema schema) {
    configuration.set(AVRO_SCHEMA, schema.toString());
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (rootAvroSchema == null) {
      this.rootAvroSchema = new Schema.Parser().parse(configuration.get(AVRO_SCHEMA));
      this.rootSchema = new AvroSchemaConverter(configuration).convert(rootAvroSchema);
    }

    if (model == null) {
      this.model = getDataModel(configuration);
    }

    boolean writeOldListStructure = configuration.getBoolean(
        WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT);
    if (writeOldListStructure) {
      this.listWriter = new TwoLevelListWriter();
    } else {
      this.listWriter = new ThreeLevelListWriter();
    }

    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(AvroReadSupport.AVRO_SCHEMA_METADATA_KEY, rootAvroSchema.toString());
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  // overloaded version for backward compatibility
  @SuppressWarnings("unchecked")
  public void write(IndexedRecord record) {
    write((T) record);
  }

  @Override
  public void write(T record) {
    if (rootLogicalType != null) {
      Conversion<?> conversion = model.getConversionByClass(
          record.getClass(), rootLogicalType);

      recordConsumer.startMessage();
      writeRecordFields(rootSchema, rootAvroSchema,
          convert(rootAvroSchema, rootLogicalType, conversion, record));
      recordConsumer.endMessage();

    } else {
      recordConsumer.startMessage();
      writeRecordFields(rootSchema, rootAvroSchema, record);
      recordConsumer.endMessage();
    }
  }

  private void writeRecord(GroupType schema, Schema avroSchema,
                           Object record) {
    recordConsumer.startGroup();
    writeRecordFields(schema, avroSchema, record);
    recordConsumer.endGroup();
  }

  private void writeRecordFields(GroupType schema, Schema avroSchema,
                                 Object record) {
    List<Type> fields = schema.getFields();
    List<Schema.Field> avroFields = avroSchema.getFields();
    int index = 0; // parquet ignores Avro nulls, so index may differ
    for (int avroIndex = 0; avroIndex < avroFields.size(); avroIndex++) {
      Schema.Field avroField = avroFields.get(avroIndex);
      if (avroField.schema().getType().equals(Schema.Type.NULL)) {
        continue;
      }
      Type fieldType = fields.get(index);
      Object value = model.getField(record, avroField.name(), avroIndex);
      if (value != null) {
        recordConsumer.startField(fieldType.getName(), index);
        writeValue(fieldType, avroField.schema(), value);
        recordConsumer.endField(fieldType.getName(), index);
      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " + avroField.name());
      }
      index++;
    }
  }

  private <V> void writeMap(GroupType schema, Schema avroSchema,
                            Map<CharSequence, V> map) {
    GroupType innerGroup = schema.getType(0).asGroupType();
    Type keyType = innerGroup.getType(0);
    Type valueType = innerGroup.getType(1);

    recordConsumer.startGroup(); // group wrapper (original type MAP)
    if (map.size() > 0) {
      recordConsumer.startField(MAP_REPEATED_NAME, 0);

      for (Map.Entry<CharSequence, V> entry : map.entrySet()) {
        recordConsumer.startGroup(); // repeated group key_value, middle layer
        recordConsumer.startField(MAP_KEY_NAME, 0);
        writeValue(keyType, MAP_KEY_SCHEMA, entry.getKey());
        recordConsumer.endField(MAP_KEY_NAME, 0);
        V value = entry.getValue();
        if (value != null) {
          recordConsumer.startField(MAP_VALUE_NAME, 1);
          writeValue(valueType, avroSchema.getValueType(), value);
          recordConsumer.endField(MAP_VALUE_NAME, 1);
        } else if (!valueType.isRepetition(Type.Repetition.OPTIONAL)) {
          throw new RuntimeException("Null map value for " + avroSchema.getName());
        }
        recordConsumer.endGroup();
      }

      recordConsumer.endField(MAP_REPEATED_NAME, 0);
    }
    recordConsumer.endGroup();
  }

  private void writeUnion(GroupType parquetSchema, Schema avroSchema,
                          Object value) {
    recordConsumer.startGroup();

    // ResolveUnion will tell us which of the union member types to
    // deserialise.
    int avroIndex = model.resolveUnion(avroSchema, value);

    // For parquet's schema we skip nulls
    GroupType parquetGroup = parquetSchema.asGroupType();
    int parquetIndex = avroIndex;
    for (int i = 0; i < avroIndex; i++) {
      if (avroSchema.getTypes().get(i).getType().equals(Schema.Type.NULL)) {
        parquetIndex--;
      }
    }

    // TODO: what if the value is null?

    // Sparsely populated method of encoding unions, each member has its own
    // set of columns.
    String memberName = "member" + parquetIndex;
    recordConsumer.startField(memberName, parquetIndex);
    writeValue(parquetGroup.getType(parquetIndex),
               avroSchema.getTypes().get(avroIndex), value);
    recordConsumer.endField(memberName, parquetIndex);

    recordConsumer.endGroup();
  }

  /**
   * Calls an appropriate write method based on the value.
   * Value MUST not be null.
   *
   * @param type the Parquet type
   * @param avroSchema the Avro schema
   * @param value a non-null value to write
   */
  private void writeValue(Type type, Schema avroSchema, Object value) {
    Schema nonNullAvroSchema = AvroSchemaConverter.getNonNull(avroSchema);
    LogicalType logicalType = nonNullAvroSchema.getLogicalType();
    if (logicalType != null) {
      Conversion<?> conversion = model.getConversionByClass(
          value.getClass(), logicalType);
      writeValueWithoutConversion(type, nonNullAvroSchema,
          convert(nonNullAvroSchema, logicalType, conversion, value));
    } else {
      writeValueWithoutConversion(type, nonNullAvroSchema, value);
    }
  }

  private <D> Object convert(Schema schema, LogicalType logicalType,
                             Conversion<D> conversion, Object datum) {
    if (conversion == null) {
      return datum;
    }
    Class<D> fromClass = conversion.getConvertedType();
    switch (schema.getType()) {
      case RECORD:  return conversion.toRecord(fromClass.cast(datum), schema, logicalType);
      case ENUM:    return conversion.toEnumSymbol(fromClass.cast(datum), schema, logicalType);
      case ARRAY:   return conversion.toArray(fromClass.cast(datum), schema, logicalType);
      case MAP:     return conversion.toMap(fromClass.cast(datum), schema, logicalType);
      case FIXED:   return conversion.toFixed(fromClass.cast(datum), schema, logicalType);
      case STRING:  return conversion.toCharSequence(fromClass.cast(datum), schema, logicalType);
      case BYTES:   return conversion.toBytes(fromClass.cast(datum), schema, logicalType);
      case INT:     return conversion.toInt(fromClass.cast(datum), schema, logicalType);
      case LONG:    return conversion.toLong(fromClass.cast(datum), schema, logicalType);
      case FLOAT:   return conversion.toFloat(fromClass.cast(datum), schema, logicalType);
      case DOUBLE:  return conversion.toDouble(fromClass.cast(datum), schema, logicalType);
      case BOOLEAN: return conversion.toBoolean(fromClass.cast(datum), schema, logicalType);
    }
    return datum;
  }

  /**
   * Calls an appropriate write method based on the value.
   * Value must not be null and the schema must not be nullable.
   *
   * @param type a Parquet type
   * @param avroSchema a non-nullable Avro schema
   * @param value a non-null value to write
   */
  @SuppressWarnings("unchecked")
  private void writeValueWithoutConversion(Type type, Schema avroSchema, Object value) {
    switch (avroSchema.getType()) {
      case BOOLEAN:
        recordConsumer.addBoolean((Boolean) value);
        break;
      case INT:
        if (value instanceof Character) {
          recordConsumer.addInteger((Character) value);
        } else {
          recordConsumer.addInteger(((Number) value).intValue());
        }
        break;
      case LONG:
        recordConsumer.addLong(((Number) value).longValue());
        break;
      case FLOAT:
        recordConsumer.addFloat(((Number) value).floatValue());
        break;
      case DOUBLE:
        recordConsumer.addDouble(((Number) value).doubleValue());
        break;
      case FIXED:
        recordConsumer.addBinary(Binary.fromReusedByteArray(((GenericFixed) value).bytes()));
        break;
      case BYTES:
        if (value instanceof byte[]) {
          recordConsumer.addBinary(Binary.fromReusedByteArray((byte[]) value));
        } else {
          recordConsumer.addBinary(Binary.fromReusedByteBuffer((ByteBuffer) value));
        }
        break;
      case STRING:
        if (type.asPrimitiveType().getLogicalTypeAnnotation() instanceof UUIDLogicalTypeAnnotation) {
          recordConsumer.addBinary(fromUUIDString(value));
        } else {
          recordConsumer.addBinary(fromAvroString(value));
        }
        break;
      case RECORD:
        writeRecord(type.asGroupType(), avroSchema, value);
        break;
      case ENUM:
        recordConsumer.addBinary(Binary.fromString(value.toString()));
        break;
      case ARRAY:
        listWriter.writeList(type.asGroupType(), avroSchema, value);
        break;
      case MAP:
        writeMap(type.asGroupType(), avroSchema, (Map<CharSequence, ?>) value);
        break;
      case UNION:
        writeUnion(type.asGroupType(), avroSchema, value);
        break;
    }
  }

  private Binary fromUUIDString(Object value) {
    byte[] data = new byte[UUIDLogicalTypeAnnotation.BYTES];
    UUID uuid = UUID.fromString(value.toString());
    writeLong(data, 0, uuid.getMostSignificantBits());
    writeLong(data, Long.BYTES, uuid.getLeastSignificantBits());
    return Binary.fromConstantByteArray(data);
  }

  private void writeLong(byte[] array, int offset, long value) {
    for (int i = 0; i < Long.BYTES; ++i) {
      array[i + offset] = (byte) (value >>> ((Long.BYTES - i - 1) * Byte.SIZE));
    }
  }

  private Binary fromAvroString(Object value) {
    if (value instanceof Utf8) {
      Utf8 utf8 = (Utf8) value;
      return Binary.fromReusedByteArray(utf8.getBytes(), 0, utf8.getByteLength());
    } else if (value instanceof CharSequence) {
      return Binary.fromCharSequence((CharSequence) value);
    }
    return Binary.fromCharSequence(value.toString());
  }

  private static GenericData getDataModel(Configuration conf) {
    Class<? extends AvroDataSupplier> suppClass = conf.getClass(
        AVRO_DATA_SUPPLIER, SpecificDataSupplier.class, AvroDataSupplier.class);
    return ReflectionUtils.newInstance(suppClass, conf).get();
  }

  private abstract class ListWriter {

    protected abstract void writeCollection(
        GroupType type, Schema schema, Collection<?> collection);

    protected abstract void writeObjectArray(
        GroupType type, Schema schema, Object[] array);

    protected abstract void startArray();

    protected abstract void endArray();

    public void writeList(GroupType schema, Schema avroSchema, Object value) {
      recordConsumer.startGroup(); // group wrapper (original type LIST)
      if (value instanceof Collection) {
        writeCollection(schema, avroSchema, (Collection) value);
      } else {
        Class<?> arrayClass = value.getClass();
        Preconditions.checkArgument(arrayClass.isArray(),
            "Cannot write unless collection or array: %s", arrayClass.getName());
        writeJavaArray(schema, avroSchema, arrayClass, value);
      }
      recordConsumer.endGroup();
    }

    public void writeJavaArray(GroupType schema, Schema avroSchema,
                               Class<?> arrayClass, Object value) {
      Class<?> elementClass = arrayClass.getComponentType();

      if (!elementClass.isPrimitive()) {
        writeObjectArray(schema, avroSchema, (Object[]) value);
        return;
      }

      switch (avroSchema.getElementType().getType()) {
        case BOOLEAN:
          Preconditions.checkArgument(elementClass == boolean.class,
              "Cannot write as boolean array: %s", arrayClass.getName());
          writeBooleanArray((boolean[]) value);
          break;
        case INT:
          if (elementClass == byte.class) {
            writeByteArray((byte[]) value);
          } else if (elementClass == char.class) {
            writeCharArray((char[]) value);
          } else if (elementClass == short.class) {
            writeShortArray((short[]) value);
          } else if (elementClass == int.class) {
            writeIntArray((int[]) value);
          } else {
            throw new IllegalArgumentException(
                "Cannot write as an int array: " + arrayClass.getName());
          }
          break;
        case LONG:
          Preconditions.checkArgument(elementClass == long.class,
              "Cannot write as long array: %s", arrayClass.getName());
          writeLongArray((long[]) value);
          break;
        case FLOAT:
          Preconditions.checkArgument(elementClass == float.class,
              "Cannot write as float array: %s", arrayClass.getName());
          writeFloatArray((float[]) value);
          break;
        case DOUBLE:
          Preconditions.checkArgument(elementClass == double.class,
              "Cannot write as double array: %s", arrayClass.getName());
          writeDoubleArray((double[]) value);
          break;
        default:
          throw new IllegalArgumentException("Cannot write " +
              avroSchema.getElementType() + " array: " + arrayClass.getName());
      }
    }

    protected void writeBooleanArray(boolean[] array) {
      if (array.length > 0) {
        startArray();
        for (boolean element : array) {
          recordConsumer.addBoolean(element);
        }
        endArray();
      }
    }

    protected void writeByteArray(byte[] array) {
      if (array.length > 0) {
        startArray();
        for (byte element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeShortArray(short[] array) {
      if (array.length > 0) {
        startArray();
        for (short element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeCharArray(char[] array) {
      if (array.length > 0) {
        startArray();
        for (char element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeIntArray(int[] array) {
      if (array.length > 0) {
        startArray();
        for (int element : array) {
          recordConsumer.addInteger(element);
        }
        endArray();
      }
    }

    protected void writeLongArray(long[] array) {
      if (array.length > 0) {
        startArray();
        for (long element : array) {
          recordConsumer.addLong(element);
        }
        endArray();
      }
    }

    protected void writeFloatArray(float[] array) {
      if (array.length > 0) {
        startArray();
        for (float element : array) {
          recordConsumer.addFloat(element);
        }
        endArray();
      }
    }

    protected void writeDoubleArray(double[] array) {
      if (array.length > 0) {
        startArray();
        for (double element : array) {
          recordConsumer.addDouble(element);
        }
        endArray();
      }
    }
  }

  /**
   * For backward-compatibility. This preserves how lists were written in 1.x.
   */
  private class TwoLevelListWriter extends ListWriter {
    @Override
    public void writeCollection(GroupType schema, Schema avroSchema,
                                Collection<?> array) {
      if (!array.isEmpty()) {
        recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
        try {
          for (Object elt : array) {
            writeValue(schema.getType(0), avroSchema.getElementType(), elt);
          }
        } catch (NullPointerException e) {
          // find the null element and throw a better error message
          final int idx =
              Arrays.asList(array.toArray(new Object[0])).indexOf(null);
          if (idx < 0) {
            // no element was null, throw the original exception
            throw e;
          }
          throw new NullPointerException(
              "Array contains a null element at " + idx + ". "
                  + "Set parquet.avro.write-old-list-structure=false to turn "
                  + "on support for arrays with null elements.");
        }
        recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Schema schema,
                                    Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
        try {
          for (Object element : array) {
            writeValue(type.getType(0), schema.getElementType(), element);
          }
        } catch (NullPointerException e) {
          // find the null element and throw a better error message
          final int idx = Arrays.asList(array).indexOf(null);
          if (idx < 0) {
            // no element was null, throw the original exception
            throw e;
          }
          throw new NullPointerException(
              "Array contains a null element at " + idx + ". " +
              "Set parquet.avro.write-old-list-structure=false to turn " +
              "on support for arrays with null elements.");
        }
        recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void startArray() {
      recordConsumer.startField(OLD_LIST_REPEATED_NAME, 0);
    }

    @Override
    protected void endArray() {
      recordConsumer.endField(OLD_LIST_REPEATED_NAME, 0);
    }
  }

  private class ThreeLevelListWriter extends ListWriter {
    @Override
    protected void writeCollection(GroupType type, Schema schema, Collection<?> collection) {
      if (collection.size() > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : collection) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, schema.getElementType(), element);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                "Null list element for " + schema.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void writeObjectArray(GroupType type, Schema schema,
                                    Object[] array) {
      if (array.length > 0) {
        recordConsumer.startField(LIST_REPEATED_NAME, 0);
        GroupType repeatedType = type.getType(0).asGroupType();
        Type elementType = repeatedType.getType(0);
        for (Object element : array) {
          recordConsumer.startGroup(); // repeated group array, middle layer
          if (element != null) {
            recordConsumer.startField(LIST_ELEMENT_NAME, 0);
            writeValue(elementType, schema.getElementType(), element);
            recordConsumer.endField(LIST_ELEMENT_NAME, 0);
          } else if (!elementType.isRepetition(Type.Repetition.OPTIONAL)) {
            throw new RuntimeException(
                "Null list element for " + schema.getName());
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST_REPEATED_NAME, 0);
      }
    }

    @Override
    protected void startArray() {
      recordConsumer.startField(LIST_REPEATED_NAME, 0);
      recordConsumer.startGroup(); // repeated group array, middle layer
      recordConsumer.startField(LIST_ELEMENT_NAME, 0);
    }

    @Override
    protected void endArray() {
      recordConsumer.endField(LIST_ELEMENT_NAME, 0);
      recordConsumer.endGroup();
      recordConsumer.endField(LIST_REPEATED_NAME, 0);
    }
  }
}
