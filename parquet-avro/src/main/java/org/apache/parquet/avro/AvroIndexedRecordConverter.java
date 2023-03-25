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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * This {@link Converter} class materializes records as Avro
 * {@link IndexedRecord} instances. This is replaced by
 * {@link AvroRecordConverter}, but is included for backward-compatibility.
 *
 * @param <T> a subclass of Avro's IndexedRecord
 */
class AvroIndexedRecordConverter<T extends IndexedRecord> extends GroupConverter {

  private final ParentValueContainer parent;
  protected T currentRecord;
  private final Converter[] converters;

  private final Schema avroSchema;
  private final Class<? extends IndexedRecord> specificClass;

  private final GenericData model;
  private final Map<Schema.Field, Object> recordDefaults = new HashMap<Schema.Field, Object>();

  public AvroIndexedRecordConverter(MessageType parquetSchema, Schema avroSchema) {
    this(parquetSchema, avroSchema, SpecificData.get());
  }

  public AvroIndexedRecordConverter(MessageType parquetSchema, Schema avroSchema,
      GenericData baseModel) {
    this(null, parquetSchema, avroSchema, baseModel);
  }

  public AvroIndexedRecordConverter(ParentValueContainer parent, GroupType
      parquetSchema, Schema avroSchema) {
    this(parent, parquetSchema, avroSchema, SpecificData.get());
  }

  public AvroIndexedRecordConverter(ParentValueContainer parent, GroupType
      parquetSchema, Schema avroSchema, GenericData baseModel) {
    this.parent = parent;
    this.avroSchema = avroSchema;
    int schemaSize = parquetSchema.getFieldCount();
    this.converters = new Converter[schemaSize];
    this.specificClass = getDatumClass(baseModel, avroSchema);

    this.model = this.specificClass == null ? GenericData.get() : baseModel;

    Map<String, Integer> avroFieldIndexes = new HashMap<String, Integer>();
    int avroFieldIndex = 0;
    for (Schema.Field field: avroSchema.getFields()) {
        avroFieldIndexes.put(field.name(), avroFieldIndex++);
    }
    int parquetFieldIndex = 0;
    for (Type parquetField: parquetSchema.getFields()) {
      Schema.Field avroField = getAvroField(parquetField.getName());
      Schema nonNullSchema = AvroSchemaConverter.getNonNull(avroField.schema());
      final int finalAvroIndex = avroFieldIndexes.remove(avroField.name());
      converters[parquetFieldIndex++] = newConverter(nonNullSchema, parquetField, model, new ParentValueContainer() {
        @Override
        public void add(Object value) {
          AvroIndexedRecordConverter.this.set(finalAvroIndex, value);
        }
      });
    }
    // store defaults for any new Avro fields from avroSchema that are not in the writer schema (parquetSchema)
    for (String fieldName : avroFieldIndexes.keySet()) {
      Schema.Field field = avroSchema.getField(fieldName);
      if (field.schema().getType() == Schema.Type.NULL) {
        continue; // skip null since Parquet does not write nulls
      }
      if (field.defaultVal() == null || model.getDefaultValue(field) == null) {
        continue; // field has no default
      }
      recordDefaults.put(field, model.getDefaultValue(field));
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> getDatumClass(GenericData model, Schema schema) {
    if (model.getConversionFor(schema.getLogicalType()) != null) {
      // use generic classes to pass data to conversions
      return null;
    }

    if (model instanceof SpecificData) {
      return (Class<T>) ((SpecificData) model).getClass(schema);
    }
    return null;
  }

  private Schema.Field getAvroField(String parquetFieldName) {
    Schema.Field avroField = avroSchema.getField(parquetFieldName);
    for (Schema.Field f : avroSchema.getFields()) {
      if (f.aliases().contains(parquetFieldName)) {
        return f;
      }
    }
    if (avroField == null) {
      throw new InvalidRecordException(String.format(
          "Parquet/Avro schema mismatch. Avro field '%s' not found.",
          parquetFieldName));
    }
    return avroField;
  }

  private static Converter newConverter(Schema schema, Type type,
      GenericData model, ParentValueContainer setter) {

    LogicalType logicalType = schema.getLogicalType();
    // the expected type is always null because it is determined by the parent
    // datum class, which never helps for generic. when logical types are added
    // to specific, this should pass the expected type here.
    Conversion<?> conversion = model.getConversionFor(logicalType);
    ParentValueContainer parent = ParentValueContainer
        .getConversionContainer(setter, conversion, schema);

    switch (schema.getType()) {
    case ARRAY:
      return new AvroArrayConverter(parent, type.asGroupType(), schema, model);
    case BOOLEAN:
      return new AvroConverters.FieldBooleanConverter(parent);
    case BYTES:
      return new AvroConverters.FieldByteBufferConverter(parent);
    case DOUBLE:
      return new AvroConverters.FieldDoubleConverter(parent);
    case ENUM:
      return new FieldEnumConverter(parent, schema, model);
    case FIXED:
      return new FieldFixedConverter(parent, schema, model);
    case FLOAT:
      return new AvroConverters.FieldFloatConverter(parent);
    case INT:
      return new AvroConverters.FieldIntegerConverter(parent);
    case LONG:
      return new AvroConverters.FieldLongConverter(parent);
    case MAP:
      return new MapConverter(parent, type.asGroupType(), schema, model);
    case RECORD:
      return new AvroIndexedRecordConverter(parent, type.asGroupType(), schema, model);
    case STRING:
      return new AvroConverters.FieldStringConverter(parent);
    case UNION:
      return new AvroUnionConverter(parent, type, schema, model);
    case NULL: // fall through
    default:
      throw new UnsupportedOperationException(String.format("Cannot convert Avro type: %s" +
          " (Parquet type: %s) ", schema, type));
    }
  }

  private void set(int index, Object value) {
    this.currentRecord.put(index, value);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  @SuppressWarnings("unchecked")
  public void start() {
    // Should do the right thing whether it is generic or specific
    this.currentRecord = (T) ((this.specificClass == null) ?
            new GenericData.Record(avroSchema) :
            SpecificData.newInstance(specificClass, avroSchema));
  }

  @Override
  public void end() {
    fillInDefaults();
    if (parent != null) {
      parent.add(currentRecord);
    }
  }

  private void fillInDefaults() {
    for (Map.Entry<Schema.Field, Object> entry : recordDefaults.entrySet()) {
      Schema.Field f = entry.getKey();
      // replace following with model.deepCopy once AVRO-1455 is being used
      Object defaultValue = deepCopy(f.schema(), entry.getValue());
      this.currentRecord.put(f.pos(), defaultValue);
    }
  }

  private Object deepCopy(Schema schema, Object value) {
    switch (schema.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return value;
      default:
        return model.deepCopy(schema, value);
    }
  }

  T getCurrentRecord() {
    return currentRecord;
  }

  static final class FieldEnumConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;
    private final Class<? extends Enum> enumClass;

    public FieldEnumConverter(ParentValueContainer parent, Schema enumSchema,
        GenericData model) {
      this.parent = parent;
      this.enumClass = model instanceof SpecificData ?
          ((SpecificData) model).getClass(enumSchema) :
          SpecificData.get().getClass(enumSchema);
    }

    @Override
    final public void addBinary(Binary value) {
      Object enumValue = value.toStringUsingUTF8();
      if (enumClass != null) {
        enumValue = (Enum.valueOf(enumClass,(String)enumValue));
      }
      parent.add(enumValue);
    }
  }

  static final class FieldFixedConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;
    private final Schema avroSchema;
    private final Class<? extends GenericData.Fixed> fixedClass;
    private final Constructor fixedClassCtor;

    public FieldFixedConverter(ParentValueContainer parent, Schema avroSchema,
        GenericData model) {
      this.parent = parent;
      this.avroSchema = avroSchema;
      this.fixedClass = model instanceof SpecificData ?
          ((SpecificData) model).getClass(avroSchema) :
          SpecificData.get().getClass(avroSchema);
      if (fixedClass != null) {
        try {
          this.fixedClassCtor = 
              fixedClass.getConstructor(new Class[] { byte[].class });
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        this.fixedClassCtor = null;
      }
    }

    @Override
    final public void addBinary(Binary value) {
      if (fixedClass == null) {
        parent.add(new GenericData.Fixed(avroSchema, value.getBytes()));
      } else {
        if (fixedClassCtor == null) {
          throw new IllegalArgumentException(
              "fixedClass specified but fixedClassCtor is null.");
        }
        try {
          Object fixed = fixedClassCtor.newInstance(value.getBytes());
          parent.add(fixed);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Converter for a list.
   *
   * <pre>
   *   optional group the_list (LIST) { <-- this layer
   *     repeated group array {
   *       optional (type) element;
   *     }
   *   }
   * </pre>
   *
   * This class also implements LIST element backward-compatibility rules.
   */
  static final class AvroArrayConverter extends GroupConverter {

    private final ParentValueContainer parent;
    private final Schema avroSchema;
    private final Converter converter;
    private GenericArray<Object> array;

    public AvroArrayConverter(ParentValueContainer parent, GroupType type,
        Schema avroSchema, GenericData model) {
      this.parent = parent;
      this.avroSchema = avroSchema;
      Schema elementSchema = AvroSchemaConverter
          .getNonNull(avroSchema.getElementType());
      Type repeatedType = type.getType(0);
      // always determine whether the repeated type is the element type by
      // matching it against the element schema.
      if (AvroRecordConverter.isElementType(repeatedType, elementSchema)) {
        // the element type is the repeated type (and required)
        converter = newConverter(elementSchema, repeatedType, model, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          public void add(Object value) {
            array.add(value);
          }
        });
      } else {
        // the element is wrapped in a synthetic group and may be optional
        converter = new ElementConverter(repeatedType.asGroupType(), elementSchema, model);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converter;
    }

    @Override
    public void start() {
      array = new GenericData.Array<Object>(0, avroSchema);
    }

    @Override
    public void end() {
      parent.add(array);
    }

    /**
     * Converter for list elements.
     *
     * <pre>
     *   optional group the_list (LIST) {
     *     repeated group array { <-- this layer
     *       optional (type) element;
     *     }
     *   }
     * </pre>
     */
    final class ElementConverter extends GroupConverter {
      private Object element;
      private final Converter elementConverter;

      public ElementConverter(GroupType repeatedType, Schema elementSchema, GenericData model) {
        Type elementType = repeatedType.getType(0);
        Schema nonNullElementSchema = AvroSchemaConverter.getNonNull(elementSchema);
        this.elementConverter = newConverter(nonNullElementSchema, elementType, model, new ParentValueContainer() {
          @Override
          public void add(Object value) {
            ElementConverter.this.element = value;
          }
        });
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        Preconditions.checkArgument(
            fieldIndex == 0, "Illegal field index: %s", fieldIndex);
        return elementConverter;
      }

      @Override
      public void start() {
        element = null;
      }

      @Override
      public void end() {
        array.add(element);
      }
    }
  }

  static final class AvroUnionConverter extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter[] memberConverters;
    private Object memberValue = null;

    public AvroUnionConverter(ParentValueContainer parent, Type parquetSchema,
                              Schema avroSchema, GenericData model) {
      this.parent = parent;
      GroupType parquetGroup = parquetSchema.asGroupType();
      this.memberConverters = new Converter[ parquetGroup.getFieldCount()];

      int parquetIndex = 0;
      for (int index = 0; index < avroSchema.getTypes().size(); index++) {
        Schema memberSchema = avroSchema.getTypes().get(index);
        if (!memberSchema.getType().equals(Schema.Type.NULL)) {
          Type memberType = parquetGroup.getType(parquetIndex);
          memberConverters[parquetIndex] = newConverter(memberSchema, memberType, model, new ParentValueContainer() {
            @Override
            public void add(Object value) {
              Preconditions.checkArgument(memberValue==null, "Union is resolving to more than one type");
              memberValue = value;
            }
          });
          parquetIndex++; // Note for nulls the parquetIndex id not increased
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return memberConverters[fieldIndex];
    }

    @Override
    public void start() {
      memberValue = null;
    }

    @Override
    public void end() {
      parent.add(memberValue);
    }
  }

  static final class MapConverter<V> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter keyValueConverter;
    private Map<String, V> map;

    public MapConverter(ParentValueContainer parent, GroupType mapType,
        Schema mapSchema, GenericData model) {
      this.parent = parent;
      GroupType repeatedKeyValueType = mapType.getType(0).asGroupType();
      this.keyValueConverter = new MapKeyValueConverter(
          repeatedKeyValueType, mapSchema, model);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return keyValueConverter;
    }

    @Override
    public void start() {
      this.map = new HashMap<String, V>();
    }

    @Override
    public void end() {
      parent.add(map);
    }

    final class MapKeyValueConverter extends GroupConverter {

      private String key;
      private V value;
      private final Converter keyConverter;
      private final Converter valueConverter;

      public MapKeyValueConverter(GroupType keyValueType, Schema mapSchema,
          GenericData model) {
        keyConverter = new PrimitiveConverter() {
          @Override
          final public void addBinary(Binary value) {
            key = value.toStringUsingUTF8();
          }
        };

        Type valueType = keyValueType.getType(1);
        Schema nonNullValueSchema = AvroSchemaConverter.getNonNull(mapSchema.getValueType());
        valueConverter = newConverter(nonNullValueSchema, valueType, model, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          public void add(Object value) {
            MapKeyValueConverter.this.value = (V) value;
          }
        });
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        if (fieldIndex == 0) {
          return keyConverter;
        } else if (fieldIndex == 1) {
          return valueConverter;
        }
        throw new IllegalArgumentException("only the key (0) and value (1) fields expected: " + fieldIndex);
      }

      @Override
      public void start() {
        key = null;
        value = null;
      }

      @Override
      public void end() {
        map.put(key, value);
      }
    }
  }

}
