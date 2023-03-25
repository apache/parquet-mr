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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Stringable;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.ClassUtils;
import org.apache.parquet.Preconditions;
import org.apache.parquet.avro.AvroConverters.FieldStringConverter;
import org.apache.parquet.avro.AvroConverters.FieldStringableConverter;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import static org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * This {@link Converter} class materializes records for a given
 * {@link GenericData Avro data model}. This replaces
 * {@link AvroIndexedRecordConverter} and works with generic, specific, and
 * reflect records.
 *
 * @param <T> a subclass of Avro's IndexedRecord
 */
class AvroRecordConverter<T> extends AvroConverters.AvroGroupConverter {

  private static final String STRINGABLE_PROP = "avro.java.string";
  private static final String JAVA_CLASS_PROP = "java-class";
  private static final String JAVA_KEY_CLASS_PROP = "java-key-class";

  protected T currentRecord = null;
  private ParentValueContainer rootContainer = null;
  private final Converter[] converters;

  private final Schema avroSchema;

  private final GenericData model;
  private final Map<Schema.Field, Object> recordDefaults = new HashMap<Schema.Field, Object>();

  public AvroRecordConverter(MessageType parquetSchema, Schema avroSchema,
                             GenericData baseModel) {
    this(null, parquetSchema, avroSchema, baseModel);
    LogicalType logicalType = avroSchema.getLogicalType();
    Conversion<?> conversion = baseModel.getConversionFor(logicalType);
    this.rootContainer = ParentValueContainer.getConversionContainer(new ParentValueContainer() {
      @Override
      @SuppressWarnings("unchecked")
      public void add(Object value) {
        AvroRecordConverter.this.currentRecord = (T) value;
      }
    }, conversion, avroSchema);
  }

  public AvroRecordConverter(ParentValueContainer parent,
                             GroupType parquetSchema, Schema avroSchema,
                             GenericData model) {
    super(parent);
    this.avroSchema = avroSchema;
    this.model = (model == null ? ReflectData.get() : model);
    this.converters = new Converter[parquetSchema.getFieldCount()];

    Map<String, Integer> avroFieldIndexes = new HashMap<String, Integer>();
    int avroFieldIndex = 0;
    for (Schema.Field field: avroSchema.getFields()) {
        avroFieldIndexes.put(field.name(), avroFieldIndex++);
    }

    Class<?> recordClass = null;
    if (model instanceof ReflectData) {
      recordClass = getDatumClass(avroSchema, model);
    }

    Map<String, Class<?>> fields = getFieldsByName(recordClass, false);

    int parquetFieldIndex = 0;
    for (Type parquetField: parquetSchema.getFields()) {
      final Schema.Field avroField = getAvroField(parquetField.getName());
      Schema nonNullSchema = AvroSchemaConverter.getNonNull(avroField.schema());
      final int finalAvroIndex = avroFieldIndexes.remove(avroField.name());
      ParentValueContainer container = new ParentValueContainer() {
        @Override
        public void add(Object value) {
          AvroRecordConverter.this.set(avroField.name(), finalAvroIndex, value);
        }
      };

      Class<?> fieldClass = fields.get(avroField.name());
      converters[parquetFieldIndex] = newConverter(
          nonNullSchema, parquetField, this.model, fieldClass, container);

      // @Stringable doesn't affect the reflected schema; must be enforced here
      if (recordClass != null &&
          converters[parquetFieldIndex] instanceof FieldStringConverter) {
        try {
          Field field = recordClass.getDeclaredField(avroField.name());
          if (field.isAnnotationPresent(Stringable.class)) {
            converters[parquetFieldIndex] = new FieldStringableConverter(
                container, field.getType());
          }
        } catch (NoSuchFieldException e) {
          // must not be stringable
        }
      }

      parquetFieldIndex += 1;
    }

    // store defaults for any new Avro fields from avroSchema that are not in
    // the writer schema (parquetSchema)
    for (String fieldName : avroFieldIndexes.keySet()) {
      Schema.Field field = avroSchema.getField(fieldName);
      if (field.schema().getType() == Schema.Type.NULL) {
        continue; // skip null since Parquet does not write nulls
      }
      if (field.defaultVal() == null || this.model.getDefaultValue(field) == null) {
        continue; // field has no default
      }
      // use this.model because model may be null
      recordDefaults.put(field, this.model.getDefaultValue(field));
    }
  }

  // this was taken from Avro's ReflectData
  private static Map<String, Class<?>> getFieldsByName(Class<?> recordClass,
                                                       boolean excludeJava) {
    Map<String, Class<?>> fields = new LinkedHashMap<String, Class<?>>();

    if (recordClass != null) {
      Class<?> current = recordClass;
      do {
        if (excludeJava && current.getPackage() != null
            && current.getPackage().getName().startsWith("java.")) {
          break; // skip java built-in classes
        }
        for (Field field : current.getDeclaredFields()) {
          if (field.isAnnotationPresent(AvroIgnore.class) ||
              isTransientOrStatic(field)) {
            continue;
          }
          AvroName altName = field.getAnnotation(AvroName.class);
          Class<?> existing = fields.put(
              altName != null ? altName.value() : field.getName(),
              field.getType());
          if (existing != null) {
            throw new AvroTypeException(
                current + " contains two fields named: " + field.getName());
          }
        }
        current = current.getSuperclass();
      } while (current != null);
    }

    return fields;
  }

  private static boolean isTransientOrStatic(Field field) {
    return (field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) != 0;
  }

  private Schema.Field getAvroField(String parquetFieldName) {
    Schema.Field avroField = avroSchema.getField(parquetFieldName);
    if (avroField != null) {
      return avroField;
    }

    for (Schema.Field f : avroSchema.getFields()) {
      if (f.aliases().contains(parquetFieldName)) {
        return f;
      }
    }

    throw new InvalidRecordException(String.format(
        "Parquet/Avro schema mismatch: Avro field '%s' not found",
        parquetFieldName));
  }

  private static Converter newConverter(
      Schema schema, Type type, GenericData model, ParentValueContainer setter) {
    return newConverter(schema, type, model, null, setter);
  }

  private static Converter newConverter(Schema schema, Type type,
      GenericData model, Class<?> knownClass, ParentValueContainer setter) {
    LogicalType logicalType = schema.getLogicalType();
    Conversion<?> conversion;

    if (knownClass != null) {
      conversion = model.getConversionByClass(knownClass, logicalType);
    } else {
      conversion = model.getConversionFor(logicalType);
    }

    ParentValueContainer parent = ParentValueContainer
        .getConversionContainer(setter, conversion, schema);

    switch (schema.getType()) {
    case BOOLEAN:
      return new AvroConverters.FieldBooleanConverter(parent);
    case INT:
      Class<?> intDatumClass = getDatumClass(conversion, knownClass, schema, model);
      if (intDatumClass == null) {
        return new AvroConverters.FieldIntegerConverter(parent);
      }
      if (intDatumClass == byte.class || intDatumClass == Byte.class) {
        return new AvroConverters.FieldByteConverter(parent);
      }
      if (intDatumClass == char.class || intDatumClass == Character.class) {
        return new AvroConverters.FieldCharConverter(parent);
      }
      if (intDatumClass == short.class || intDatumClass == Short.class) {
        return new AvroConverters.FieldShortConverter(parent);
      }
      return new AvroConverters.FieldIntegerConverter(parent);
    case LONG:
      return new AvroConverters.FieldLongConverter(parent);
    case FLOAT:
      return new AvroConverters.FieldFloatConverter(parent);
    case DOUBLE:
      return new AvroConverters.FieldDoubleConverter(parent);
    case BYTES:
      Class<?> byteDatumClass = getDatumClass(conversion, knownClass, schema, model);
      if (byteDatumClass == null) {
        return new AvroConverters.FieldByteBufferConverter(parent);
      }
      if (byteDatumClass.isArray() && byteDatumClass.getComponentType() == byte.class) {
        return new AvroConverters.FieldByteArrayConverter(parent);
      }
      return new AvroConverters.FieldByteBufferConverter(parent);
    case STRING:
      if (logicalType != null && logicalType.getName().equals(LogicalTypes.uuid().getName())) {
        return new AvroConverters.FieldUUIDConverter(parent, type.asPrimitiveType());
      }
      return newStringConverter(schema, model, parent);
    case RECORD:
      return new AvroRecordConverter(parent, type.asGroupType(), schema, model);
    case ENUM:
      return new AvroConverters.FieldEnumConverter(parent, schema, model);
    case ARRAY:
      Class<?> arrayDatumClass = getDatumClass(conversion, knownClass, schema, model);
      if (arrayDatumClass != null && arrayDatumClass.isArray()) {
        return new AvroArrayConverter(parent, type.asGroupType(), schema, model,
            arrayDatumClass);
      }
      return new AvroCollectionConverter(parent, type.asGroupType(), schema,
          model, arrayDatumClass);
    case MAP:
      return new MapConverter(parent, type.asGroupType(), schema, model);
    case UNION:
      return new AvroUnionConverter(parent, type, schema, model);
    case FIXED:
      return new AvroConverters.FieldFixedConverter(parent, schema, model);
    default:
      throw new UnsupportedOperationException(String.format(
          "Cannot convert Avro type: %s to Parquet type: %s", schema, type));
    }
  }

  private static Converter newStringConverter(Schema schema, GenericData model,
                                              ParentValueContainer parent) {
    Class<?> stringableClass = getStringableClass(schema, model);
    if (stringableClass == String.class) {
      return new FieldStringConverter(parent);
    } else if (stringableClass == CharSequence.class) {
      return new AvroConverters.FieldUTF8Converter(parent);
    }
    return new FieldStringableConverter(parent, stringableClass);
  }

  private static Class<?> getStringableClass(Schema schema, GenericData model) {
    if (model instanceof SpecificData) {
      // both specific and reflect (and any subclasses) use this logic
      boolean isMap = (schema.getType() == Schema.Type.MAP);
      String stringableClass = schema.getProp(
          isMap ? JAVA_KEY_CLASS_PROP : JAVA_CLASS_PROP);
      if (stringableClass != null) {
        try {
          return ClassUtils.forName(model.getClassLoader(), stringableClass);
        } catch (ClassNotFoundException e) {
          // not available, use a String instead
        }
      }
    }

    if (ReflectData.class.isAssignableFrom(model.getClass())) {
      // reflect uses String, not Utf8
      return String.class;
    }

    // generic and specific use the avro.java.string setting
    String name = schema.getProp(STRINGABLE_PROP);
    if (name == null) {
      return CharSequence.class;
    }

    switch (GenericData.StringType.valueOf(name)) {
      case String:
        return String.class;
      default:
        return CharSequence.class; // will use Utf8
    }
  }

  private static <T> Class<T> getDatumClass(Schema schema, GenericData model) {
    return getDatumClass(null, null, schema, model);
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> getDatumClass(Conversion<?> conversion,
                                            Class<T> knownClass,
                                            Schema schema, GenericData model) {
    if (conversion != null) {
      // use generic classes to pass data to conversions
      return null;
    }

    // known class can be set when using reflect
    if (knownClass != null) {
      return knownClass;
    }

    if (model instanceof SpecificData) {
      // this works for reflect as well
      return ((SpecificData) model).getClass(schema);

    } else if (model.getClass() == GenericData.class) {
      return null;

    } else {
      // try to use reflection (for ThriftData and others)
      Class<? extends GenericData> modelClass = model.getClass();
      Method getClassMethod;
      try {
        getClassMethod = modelClass.getMethod("getClass", Schema.class);
      } catch (NoSuchMethodException e) {
        return null; // no getClass method
      }

      try {
        return (Class<T>) getClassMethod.invoke(schema);
      } catch (IllegalAccessException | InvocationTargetException e) {
        return null;
      }
    }
  }

  protected void set(String name, int avroIndex, Object value) {
    model.setField(currentRecord, name, avroIndex, value);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  @SuppressWarnings("unchecked")
  public void start() {
    this.currentRecord = (T) model.newRecord(null, avroSchema);
  }

  @Override
  public void end() {
    fillInDefaults();
    if (parent != null) {
      parent.add(currentRecord);
    } else {
      // this applies any converters needed for the root value
      rootContainer.add(currentRecord);
    }
  }

  private void fillInDefaults() {
    for (Map.Entry<Schema.Field, Object> entry : recordDefaults.entrySet()) {
      Schema.Field f = entry.getKey();
      // replace following with model.deepCopy once AVRO-1455 is being used
      Object defaultValue = deepCopy(f.schema(), entry.getValue());
      set(f.name(), f.pos(), defaultValue);
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

  /**
   * Converter for a list to a Java Collection.
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
  static final class AvroCollectionConverter extends GroupConverter {

    private final ParentValueContainer parent;
    private final Schema avroSchema;
    private final Converter converter;
    private Class<?> containerClass;
    private Collection<Object> container;

    public AvroCollectionConverter(ParentValueContainer parent, GroupType type,
                                   Schema avroSchema, GenericData model,
                                   Class<?> containerClass) {
      this.parent = parent;
      this.avroSchema = avroSchema;
      this.containerClass = containerClass;
      Schema elementSchema = AvroSchemaConverter.getNonNull(avroSchema.getElementType());
      Type repeatedType = type.getType(0);
      // always determine whether the repeated type is the element type by
      // matching it against the element schema.
      if (isElementType(repeatedType, elementSchema)) {
        // the element type is the repeated type (and required)
        converter = newConverter(elementSchema, repeatedType, model, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          public void add(Object value) {
            container.add(value);
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
      container = newContainer();
    }

    @Override
    public void end() {
      parent.add(container);
    }

    @SuppressWarnings("unchecked")
    private Collection<Object> newContainer() {
      if (containerClass == null) {
        return new GenericData.Array<Object>(0, avroSchema);
      } else if (containerClass.isAssignableFrom(ArrayList.class)) {
        return new ArrayList<Object>();
      } else {
        // not need to use the data model to instantiate because it resolved
        // the class, which used the correct ClassLoader
        return (Collection<Object>) ReflectData.newInstance(containerClass, avroSchema);
      }
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
          @SuppressWarnings("unchecked")
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
        container.add(element);
      }
    }
  }

  /**
   * Converter for a list to a Java array.
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
    private Class<?> elementClass;
    private Collection<?> container;

    public AvroArrayConverter(ParentValueContainer parent, GroupType type,
                              Schema avroSchema, GenericData model,
                              Class<?> arrayClass) {
      this.parent = parent;
      this.avroSchema = avroSchema;

      Preconditions.checkArgument(arrayClass.isArray(),
          "Cannot convert non-array: %s", arrayClass.getName());
      this.elementClass = arrayClass.getComponentType();

      ParentValueContainer setter = createSetterAndContainer();
      Schema elementSchema = this.avroSchema.getElementType();
      Type repeatedType = type.getType(0);

      // always determine whether the repeated type is the element type by
      // matching it against the element schema.
      if (isElementType(repeatedType, elementSchema)) {
        // the element type is the repeated type (and required)
        converter = newConverter(elementSchema, repeatedType, model, elementClass, setter);
      } else {
        // the element is wrapped in a synthetic group and may be optional
        converter = new ArrayElementConverter(
            repeatedType.asGroupType(), elementSchema, model, setter);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converter;
    }

    @Override
    public void start() {
      // end creates a new copy of the array so the container is safe to reuse
      container.clear();
    }

    @Override
    public void end() {
      if (elementClass == boolean.class) {
        parent.add(((BooleanArrayList) container).toBooleanArray());
      } else if (elementClass == byte.class) {
        parent.add(((ByteArrayList) container).toByteArray());
      } else if (elementClass == char.class) {
        parent.add(((CharArrayList) container).toCharArray());
      } else if (elementClass == short.class) {
        parent.add(((ShortArrayList) container).toShortArray());
      } else if (elementClass == int.class) {
        parent.add(((IntArrayList) container).toIntArray());
      } else if (elementClass == long.class) {
        parent.add(((LongArrayList) container).toLongArray());
      } else if (elementClass == float.class) {
        parent.add(((FloatArrayList) container).toFloatArray());
      } else if (elementClass == double.class) {
        parent.add(((DoubleArrayList) container).toDoubleArray());
      } else {
        parent.add(((ArrayList) container).toArray());
      }
    }

    @SuppressWarnings("unchecked")
    private ParentValueContainer createSetterAndContainer() {
      if (elementClass == boolean.class) {
        final BooleanArrayList list = new BooleanArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addBoolean(boolean value) {
            list.add(value);
          }
        };
      } else if (elementClass == byte.class) {
        final ByteArrayList list = new ByteArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addByte(byte value) {
            list.add(value);
          }
        };
      } else if (elementClass == char.class) {
        final CharArrayList list = new CharArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addChar(char value) {
            list.add(value);
          }
        };
      } else if (elementClass == short.class) {
        final ShortArrayList list = new ShortArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addShort(short value) {
            list.add(value);
          }
        };
      } else if (elementClass == int.class) {
        final IntArrayList list = new IntArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addInt(int value) {
            list.add(value);
          }
        };
      } else if (elementClass == long.class) {
        final LongArrayList list = new LongArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addLong(long value) {
            list.add(value);
          }
        };
      } else if (elementClass == float.class) {
        final FloatArrayList list = new FloatArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addFloat(float value) {
            list.add(value);
          }
        };
      } else if (elementClass == double.class) {
        final DoubleArrayList list = new DoubleArrayList();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void addDouble(double value) {
            list.add(value);
          }
        };
      } else {
        // this will end up as Object[]
        final List<Object> list = new ArrayList<Object>();
        this.container = list;
        return new ParentValueContainer() {
          @Override
          public void add(Object value) {
            list.add(value);
          }
        };
      }

    }

    /**
     * Converter for primitive list elements.
     *
     * <pre>
     *   optional group the_list (LIST) {
     *     repeated group array { <-- this layer
     *       optional (type) element;
     *     }
     *   }
     * </pre>
     */
    final class ArrayElementConverter extends GroupConverter {
      private boolean isSet;
      private final Converter elementConverter;

      public ArrayElementConverter(GroupType repeatedType,
                                   Schema elementSchema, GenericData model,
                                   final ParentValueContainer setter) {
        Type elementType = repeatedType.getType(0);
        Preconditions.checkArgument(
            !elementClass.isPrimitive() || elementType.isRepetition(REQUIRED),
            "Cannot convert list of optional elements to primitive array");
        Schema nonNullElementSchema = AvroSchemaConverter.getNonNull(elementSchema);
        this.elementConverter = newConverter(
            nonNullElementSchema, elementType, model, elementClass, new ParentValueContainer() {
              @Override
              public void add(Object value) {
                isSet = true;
                setter.add(value);
              }

              @Override
              public void addByte(byte value) {
                isSet = true;
                setter.addByte(value);
              }

              @Override
              public void addBoolean(boolean value) {
                isSet = true;
                setter.addBoolean(value);
              }

              @Override
              public void addChar(char value) {
                isSet = true;
                setter.addChar(value);
              }

              @Override
              public void addShort(short value) {
                isSet = true;
                setter.addShort(value);
              }

              @Override
              public void addInt(int value) {
                isSet = true;
                setter.addInt(value);
              }

              @Override
              public void addLong(long value) {
                isSet = true;
                setter.addLong(value);
              }

              @Override
              public void addFloat(float value) {
                isSet = true;
                setter.addFloat(value);
              }

              @Override
              public void addDouble(double value) {
                isSet = true;
                setter.addDouble(value);
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
        isSet = false;
      }

      @Override
      public void end() {
        if (!isSet) {
          container.add(null);
        }
      }
    }
  }

  // Converter used to test whether a requested schema is a 2-level schema.
  // This is used to convert the file's type assuming that the file uses
  // 2-level lists and the result is checked to see if it matches the requested
  // element type. This should always convert assuming 2-level lists because
  // 2-level and 3-level can't be mixed.
  private static final AvroSchemaConverter CONVERTER =
      new AvroSchemaConverter(true);

  /**
   * Returns whether the given type is the element type of a list or is a
   * synthetic group with one field that is the element type. This is
   * determined by checking whether the type can be a synthetic group and by
   * checking whether a potential synthetic group matches the expected schema.
   * <p>
   * Unlike {@link AvroSchemaConverter#isElementType(Type, String)}, this
   * method never guesses because the expected schema is known.
   *
   * @param repeatedType a type that may be the element type
   * @param elementSchema the expected Schema for list elements
   * @return {@code true} if the repeatedType is the element schema
   */
  static boolean isElementType(Type repeatedType, Schema elementSchema) {
    if (repeatedType.isPrimitive() ||
        repeatedType.asGroupType().getFieldCount() > 1 ||
        repeatedType.asGroupType().getType(0).isRepetition(REPEATED)) {
      // The repeated type must be the element type because it is an invalid
      // synthetic wrapper. Must be a group with one optional or required field
      return true;
    } else if (elementSchema != null &&
        elementSchema.getType() == Schema.Type.RECORD) {
      Schema schemaFromRepeated = CONVERTER.convert(repeatedType.asGroupType());
      if (checkReaderWriterCompatibility(elementSchema, schemaFromRepeated)
          .getType() == COMPATIBLE) {
        return true;
      }
    }
    return false;
  }

  static final class AvroUnionConverter extends AvroConverters.AvroGroupConverter {
    private final Converter[] memberConverters;
    private Object memberValue = null;

    public AvroUnionConverter(ParentValueContainer parent, Type parquetSchema,
                              Schema avroSchema, GenericData model) {
      super(parent);
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
              Preconditions.checkArgument(
                  AvroUnionConverter.this.memberValue == null,
                  "Union is resolving to more than one type");
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

  static final class MapConverter<K, V> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter keyValueConverter;
    private final Schema schema;
    private final Class<?> mapClass;
    private Map<K, V> map;

    public MapConverter(ParentValueContainer parent, GroupType mapType,
        Schema mapSchema, GenericData model) {
      this.parent = parent;
      GroupType repeatedKeyValueType = mapType.getType(0).asGroupType();
      this.keyValueConverter = new MapKeyValueConverter(
          repeatedKeyValueType, mapSchema, model);
      this.schema = mapSchema;
      this.mapClass = getDatumClass(mapSchema, model);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return keyValueConverter;
    }

    @Override
    public void start() {
      this.map = newMap();
    }

    @Override
    public void end() {
      parent.add(map);
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> newMap() {
      if (mapClass == null || mapClass.isAssignableFrom(HashMap.class)) {
        return new HashMap<K, V>();
      } else {
        return (Map<K, V>) ReflectData.newInstance(mapClass, schema);
      }
    }

    final class MapKeyValueConverter extends GroupConverter {

      private K key;
      private V value;
      private final Converter keyConverter;
      private final Converter valueConverter;

      public MapKeyValueConverter(GroupType keyValueType, Schema mapSchema,
          GenericData model) {
        keyConverter = newStringConverter(mapSchema, model,
            new ParentValueContainer() {
              @Override
              @SuppressWarnings("unchecked")
              public void add(Object value) {
                MapKeyValueConverter.this.key = (K) value;
              }
            });

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
