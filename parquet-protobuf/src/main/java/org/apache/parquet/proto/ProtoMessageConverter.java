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
package org.apache.parquet.proto;

import com.google.protobuf.*;
import com.google.protobuf.util.Timestamps;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.example.data.simple.BooleanValue;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.IncompatibleSchemaModificationException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import static java.util.Optional.of;
import static org.apache.parquet.proto.ProtoConstants.CONFIG_ACCEPT_UNKNOWN_ENUM;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_ITEM_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_KEY_VALUE_SEPARATOR;
import static org.apache.parquet.proto.ProtoConstants.METADATA_ENUM_PREFIX;

/**
 * Converts Protocol Buffer message (both top level and inner) to parquet.
 * This is internal class, use {@link ProtoRecordConverter}.
 */
class ProtoMessageConverter extends GroupConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoMessageConverter.class);

  protected final Configuration conf;
  protected final Converter[] converters;
  protected final ParentValueContainer parent;
  protected final Message.Builder myBuilder;
  protected final Map<String, String> extraMetadata;

  /**
   * Used in record converter.
   *
   * @param conf Configuration for some customizable behavior,
   *            eg. "parquet.proto.accept.unknown.enum" - whether to accept an unparsable (after trying with proto enum label and number) enum as `UNKNOWN` with a number -1 (the one generated automatically for each proto enum)
   * @param pvc The parent value containing the converted proto
   * @param protoClass The class of the converted proto
   * @param parquetSchema The (part of) parquet schema that should match to the expected proto
   * @param extraMetadata Metadata from parquet footer, containing useful information about parquet-proto convertion behavior
   */
  ProtoMessageConverter(Configuration conf, ParentValueContainer pvc, Class<? extends Message> protoClass, GroupType parquetSchema, Map<String, String> extraMetadata) {
    this(conf, pvc, Protobufs.getMessageBuilder(protoClass), parquetSchema, extraMetadata);
  }

  // For usage in message arrays
  ProtoMessageConverter(Configuration conf, ParentValueContainer pvc, Message.Builder builder, GroupType parquetSchema, Map<String, String> extraMetadata) {

    int schemaSize = parquetSchema.getFieldCount();
    converters = new Converter[schemaSize];
    this.conf = conf;
    this.parent = pvc;
    this.extraMetadata = extraMetadata;
    int parquetFieldIndex = 1;

    if (pvc == null) {
      throw new IllegalStateException("Missing parent value container");
    }

    myBuilder = builder;

    Descriptors.Descriptor protoDescriptor = builder.getDescriptorForType();

    for (Type parquetField : parquetSchema.getFields()) {
      Descriptors.FieldDescriptor protoField = protoDescriptor.findFieldByName(parquetField.getName());

      if (protoField == null) {
        String description = "Scheme mismatch \n\"" + parquetField + "\"" +
                "\n proto descriptor:\n" + protoDescriptor.toProto();
        throw new IncompatibleSchemaModificationException("Cant find \"" + parquetField.getName() + "\" " + description);
      }

      converters[parquetFieldIndex - 1] = newMessageConverter(myBuilder, protoField, parquetField);

      parquetFieldIndex++;
    }
  }


  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {

  }

  @Override
  public void end() {
    parent.add(myBuilder.build());
    myBuilder.clear();
  }

  protected Converter newMessageConverter(final Message.Builder parentBuilder, final Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {

    boolean isRepeated = fieldDescriptor.isRepeated();

    ParentValueContainer parent;

    if (isRepeated) {
      parent = new ParentValueContainer() {
        @Override
        public void add(Object value) {
          parentBuilder.addRepeatedField(fieldDescriptor, value);
        }
      };
    } else {
      parent = new ParentValueContainer() {
        @Override
        public void add(Object value) {
          parentBuilder.setField(fieldDescriptor, value);
        }
      };
    }

    LogicalTypeAnnotation logicalTypeAnnotation = parquetType.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      return newScalarConverter(parent, parentBuilder, fieldDescriptor, parquetType);
    }

    return logicalTypeAnnotation.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
        return of(new ListConverter(parentBuilder, fieldDescriptor, parquetType));
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
        return of(new MapConverter(parentBuilder, fieldDescriptor, parquetType));
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
        return of(new ProtoTimestampConverter(parent, timestampLogicalType));
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
        return of(new ProtoDateConverter(parent));
      }

      @Override
      public Optional<Converter> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
        return of(new ProtoTimeConverter(parent, timeLogicalType));
      }
    }).orElseGet(() -> newScalarConverter(parent, parentBuilder, fieldDescriptor, parquetType));
  }

  protected Converter newScalarConverter(ParentValueContainer pvc, Message.Builder parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {

    JavaType javaType = fieldDescriptor.getJavaType();

    switch (javaType) {
      case STRING: return new ProtoStringConverter(pvc);
      case FLOAT: return new ProtoFloatConverter(pvc);
      case DOUBLE: return new ProtoDoubleConverter(pvc);
      case BOOLEAN: return new ProtoBooleanConverter(pvc);
      case BYTE_STRING: return new ProtoBinaryConverter(pvc);
      case ENUM: return new ProtoEnumConverter(pvc, fieldDescriptor);
      case INT: return new ProtoIntConverter(pvc);
      case LONG: return new ProtoLongConverter(pvc);
      case MESSAGE: {
        Message.Builder subBuilder = parentBuilder.newBuilderForField(fieldDescriptor);
        String typeName = fieldDescriptor.getMessageType().getFullName();
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_DOUBLE_TYPE)) {
          return new ProtoDoubleValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_FLOAT_TYPE)) {
          return new ProtoFloatValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_INT64_TYPE)) {
          return new ProtoInt64ValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_UINT64_TYPE)) {
          return new ProtoUInt64ValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_INT32_TYPE)) {
          return new ProtoInt32ValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_UINT32_TYPE)) {
          return new ProtoUInt32ValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_BOOL_TYPE)) {
          return new ProtoBoolValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_STRING_TYPE)) {
          return new ProtoStringValueConverter(pvc);
        }
        if (typeName.equals(ProtoSchemaConverter.PROTOBUF_BYTES_TYPE)) {
          return new ProtoBytesValueConverter(pvc);
        }
        return new ProtoMessageConverter(conf, pvc, subBuilder, parquetType.asGroupType(), extraMetadata);
      }
    }

    throw new UnsupportedOperationException(String.format("Cannot convert type: %s" +
            " (Parquet type: %s) ", javaType, parquetType));
  }

  public Message.Builder getBuilder() {
    return myBuilder;
  }

  static abstract class ParentValueContainer {

    /**
     * Adds the value to the parent.
     */
    public abstract void add(Object value);

  }

  final class ProtoEnumConverter extends PrimitiveConverter {

    private final Descriptors.FieldDescriptor fieldType;
    private final Map<Binary, Descriptors.EnumValueDescriptor> enumLookup;
    private Descriptors.EnumValueDescriptor[] dict;
    private final ParentValueContainer parent;
    private final Descriptors.EnumDescriptor enumType;
    private final String unknownEnumPrefix;
    private final boolean acceptUnknownEnum;

    public ProtoEnumConverter(ParentValueContainer parent, Descriptors.FieldDescriptor fieldType) {
      this.parent = parent;
      this.fieldType = fieldType;
      this.enumType = fieldType.getEnumType();
      this.enumLookup = makeLookupStructure(enumType);
      unknownEnumPrefix = "UNKNOWN_ENUM_VALUE_" + enumType.getName() + "_";
      acceptUnknownEnum = conf.getBoolean(CONFIG_ACCEPT_UNKNOWN_ENUM, false);
    }

    /**
     * Fills lookup structure for translating between parquet enum values and Protocol buffer enum values.
     * */
    private Map<Binary, Descriptors.EnumValueDescriptor> makeLookupStructure(Descriptors.EnumDescriptor enumType) {
      Map<Binary, Descriptors.EnumValueDescriptor> lookupStructure = new HashMap<Binary, Descriptors.EnumValueDescriptor>();

      if (extraMetadata.containsKey(METADATA_ENUM_PREFIX + enumType.getFullName())) {
        String enumNameNumberPairs = extraMetadata.get(METADATA_ENUM_PREFIX + enumType.getFullName());
        if (enumNameNumberPairs == null || enumNameNumberPairs.trim().isEmpty()) {
          LOG.debug("No enum is written for " + enumType.getFullName());
          return lookupStructure;
        }
        for (String enumItem : enumNameNumberPairs.split(METADATA_ENUM_ITEM_SEPARATOR)) {
          String[] nameAndNumber = enumItem.split(METADATA_ENUM_KEY_VALUE_SEPARATOR);
          if (nameAndNumber.length != 2) {
            throw new BadConfigurationException("Invalid enum bookkeeper from the metadata: " + enumNameNumberPairs);
          }
          lookupStructure.put(Binary.fromString(nameAndNumber[0]), enumType.findValueByNumberCreatingIfUnknown(Integer.parseInt(nameAndNumber[1])));
        }
      } else {
        List<Descriptors.EnumValueDescriptor> enumValues = enumType.getValues();

        for (Descriptors.EnumValueDescriptor value : enumValues) {
          String name = value.getName();
          lookupStructure.put(Binary.fromString(name), enumType.findValueByName(name));
        }
      }

      return lookupStructure;
    }

    /**
     * Translates given parquet enum value to protocol buffer enum value.
     * @throws org.apache.parquet.io.InvalidRecordException is there is no corresponding value.
     * */
    private Descriptors.EnumValueDescriptor translateEnumValue(Binary binaryValue) {
      Descriptors.EnumValueDescriptor protoValue = enumLookup.get(binaryValue);

      if (protoValue == null) {
        // in case of unknown enum value, protobuf is creating new EnumValueDescriptor with the unknown number
        // and name as following "UNKNOWN_ENUM_VALUE_" + parent.getName() + "_" + number
        // so the idea is to parse the name for data created by parquet-proto before this patch
        String unknownLabel = binaryValue.toStringUsingUTF8();
        if (unknownLabel.startsWith(unknownEnumPrefix)) {
          try {
            int i = Integer.parseInt(unknownLabel.substring(unknownEnumPrefix.length()));
            Descriptors.EnumValueDescriptor unknownEnumValue = enumType.findValueByNumberCreatingIfUnknown(i);
            // build new EnumValueDescriptor and put it in the value cache
            enumLookup.put(binaryValue, unknownEnumValue);
            return unknownEnumValue;
          } catch (NumberFormatException e) {
            // The value does not respect "UNKNOWN_ENUM_VALUE_" + parent.getName() + "_" + number pattern
            // We accept it as unknown enum with number -1.
          }
        }
        if (!acceptUnknownEnum) {
          // Safe mode, when an enum does not have its number in metadata (data written before this fix), and its label
          // is unrecognizable (neither defined in the schema, nor parsable with "UNKNOWN_ENUM_*" pattern, which means
          // probably the reader schema is not up-to-date), we reject with an error.
          Set<Binary> knownValues = enumLookup.keySet();
          String msg = "Illegal enum value \"" + binaryValue + "\""
            + " in protocol buffer \"" + fieldType.getFullName() + "\""
            + " legal values are: \"" + knownValues + "\"";
          throw new InvalidRecordException(msg);
        }
        LOG.error("Found unknown value " +  unknownLabel + " for field " + fieldType.getFullName() +
          " probably because your proto schema is outdated, accept it as unknown enum with number -1");
        Descriptors.EnumValueDescriptor unrecognized = enumType.findValueByNumberCreatingIfUnknown(-1);
        enumLookup.put(binaryValue, unrecognized);
        return unrecognized;
      }
      return protoValue;
    }

    @Override
    final public void addBinary(Binary binaryValue) {
      Descriptors.EnumValueDescriptor protoValue = translateEnumValue(binaryValue);
      parent.add(protoValue);
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      parent.add(dict[dictionaryId]);
    }

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      dict = new  Descriptors.EnumValueDescriptor[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        Binary binaryValue = dictionary.decodeToBinary(i);
        dict[i] = translateEnumValue(binaryValue);
      }
    }

  }

  final class ProtoBinaryConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoBinaryConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addBinary(Binary binary) {
      ByteString byteString = ByteString.copyFrom(binary.toByteBuffer());
      parent.add(byteString);
    }
  }


  final class ProtoBooleanConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoBooleanConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(value);
    }

  }

  final class ProtoDoubleConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoDoubleConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addDouble(double value) {
      parent.add(value);
    }
  }

  final class ProtoFloatConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoFloatConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addFloat(float value) {
      parent.add(value);
    }
  }

  final class ProtoIntConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoIntConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addInt(int value) {
      parent.add(value);
    }
  }

  final class ProtoLongConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoLongConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addLong(long value) {
      parent.add(value);
    }
  }

  final class ProtoStringConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoStringConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addBinary(Binary binary) {
      String str = binary.toStringUsingUTF8();
      parent.add(str);
    }

  }

  final class ProtoTimestampConverter extends PrimitiveConverter {

    final ParentValueContainer parent;
    final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalTypeAnnotation;

    public ProtoTimestampConverter(ParentValueContainer parent, LogicalTypeAnnotation.TimestampLogicalTypeAnnotation logicalTypeAnnotation) {
      this.parent = parent;
      this.logicalTypeAnnotation = logicalTypeAnnotation;
    }

    @Override
    public void addLong(long value) {
      switch (logicalTypeAnnotation.getUnit()) {
        case MICROS:
          parent.add(Timestamps.fromMicros(value));
          break;
        case MILLIS:
          parent.add(Timestamps.fromMillis(value));
          break;
        case NANOS:
          parent.add(Timestamps.fromNanos(value));
          break;
      }
    }
  }

  final class ProtoDateConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoDateConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addInt(int value) {
      LocalDate localDate = LocalDate.ofEpochDay(value);
      com.google.type.Date date = com.google.type.Date.newBuilder()
        .setYear(localDate.getYear())
        .setMonth(localDate.getMonthValue())
        .setDay(localDate.getDayOfMonth())
        .build();
      parent.add(date);
    }
  }

  final class ProtoTimeConverter extends PrimitiveConverter {

    final ParentValueContainer parent;
    final LogicalTypeAnnotation.TimeLogicalTypeAnnotation logicalTypeAnnotation;

    public ProtoTimeConverter(ParentValueContainer parent, LogicalTypeAnnotation.TimeLogicalTypeAnnotation logicalTypeAnnotation) {
      this.parent = parent;
      this.logicalTypeAnnotation = logicalTypeAnnotation;
    }

    @Override
    public void addLong(long value) {
      LocalTime localTime;
      switch (logicalTypeAnnotation.getUnit()) {
        case MILLIS:
          localTime = LocalTime.ofNanoOfDay(value * 1_000_000);
          break;
        case MICROS:
          localTime = LocalTime.ofNanoOfDay(value * 1_000);
          break;
        case NANOS:
          localTime = LocalTime.ofNanoOfDay(value);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized TimeUnit: " + logicalTypeAnnotation.getUnit());
      }
      com.google.type.TimeOfDay timeOfDay = com.google.type.TimeOfDay.newBuilder()
        .setHours(localTime.getHour())
        .setMinutes(localTime.getMinute())
        .setSeconds(localTime.getSecond())
        .setNanos(localTime.getNano())
        .build();
      parent.add(timeOfDay);
    }
  }

  final class ProtoDoubleValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoDoubleValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addDouble(double value) {
      parent.add(DoubleValue.of(value));
    }
  }

  final class ProtoFloatValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoFloatValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addFloat(float value) {
      parent.add(FloatValue.of(value));
    }
  }

  final class ProtoInt64ValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoInt64ValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addLong(long value) {
      parent.add(Int64Value.of(value));
    }
  }

  final class ProtoUInt64ValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoUInt64ValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addLong(long value) {
      parent.add(UInt64Value.of(value));
    }
  }

  final class ProtoInt32ValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoInt32ValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addInt(int value) {
      parent.add(Int32Value.of(value));
    }
  }

  final class ProtoUInt32ValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoUInt32ValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addLong(long value) {
      parent.add(UInt32Value.of((int) value));
    }
  }

  final class ProtoBoolValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoBoolValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(BoolValue.of(value));
    }

  }

  final class ProtoStringValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoStringValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addBinary(Binary binary) {
      String str = binary.toStringUsingUTF8();
      parent.add(StringValue.of(str));
    }

  }

  final class ProtoBytesValueConverter extends PrimitiveConverter {

    final ParentValueContainer parent;

    public ProtoBytesValueConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    public void addBinary(Binary binary) {
      ByteString byteString = ByteString.copyFrom(binary.toByteBuffer());
      parent.add(BytesValue.of(byteString));
    }
  }

  /**
   * This class unwraps the additional LIST wrapper and makes it possible to read the underlying data and then convert
   * it to protobuf.
   * <p>
   * Consider the following protobuf schema:
   * message SimpleList {
   *   repeated int64 first_array = 1;
   * }
   * <p>
   * A LIST wrapper is created in parquet for the above mentioned protobuf schema:
   * message SimpleList {
   *   optional group first_array (LIST) = 1 {
   *     repeated group list {
   *         optional int32 element;
   *     }
   *   }
   * }
   * <p>
   * The LIST wrappers are used by 3rd party tools, such as Hive, to read parquet arrays. The wrapper contains
   * a repeated group named 'list', itself containing only one field called 'element' of the type of the repeated
   * object (can be a primitive as in this example or a group in case of a repeated message in protobuf).
   */
  final class ListConverter extends GroupConverter {
    private final Converter converter;

    public ListConverter(Message.Builder parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {
      LogicalTypeAnnotation logicalTypeAnnotation = parquetType.getLogicalTypeAnnotation();
      if (!(logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) || parquetType.isPrimitive()) {
        throw new ParquetDecodingException("Expected LIST wrapper. Found: " + logicalTypeAnnotation + " instead.");
      }

      GroupType rootWrapperType = parquetType.asGroupType();
      if (!rootWrapperType.containsField("list") || rootWrapperType.getType("list").isPrimitive()) {
        throw new ParquetDecodingException("Expected repeated 'list' group inside LIST wrapperr but got: " + rootWrapperType);
      }

      GroupType listType = rootWrapperType.getType("list").asGroupType();
      if (!listType.containsField("element")) {
        throw new ParquetDecodingException("Expected 'element' inside repeated list group but got: " + listType);
      }

      Type elementType = listType.getType("element");
      converter = newMessageConverter(parentBuilder, fieldDescriptor, elementType);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex > 0) {
        throw new ParquetDecodingException("Unexpected multiple fields in the LIST wrapper");
      }

      return new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
          return converter;
        }

        @Override
        public void start() {

        }

        @Override
        public void end() {

        }
      };
    }

    @Override
    public void start() {

    }

    @Override
    public void end() {

    }
  }


  final class MapConverter extends GroupConverter {
    private final Converter converter;

    public MapConverter(Message.Builder parentBuilder, Descriptors.FieldDescriptor fieldDescriptor, Type parquetType) {
      LogicalTypeAnnotation logicalTypeAnnotation = parquetType.getLogicalTypeAnnotation();
      if (!(logicalTypeAnnotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation)) {
        throw new ParquetDecodingException("Expected MAP wrapper. Found: " + logicalTypeAnnotation + " instead.");
      }

      Type parquetSchema;
      if (parquetType.asGroupType().containsField("key_value")){
        parquetSchema = parquetType.asGroupType().getType("key_value");
      } else {
        throw new ParquetDecodingException("Expected map but got: " + parquetType);
      }

      converter = newMessageConverter(parentBuilder, fieldDescriptor, parquetSchema);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex > 0) {
        throw new ParquetDecodingException("Unexpected multiple fields in the MAP wrapper");
      }
      return converter;
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
    }
  }
}
