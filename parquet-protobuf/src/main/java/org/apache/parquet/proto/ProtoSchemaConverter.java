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

import com.google.common.collect.ImmutableSetMultimap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.Builder;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nullable;

import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Converts a Protocol Buffer Descriptor into a Parquet schema.
 */
public class ProtoSchemaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoSchemaConverter.class);
  public static final String PB_MAX_RECURSION = "parquet.proto.maxRecursion";

  private final boolean parquetSpecsCompliant;
  // TODO: use proto custom options to override per field.
  private final int maxRecursion;

  /**
   * Instantiate a schema converter to get the parquet schema corresponding to protobuf classes.
   * Returns instances that are not parquetSpecsCompliant with a maxRecursion of 5.
   */
  public ProtoSchemaConverter() {
    this(false);
  }

  /**
   * Instantiate a schema converter to get the parquet schema corresponding to protobuf classes.
   * Returns instances limited to 5 levels of recursion depth.
   *
   * @param parquetSpecsCompliant   If set to false, the parquet schema generated will be using the old
   *                                schema style (prior to PARQUET-968) to provide backward-compatibility
   *                                but which does not use LIST and MAP wrappers around collections as required
   *                                by the parquet specifications. If set to true, specs compliant schemas are used.
   */
  public ProtoSchemaConverter(boolean parquetSpecsCompliant) {
    this(parquetSpecsCompliant, 5);
  }

  /**
   * Instantiate a schema converter to get the parquet schema corresponding to protobuf classes.
   * Returns instances that are not specs compliant and limited to 5 levels of recursion depth.
   *
   * @param config   Hadoop configuration object to parrse parquetSpecsCompliant and maxRecursion settings.
   */
  public ProtoSchemaConverter(Configuration config) {
    this(
        config.getBoolean(ProtoWriteSupport.PB_SPECS_COMPLIANT_WRITE, false),
        config.getInt(PB_MAX_RECURSION, 5));
  }

  /**
   * Instantiate a schema converter to get the parquet schema corresponding to protobuf classes.
   *
   * @param parquetSpecsCompliant   If set to false, the parquet schema generated will be using the old
   *                                schema style (prior to PARQUET-968) to provide backward-compatibility
   *                                but which does not use LIST and MAP wrappers around collections as required
   *                                by the parquet specifications. If set to true, specs compliant schemas are used.
   * @param maxRecursion            The maximum recursion depth messages are allowed to go before terminating as
   *                                bytes instead of their actual schema.
   */
  public ProtoSchemaConverter(boolean parquetSpecsCompliant, int maxRecursion) {
    this.parquetSpecsCompliant = parquetSpecsCompliant;
    this.maxRecursion = maxRecursion;
  }

  /**
   * Sets the maximum recursion depth for recursive schemas.
   *
   * @param config        The hadoop configuration to be updated.
   * @param maxRecursion  The maximum recursion depth messages are allowed to go before terminating as
   *                      bytes instead of their actual schema.
   */
  public static void setMaxRecursion(Configuration config, int maxRecursion) {
    config.setInt(PB_MAX_RECURSION, maxRecursion);
  }

  /**
   * Converts a given protobuf message descriptor to a parquet schema.
   *
   * @param descriptor  The protobuf message descriptor to convert.
   * @return The parquet schema encoded as a MessageType.
   */
  public MessageType convert(Descriptors.Descriptor descriptor) {
    // Remember classes seen with depths to avoid cycles.
    int depth = 0;
    ImmutableSetMultimap<String, Integer> seen = ImmutableSetMultimap.of(descriptor.getFullName(), depth);
    LOG.trace("convert:\n{}", descriptor.toProto());
    MessageType messageType = convertFields(Types.buildMessage(), descriptor.getFields(), seen, depth)
        .named(descriptor.getFullName());
    LOG.debug("Converter info:\n{}\n  was converted to:\n{}", descriptor.toProto(), messageType);
    return messageType;
  }

  /**
   * Converts a given protobuf message class to a parquet schema.
   *
   * @param protobufClass  The protobuf message class (e.g. MyMessage.class) to convert.
   * @return The parquet schema encoded as a MessageType.
   */
  public MessageType convert(Class<? extends Message> protobufClass) {
    LOG.debug("Converting protocol buffer class \"{}\" to parquet schema", protobufClass);
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
    return convert(descriptor);
  }

  /* Iterates over list of fields. **/
  private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, List<FieldDescriptor> fieldDescriptors, ImmutableSetMultimap<String, Integer> seen, int depth) {
    for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
      groupBuilder = addField(fieldDescriptor, groupBuilder, seen, depth)
          .id(fieldDescriptor.getNumber())
          .named(fieldDescriptor.getName());
    }
    return groupBuilder;
  }

  private static Type.Repetition getRepetition(FieldDescriptor descriptor) {
    if (descriptor.isRequired()) {
      return Type.Repetition.REQUIRED;
    } else if (descriptor.isRepeated()) {
      return Type.Repetition.REPEATED;
    } else {
      return Type.Repetition.OPTIONAL;
    }
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(FieldDescriptor descriptor, final GroupBuilder<T> builder, ImmutableSetMultimap<String, Integer> seen, int depth) {
    if (descriptor.getJavaType() == JavaType.MESSAGE) {
      return addMessageField(descriptor, builder, seen, depth);
    }

    ParquetType parquetType = getParquetType(descriptor);
    if (descriptor.isRepeated() && parquetSpecsCompliant) {
      // the old schema style did not include the LIST wrapper around repeated fields
      return addRepeatedPrimitive(parquetType.primitiveType, parquetType.logicalTypeAnnotation, builder);
    }

    return builder.primitive(parquetType.primitiveType, getRepetition(descriptor)).as(parquetType.logicalTypeAnnotation);
  }

  private static <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addRepeatedPrimitive(PrimitiveTypeName primitiveType,
                                                                                                   LogicalTypeAnnotation logicalTypeAnnotation,
                                                                                                   final GroupBuilder<T> builder) {
    return builder
        .group(Type.Repetition.OPTIONAL).as(listType())
          .group(Type.Repetition.REPEATED)
            .primitive(primitiveType, Type.Repetition.REQUIRED).as(logicalTypeAnnotation)
          .named("element")
        .named("list");
  }

  private <T> GroupBuilder<GroupBuilder<T>> addRepeatedMessage(FieldDescriptor descriptor, GroupBuilder<T> builder, ImmutableSetMultimap<String, Integer> seen, int depth) {
    GroupBuilder<GroupBuilder<GroupBuilder<GroupBuilder<T>>>> result = builder
        .group(Type.Repetition.OPTIONAL).as(listType())
        .group(Type.Repetition.REPEATED)
        .group(Type.Repetition.OPTIONAL);

    convertFields(result, descriptor.getMessageType().getFields(), seen, depth);

    return result.named("element").named("list");
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addMessageField(FieldDescriptor descriptor, final GroupBuilder<T> builder, ImmutableSetMultimap<String, Integer> seen, int depth) {
    // Prevent recursion by terminating with optional proto bytes.
    depth += 1;
    String typeName = getInnerTypeName(descriptor);
    LOG.trace("addMessageField: {} type: {} depth: {}", descriptor.getFullName(), typeName, depth);
    if (typeName != null) {
      if (seen.get(typeName).size() > maxRecursion) {
        return builder.primitive(BINARY, Type.Repetition.OPTIONAL).as((LogicalTypeAnnotation) null);
      }
    }

    if (descriptor.isMapField() && parquetSpecsCompliant) {
      // the old schema style did not include the MAP wrapper around map groups
      return addMapField(descriptor, builder, seen, depth);
    }

    seen = ImmutableSetMultimap.<String, Integer>builder().putAll(seen).put(typeName, depth).build();

    if (descriptor.isRepeated() && parquetSpecsCompliant) {
      // the old schema style did not include the LIST wrapper around repeated messages
      return addRepeatedMessage(descriptor, builder, seen, depth);
    }

    // Plain message.
    GroupBuilder<GroupBuilder<T>> group = builder.group(getRepetition(descriptor));
    convertFields(group, descriptor.getMessageType().getFields(), seen, depth);
    return group;
  }

  @Nullable
  private String getInnerTypeName(FieldDescriptor descriptor) {
    if (descriptor.isMapField() && parquetSpecsCompliant) {
      descriptor = descriptor.getMessageType().getFields().get(1);
    }
    if (descriptor.getJavaType() != JavaType.MESSAGE) {
      LOG.trace("getInnerTypeName: {} => primitive", descriptor.getFullName());
      return null;
    }
    String name = descriptor.getMessageType().getFullName();
    LOG.trace("getInnerTypeName: {} => {}", descriptor.getFullName(), name);
    return name;
  }

  private <T> GroupBuilder<GroupBuilder<T>> addMapField(FieldDescriptor descriptor, final GroupBuilder<T> builder, ImmutableSetMultimap<String, Integer> seen, int depth) {
    List<FieldDescriptor> fields = descriptor.getMessageType().getFields();
    if (fields.size() != 2) {
      throw new UnsupportedOperationException("Expected two fields for the map (key/value), but got: " + fields);
    }

    ParquetType mapKeyParquetType = getParquetType(fields.get(0));

    GroupBuilder<GroupBuilder<GroupBuilder<T>>> group = builder
        .group(Type.Repetition.OPTIONAL).as(mapType()) // only optional maps are allowed in Proto3
        .group(Type.Repetition.REPEATED) // key_value wrapper
        .primitive(mapKeyParquetType.primitiveType, Type.Repetition.REQUIRED).as(mapKeyParquetType.logicalTypeAnnotation).named("key");

    return addField(fields.get(1), group, seen, depth)
        .named("value")
        .named("key_value");
  }

  private static ParquetType getParquetType(FieldDescriptor fieldDescriptor) {
    JavaType javaType = fieldDescriptor.getJavaType();
    switch (javaType) {
      case INT: return ParquetType.of(INT32);
      case LONG: return ParquetType.of(INT64);
      case DOUBLE: return ParquetType.of(DOUBLE);
      case BOOLEAN: return ParquetType.of(BOOLEAN);
      case FLOAT: return ParquetType.of(FLOAT);
      case STRING: return ParquetType.of(BINARY, stringType());
      case ENUM: return ParquetType.of(BINARY, enumType());
      case BYTE_STRING: return ParquetType.of(BINARY);
      default:
        throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType);
    }
  }

  private static class ParquetType {
    PrimitiveTypeName primitiveType;
    LogicalTypeAnnotation logicalTypeAnnotation;

    private ParquetType(PrimitiveTypeName primitiveType, LogicalTypeAnnotation logicalTypeAnnotation) {
      this.primitiveType = primitiveType;
      this.logicalTypeAnnotation = logicalTypeAnnotation;
    }

    public static ParquetType of(PrimitiveTypeName primitiveType, LogicalTypeAnnotation logicalTypeAnnotation) {
      return new ParquetType(primitiveType, logicalTypeAnnotation);
    }

    public static ParquetType of(PrimitiveTypeName primitiveType) {
      return of(primitiveType, null);
    }
  }
}
