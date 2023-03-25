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

package org.apache.parquet.cli.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.parquet.cli.util.RecordException;
import org.apache.parquet.cli.util.RuntimeIOException;
import org.apache.parquet.cli.util.Schemas;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AvroJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory FACTORY = new JsonFactory(MAPPER);

  public static Iterator<JsonNode> parser(final InputStream stream) {
    try {
      // Don't close the parser until the iterator has been consumed
      JsonParser parser = FACTORY.createParser(stream);
      return parser.readValuesAs(JsonNode.class);
    } catch (IOException e) {
      throw new RuntimeIOException("Cannot read from stream", e);
    }
  }

  public static JsonNode parse(String json) {
    return parse(json, JsonNode.class);
  }

  public static <T> T parse(String json, Class<T> returnType) {
    try {
      return MAPPER.readValue(json, returnType);
    } catch (JsonParseException | JsonMappingException e) {
      throw new IllegalArgumentException("Invalid JSON", e);
    } catch (IOException e) {
      throw new RuntimeIOException("Cannot initialize JSON parser", e);
    }
  }

  public static JsonNode parse(InputStream json) {
    return parse(json, JsonNode.class);
  }

  public static <T> T parse(InputStream json, Class<T> returnType) {
    try {
      return MAPPER.readValue(json, returnType);
    } catch (JsonParseException | JsonMappingException e) {
      throw new IllegalArgumentException("Invalid JSON stream", e);
    } catch (IOException e) {
      throw new RuntimeIOException("Cannot initialize JSON parser", e);
    }
  }

  public static Object convertToAvro(GenericData model, JsonNode datum,
                                     Schema schema) {
    if (datum == null) {
      return null;
    }
    switch (schema.getType()) {
      case RECORD:
        RecordException.check(datum.isObject(),
            "Cannot convert non-object to record: %s", datum);
        Object record = model.newRecord(null, schema);
        for (Schema.Field field : schema.getFields()) {
          model.setField(record, field.name(), field.pos(),
              convertField(model, datum.get(field.name()), field));
        }
        return record;

      case MAP:
        RecordException.check(datum.isObject(),
            "Cannot convert non-object to map: %s", datum);
        Map<String, Object> map = Maps.newLinkedHashMap();
        Iterator<Map.Entry<String, JsonNode>> iter = datum.fields();
        while (iter.hasNext()) {
          Map.Entry<String, JsonNode> entry = iter.next();
          map.put(entry.getKey(), convertToAvro(
              model, entry.getValue(), schema.getValueType()));
        }
        return map;

      case ARRAY:
        RecordException.check(datum.isArray(),
            "Cannot convert to array: %s", datum);
        List<Object> list = Lists.newArrayListWithExpectedSize(datum.size());
        for (JsonNode element : datum) {
          list.add(convertToAvro(model, element, schema.getElementType()));
        }
        return list;

      case UNION:
        return convertToAvro(model, datum,
            resolveUnion(datum, schema.getTypes()));

      case BOOLEAN:
        RecordException.check(datum.isBoolean(),
            "Cannot convert to boolean: %s", datum);
        return datum.booleanValue();

      case FLOAT:
        RecordException.check(datum.isFloat() || datum.isInt(),
            "Cannot convert to float: %s", datum);
        return datum.floatValue();

      case DOUBLE:
        RecordException.check(
            datum.isDouble() || datum.isFloat() ||
            datum.isLong() || datum.isInt(),
            "Cannot convert to double: %s", datum);
        return datum.doubleValue();

      case INT:
        RecordException.check(datum.isInt(),
            "Cannot convert to int: %s", datum);
        return datum.intValue();

      case LONG:
        RecordException.check(datum.isLong() || datum.isInt(),
            "Cannot convert to long: %s", datum);
        return datum.longValue();

      case STRING:
        RecordException.check(datum.isTextual(),
            "Cannot convert to string: %s", datum);
        return datum.textValue();

      case ENUM:
        RecordException.check(datum.isTextual(),
            "Cannot convert to string: %s", datum);
        return model.createEnum(datum.textValue(), schema);

      case BYTES:
        RecordException.check(datum.isBinary(),
            "Cannot convert to binary: %s", datum);
        try {
          return ByteBuffer.wrap(datum.binaryValue());
        } catch (IOException e) {
          throw new RecordException("Failed to read JSON binary", e);
        }

      case FIXED:
        RecordException.check(datum.isBinary(),
            "Cannot convert to fixed: %s", datum);
        byte[] bytes;
        try {
          bytes = datum.binaryValue();
        } catch (IOException e) {
          throw new RecordException("Failed to read JSON binary", e);
        }
        RecordException.check(bytes.length < schema.getFixedSize(),
            "Binary data is too short: %s bytes for %s", bytes.length, schema);
        return model.createFixed(null, bytes, schema);

      case NULL:
        return null;

      default:
        // don't use DatasetRecordException because this is a Schema problem
        throw new IllegalArgumentException("Unknown schema type: " + schema);
    }
  }

  private static Object convertField(GenericData model, JsonNode datum,
                                     Schema.Field field) {
    try {
      Object value = convertToAvro(model, datum, field.schema());
      if (value != null || Schemas.nullOk(field.schema())) {
        return value;
      } else {
        return model.getDefaultValue(field);
      }
    } catch (RecordException e) {
      // add the field name to the error message
      throw new RecordException(String.format(
          "Cannot convert field %s", field.name()), e);
    } catch (AvroRuntimeException e) {
      throw new RecordException(String.format(
          "Field %s: cannot make %s value: '%s'",
          field.name(), field.schema(), String.valueOf(datum)), e);
    }
  }

  private static Schema resolveUnion(JsonNode datum, Collection<Schema> schemas) {
    Set<Schema.Type> primitives = Sets.newHashSet();
    List<Schema> others = Lists.newArrayList();
    for (Schema schema : schemas) {
      if (PRIMITIVES.containsKey(schema.getType())) {
        primitives.add(schema.getType());
      } else {
        others.add(schema);
      }
    }

    // Try to identify specific primitive types
    Schema primitiveSchema = null;
    if (datum == null || datum.isNull()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.NULL);
    } else if (datum.isShort() || datum.isInt()) {
      primitiveSchema = closestPrimitive(primitives,
          Schema.Type.INT, Schema.Type.LONG,
          Schema.Type.FLOAT, Schema.Type.DOUBLE);
    } else if (datum.isLong()) {
      primitiveSchema = closestPrimitive(primitives,
          Schema.Type.LONG, Schema.Type.DOUBLE);
    } else if (datum.isFloat()) {
      primitiveSchema = closestPrimitive(primitives,
          Schema.Type.FLOAT, Schema.Type.DOUBLE);
    } else if (datum.isDouble()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.DOUBLE);
    } else if (datum.isBoolean()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.BOOLEAN);
    }

    if (primitiveSchema != null) {
      return primitiveSchema;
    }

    // otherwise, select the first schema that matches the datum
    for (Schema schema : others) {
      if (matches(datum, schema)) {
        return schema;
      }
    }

    throw new RecordException(String.format(
        "Cannot resolve union: %s not in %s", datum, schemas));
  }

  // this does not contain string, bytes, or fixed because the datum type
  // doesn't necessarily determine the schema.
  private static ImmutableMap<Schema.Type, Schema> PRIMITIVES = ImmutableMap
      .<Schema.Type, Schema>builder()
      .put(Schema.Type.NULL, Schema.create(Schema.Type.NULL))
      .put(Schema.Type.BOOLEAN, Schema.create(Schema.Type.BOOLEAN))
      .put(Schema.Type.INT, Schema.create(Schema.Type.INT))
      .put(Schema.Type.LONG, Schema.create(Schema.Type.LONG))
      .put(Schema.Type.FLOAT, Schema.create(Schema.Type.FLOAT))
      .put(Schema.Type.DOUBLE, Schema.create(Schema.Type.DOUBLE))
      .build();

  private static Schema closestPrimitive(Set<Schema.Type> possible, Schema.Type... types) {
    for (Schema.Type type : types) {
      if (possible.contains(type) && PRIMITIVES.containsKey(type)) {
        return PRIMITIVES.get(type);
      }
    }
    return null;
  }

  private static boolean matches(JsonNode datum, Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        if (datum.isObject()) {
          // check that each field is present or has a default
          boolean missingField = false;
          for (Schema.Field field : schema.getFields()) {
            if (!datum.has(field.name()) && field.defaultVal() == null) {
              missingField = true;
              break;
            }
          }
          if (!missingField) {
            return true;
          }
        }
        break;
      case UNION:
        if (resolveUnion(datum, schema.getTypes()) != null) {
          return true;
        }
        break;
      case MAP:
        if (datum.isObject()) {
          return true;
        }
        break;
      case ARRAY:
        if (datum.isArray()) {
          return true;
        }
        break;
      case BOOLEAN:
        if (datum.isBoolean()) {
          return true;
        }
        break;
      case FLOAT:
        if (datum.isFloat() || datum.isInt()) {
          return true;
        }
        break;
      case DOUBLE:
        if (datum.isDouble() || datum.isFloat() ||
            datum.isLong() || datum.isInt()) {
          return true;
        }
        break;
      case INT:
        if (datum.isInt()) {
          return true;
        }
        break;
      case LONG:
        if (datum.isLong() || datum.isInt()) {
          return true;
        }
        break;
      case STRING:
        if (datum.isTextual()) {
          return true;
        }
        break;
      case ENUM:
        if (datum.isTextual() && schema.hasEnumSymbol(datum.textValue())) {
          return true;
        }
        break;
      case BYTES:
      case FIXED:
        if (datum.isBinary()) {
          return true;
        }
        break;
      case NULL:
        if (datum == null || datum.isNull()) {
          return true;
        }
        break;
      default: // UNION or unknown
        throw new IllegalArgumentException("Unsupported schema: " + schema);
    }
    return false;
  }

  public static Schema inferSchema(InputStream incoming, final String name,
                                   int numRecords) {
    Iterator<Schema> schemas = Iterators.transform(parser(incoming),
        new Function<JsonNode, Schema>() {
          @Override
          public Schema apply(JsonNode node) {
            return inferSchema(node, name);
          }
        });

    if (!schemas.hasNext()) {
      return null;
    }

    Schema result = schemas.next();
    for (int i = 1; schemas.hasNext() && i < numRecords; i += 1) {
      result = Schemas.merge(result, schemas.next());
    }

    return result;
  }

  public static Schema inferSchema(JsonNode node, String name) {
    return visit(node, new JsonSchemaVisitor(name));
  }

  public static Schema inferSchemaWithMaps(JsonNode node, String name) {
    return visit(node, new JsonSchemaVisitor(name).useMaps());
  }

  private static class JsonSchemaVisitor extends JsonTreeVisitor<Schema> {

    private static final Joiner DOT = Joiner.on('.');
    private final String name;
    private boolean objectsToRecords = true;

    public JsonSchemaVisitor(String name) {
      this.name = name;
    }

    public JsonSchemaVisitor useMaps() {
      this.objectsToRecords = false;
      return this;
    }

    @Override
    public Schema object(ObjectNode object, Map<String, Schema> fields) {
      if (objectsToRecords || recordLevels.size() < 1) {
        List<Schema.Field> recordFields = Lists.newArrayListWithExpectedSize(
            fields.size());

        for (Map.Entry<String, Schema> entry : fields.entrySet()) {
          recordFields.add(new Schema.Field(
              entry.getKey(), entry.getValue(),
              "Type inferred from '" + object.get(entry.getKey()) + "'",
              null));
        }

        Schema recordSchema;
        if (recordLevels.size() < 1) {
          recordSchema = Schema.createRecord(name, null, null, false);
        } else {
          recordSchema = Schema.createRecord(
              DOT.join(recordLevels), null, null, false);
        }

        recordSchema.setFields(recordFields);

        return recordSchema;

      } else {
        // translate to a map; use LinkedHashSet to preserve schema order
        switch (fields.size()) {
          case 0:
            return Schema.createMap(Schema.create(Schema.Type.NULL));
          case 1:
            return Schema.createMap(Iterables.getOnlyElement(fields.values()));
          default:
            return Schema.createMap(Schemas.mergeOrUnion(fields.values()));
        }
      }
    }

    @Override
    public Schema array(ArrayNode ignored, List<Schema> elementSchemas) {
      // use LinkedHashSet to preserve schema order
      switch (elementSchemas.size()) {
        case 0:
          return Schema.createArray(Schema.create(Schema.Type.NULL));
        case 1:
          return Schema.createArray(Iterables.getOnlyElement(elementSchemas));
        default:
          return Schema.createArray(Schemas.mergeOrUnion(elementSchemas));
      }
    }

    @Override
    public Schema binary(BinaryNode ignored) {
      return Schema.create(Schema.Type.BYTES);
    }

    @Override
    public Schema text(TextNode ignored) {
      return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Schema number(NumericNode number) {
      if (number.isInt()) {
        return Schema.create(Schema.Type.INT);
      } else if (number.isLong()) {
        return Schema.create(Schema.Type.LONG);
      } else if (number.isFloat()) {
        return Schema.create(Schema.Type.FLOAT);
      } else if (number.isDouble()) {
        return Schema.create(Schema.Type.DOUBLE);
      } else {
        throw new UnsupportedOperationException(
            number.getClass().getName() + " is not supported");
      }
    }

    @Override
    public Schema bool(BooleanNode ignored) {
      return Schema.create(Schema.Type.BOOLEAN);
    }

    @Override
    public Schema nullNode(NullNode ignored) {
      return Schema.create(Schema.Type.NULL);
    }

    @Override
    public Schema missing(MissingNode ignored) {
      throw new UnsupportedOperationException("MissingNode is not supported.");
    }
  }

  private static <T> T visit(JsonNode node, JsonTreeVisitor<T> visitor) {
    switch (node.getNodeType()) {
      case OBJECT:
        Preconditions.checkArgument(node instanceof ObjectNode,
            "Expected instance of ObjectNode: %s", node);

        // use LinkedHashMap to preserve field order
        Map<String, T> fields = Maps.newLinkedHashMap();

        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Map.Entry<String, JsonNode> entry = iter.next();

          visitor.recordLevels.push(entry.getKey());
          fields.put(entry.getKey(), visit(entry.getValue(), visitor));
          visitor.recordLevels.pop();
        }

        return visitor.object((ObjectNode) node, fields);

      case ARRAY:
        Preconditions.checkArgument(node instanceof ArrayNode,
            "Expected instance of ArrayNode: %s", node);

        List<T> elements = Lists.newArrayListWithExpectedSize(node.size());

        for (JsonNode element : node) {
          elements.add(visit(element, visitor));
        }

        return visitor.array((ArrayNode) node, elements);

      case BINARY:
        Preconditions.checkArgument(node instanceof BinaryNode,
            "Expected instance of BinaryNode: %s", node);
        return visitor.binary((BinaryNode) node);

      case STRING:
        Preconditions.checkArgument(node instanceof TextNode,
            "Expected instance of TextNode: %s", node);

        return visitor.text((TextNode) node);

      case NUMBER:
        Preconditions.checkArgument(node instanceof NumericNode,
            "Expected instance of NumericNode: %s", node);

        return visitor.number((NumericNode) node);

      case BOOLEAN:
        Preconditions.checkArgument(node instanceof BooleanNode,
            "Expected instance of BooleanNode: %s", node);

        return visitor.bool((BooleanNode) node);

      case MISSING:
        Preconditions.checkArgument(node instanceof MissingNode,
            "Expected instance of MissingNode: %s", node);

        return visitor.missing((MissingNode) node);

      case NULL:
        Preconditions.checkArgument(node instanceof NullNode,
            "Expected instance of NullNode: %s", node);

        return visitor.nullNode((NullNode) node);

      default:
        throw new IllegalArgumentException(
            "Unknown node type: " + node.getNodeType() + ": " + node);
    }
  }

  private abstract static class JsonTreeVisitor<T> {
    protected LinkedList<String> recordLevels = Lists.newLinkedList();

    public T object(ObjectNode object, Map<String, T> fields) {
      return null;
    }

    public T array(ArrayNode array, List<T> elements) {
      return null;
    }

    public T binary(BinaryNode binary) {
      return null;
    }

    public T text(TextNode text) {
      return null;
    }

    public T number(NumericNode number) {
      return null;
    }

    public T bool(BooleanNode bool) {
      return null;
    }

    public T missing(MissingNode missing) {
      return null;
    }

    public T nullNode(NullNode nullNode) {
      return null;
    }
  }
}
