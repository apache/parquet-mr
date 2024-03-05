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
package org.apache.parquet.example.data.simple;

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

public class SimpleGroup extends Group {

  private final GroupType schema;
  private final List<Object>[] data;

  @SuppressWarnings("unchecked")
  public SimpleGroup(GroupType schema) {
    this.schema = schema;
    this.data = new List[schema.getFields().size()];
    for (int i = 0; i < schema.getFieldCount(); i++) {
      this.data[i] = new ArrayList<>();
    }
  }

  @Override
  public String toString() {
    return toString("");
  }

  private StringBuilder appendToString(StringBuilder builder, String indent) {
    int i = 0;
    for (Type field : schema.getFields()) {
      String name = field.getName();
      List<Object> values = data[i];
      ++i;
      if (values != null && !values.isEmpty()) {
        for (Object value : values) {
          builder.append(indent).append(name);
          if (value == null) {
            builder.append(": NULL\n");
          } else if (value instanceof Group) {
            builder.append('\n');
            ((SimpleGroup) value).appendToString(builder, indent + "  ");
          } else {
            builder.append(": ").append(value).append('\n');
          }
        }
      }
    }
    return builder;
  }

  public String toString(String indent) {
    StringBuilder builder = new StringBuilder();
    appendToString(builder, indent);
    return builder.toString();
  }

  @Override
  public Group addGroup(int fieldIndex) {
    SimpleGroup g = new SimpleGroup(schema.getType(fieldIndex).asGroupType());
    add(fieldIndex, g);
    return g;
  }

  public Object getObject(String field, int index) {
    return getObject(getType().getFieldIndex(field), index);
  }

  public Object getObject(int fieldIndex, int index) {
    Object wrapped = getValue(fieldIndex, index);
    // Unwrap to Java standard object, if possible
    if (wrapped instanceof BooleanValue) {
      return ((BooleanValue) wrapped).getBoolean();
    } else if (wrapped instanceof IntegerValue) {
      return ((IntegerValue) wrapped).getInteger();
    } else if (wrapped instanceof LongValue) {
      return ((LongValue) wrapped).getLong();
    } else if (wrapped instanceof Int96Value) {
      return ((Int96Value) wrapped).getInt96();
    } else if (wrapped instanceof FloatValue) {
      return ((FloatValue) wrapped).getFloat();
    } else if (wrapped instanceof DoubleValue) {
      return ((DoubleValue) wrapped).getDouble();
    } else if (wrapped instanceof BinaryValue) {
      return ((BinaryValue) wrapped).getBinary();
    } else {
      return wrapped;
    }
  }

  @Override
  public Group getGroup(int fieldIndex, int index) {
    return (Group) getValue(fieldIndex, index);
  }

  private Object getValue(int fieldIndex, int index) {
    List<Object> list;
    try {
      list = data[fieldIndex];
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException(
          "not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") in group:\n" + this);
    }
    try {
      return list.get(index);
    } catch (IndexOutOfBoundsException e) {
      throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex)
          + ") element number " + index + " in group:\n" + this);
    }
  }

  private void add(int fieldIndex, Primitive value) {
    Type type = schema.getType(fieldIndex);
    List<Object> list = data[fieldIndex];
    if (!type.isRepetition(Type.Repetition.REPEATED) && !list.isEmpty()) {
      throw new IllegalStateException(
          "field " + fieldIndex + " (" + type.getName() + ") can not have more than one value: " + list);
    }
    list.add(value);
  }

  @Override
  public int getFieldRepetitionCount(int fieldIndex) {
    List<Object> list = data[fieldIndex];
    return list == null ? 0 : list.size();
  }

  @Override
  public String getValueToString(int fieldIndex, int index) {
    return String.valueOf(getValue(fieldIndex, index));
  }

  @Override
  public String getString(int fieldIndex, int index) {
    return ((BinaryValue) getValue(fieldIndex, index)).getString();
  }

  @Override
  public int getInteger(int fieldIndex, int index) {
    return ((IntegerValue) getValue(fieldIndex, index)).getInteger();
  }

  @Override
  public long getLong(int fieldIndex, int index) {
    return ((LongValue) getValue(fieldIndex, index)).getLong();
  }

  @Override
  public double getDouble(int fieldIndex, int index) {
    return ((DoubleValue) getValue(fieldIndex, index)).getDouble();
  }

  @Override
  public float getFloat(int fieldIndex, int index) {
    return ((FloatValue) getValue(fieldIndex, index)).getFloat();
  }

  @Override
  public boolean getBoolean(int fieldIndex, int index) {
    return ((BooleanValue) getValue(fieldIndex, index)).getBoolean();
  }

  @Override
  public Binary getBinary(int fieldIndex, int index) {
    return ((BinaryValue) getValue(fieldIndex, index)).getBinary();
  }

  public NanoTime getTimeNanos(int fieldIndex, int index) {
    return NanoTime.fromInt96((Int96Value) getValue(fieldIndex, index));
  }

  @Override
  public Binary getInt96(int fieldIndex, int index) {
    return ((Int96Value) getValue(fieldIndex, index)).getInt96();
  }

  @Override
  public void add(int fieldIndex, int value) {
    add(fieldIndex, new IntegerValue(value));
  }

  @Override
  public void add(int fieldIndex, long value) {
    add(fieldIndex, new LongValue(value));
  }

  @Override
  public void add(int fieldIndex, String value) {
    add(fieldIndex, new BinaryValue(Binary.fromString(value)));
  }

  @Override
  public void add(int fieldIndex, NanoTime value) {
    add(fieldIndex, value.toInt96());
  }

  @Override
  public void add(int fieldIndex, boolean value) {
    add(fieldIndex, new BooleanValue(value));
  }

  @Override
  public void add(int fieldIndex, Binary value) {
    switch (getType().getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        add(fieldIndex, new BinaryValue(value));
        break;
      case INT96:
        add(fieldIndex, new Int96Value(value));
        break;
      default:
        throw new UnsupportedOperationException(
            getType().asPrimitiveType().getName() + " not supported for Binary");
    }
  }

  @Override
  public void add(int fieldIndex, float value) {
    add(fieldIndex, new FloatValue(value));
  }

  @Override
  public void add(int fieldIndex, double value) {
    add(fieldIndex, new DoubleValue(value));
  }

  @Override
  public void add(int fieldIndex, Group value) {
    data[fieldIndex].add(value);
  }

  @Override
  public GroupType getType() {
    return schema;
  }

  @Override
  public void writeValue(int field, int index, RecordConsumer recordConsumer) {
    ((Primitive) getValue(field, index)).writeValue(recordConsumer);
  }
}
