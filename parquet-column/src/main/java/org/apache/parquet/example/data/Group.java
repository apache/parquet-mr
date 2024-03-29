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
package org.apache.parquet.example.data;

import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Group extends GroupValueSource {
  private static final Logger LOG = LoggerFactory.getLogger(Group.class);

  public void add(String field, int value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, long value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, float value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, double value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, String value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, NanoTime value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, boolean value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, Binary value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, Group value) {
    add(getType().getFieldIndex(field), value);
  }

  public Group addGroup(String field) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("add group {} to {}", field, getType().getName());
    }
    return addGroup(getType().getFieldIndex(field));
  }

  @Override
  public Group getGroup(String field, int index) {
    return getGroup(getType().getFieldIndex(field), index);
  }

  public abstract void add(int fieldIndex, int value);

  public abstract void add(int fieldIndex, long value);

  public abstract void add(int fieldIndex, String value);

  public abstract void add(int fieldIndex, boolean value);

  public abstract void add(int fieldIndex, NanoTime value);

  public abstract void add(int fieldIndex, Binary value);

  public abstract void add(int fieldIndex, float value);

  public abstract void add(int fieldIndex, double value);

  public abstract void add(int fieldIndex, Group value);

  public abstract Group addGroup(int fieldIndex);

  @Override
  public abstract Group getGroup(int fieldIndex, int index);

  public Group asGroup() {
    return this;
  }

  public Group append(String fieldName, int value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, float value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, double value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, long value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, NanoTime value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, String value) {
    add(fieldName, Binary.fromString(value));
    return this;
  }

  public Group append(String fieldName, boolean value) {
    add(fieldName, value);
    return this;
  }

  public Group append(String fieldName, Binary value) {
    add(fieldName, value);
    return this;
  }

  public abstract void writeValue(int field, int index, RecordConsumer recordConsumer);
}
