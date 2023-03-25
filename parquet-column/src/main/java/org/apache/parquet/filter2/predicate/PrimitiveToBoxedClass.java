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
package org.apache.parquet.filter2.predicate;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.Preconditions.checkArgument;

/**
 * Converts a {@code Class<primitive>} to it's corresponding {@code Class<Boxed>}, eg
 * {@code Class<int>} to {@code Class<Integer>}
 */
public class PrimitiveToBoxedClass {
  private static final Map<Class<?>, Class<?>> primitiveToBoxed = new HashMap<>();

  static {
    primitiveToBoxed.put(boolean.class, Boolean.class);
    primitiveToBoxed.put(byte.class, Byte.class);
    primitiveToBoxed.put(short.class, Short.class);
    primitiveToBoxed.put(char.class, Character.class);
    primitiveToBoxed.put(int.class, Integer.class);
    primitiveToBoxed.put(long.class, Long.class);
    primitiveToBoxed.put(float.class, Float.class);
    primitiveToBoxed.put(double.class, Double.class);
  }

  public static Class<?> get(Class<?> c) {
    checkArgument(c.isPrimitive(), "Class %s is not primitive!", c);
    return primitiveToBoxed.get(c);
  }

  private PrimitiveToBoxedClass() { }
}
