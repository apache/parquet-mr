/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet;

/**
 * Utility class for throwing Exceptions.
 *
 * @deprecated
 */
@Deprecated
public class Exceptions {
  /**
   * If the given throwable is an instance of E, throw it as an E.
   *
   * @param t an exception instance
   * @param excClass an exception class t may be an instance of
   * @param <E> the type of exception that will be thrown if throwable is an instance
   * @throws E if t is an instance of E
   */
  public static <E extends Exception> void throwIfInstance(Throwable t,
                                                           Class<E> excClass)
      throws E {
    if (excClass.isAssignableFrom(t.getClass())) {
      // the throwable is already an exception, so return it
      throw excClass.cast(t);
    }
  }
}
