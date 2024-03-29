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
package org.apache.parquet.thrift;

import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.projection.FieldsPath;
import org.apache.parquet.thrift.projection.ThriftProjectionException;

/**
 * A {@link FieldProjectionFilter} that keeps only the first primitive field
 * that it encounters.
 */
class KeepOnlyFirstPrimitiveFilter implements FieldProjectionFilter {
  private boolean found = false;

  @Override
  public boolean keep(FieldsPath path) {
    if (found) {
      return false;
    }

    found = true;
    return true;
  }

  @Override
  public void assertNoUnmatchedPatterns() throws ThriftProjectionException {}
}
