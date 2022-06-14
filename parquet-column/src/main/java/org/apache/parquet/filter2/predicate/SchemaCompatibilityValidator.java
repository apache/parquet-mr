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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.ColumnFilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.In;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.NotIn;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.SetColumnFilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Inspects the column types found in the provided {@link FilterPredicate} and compares them
 * to the actual schema found in the parquet file. If the provided predicate's types are
 * not consistent with the file schema, and IllegalArgumentException is thrown.
 *
 * Ideally, all this would be checked at compile time, and this class wouldn't be needed.
 * If we can come up with a way to do that, we should.
 *
 * This class is stateful, cannot be reused, and is not thread safe.
 *
 * TODO: detect if a column is optional or required and validate that eq(null)
 * TODO: is not called on required fields (is that too strict?)
 * TODO: (https://issues.apache.org/jira/browse/PARQUET-44)
 */
public class SchemaCompatibilityValidator implements FilterPredicate.Visitor<Void> {

  public static void validate(FilterPredicate predicate, MessageType schema) {
    Objects.requireNonNull(predicate, "predicate cannot be null");
    Objects.requireNonNull(schema, "schema cannot be null");
    predicate.accept(new SchemaCompatibilityValidator(schema));
  }

  // A map of column name to the type the user supplied for this column.
  // Used to validate that the user did not provide different types for the same
  // column.
  private final Map<ColumnPath, Class<?>> columnTypesEncountered = new HashMap<>();

  // the columns (keyed by path) according to the file's schema. This is the source of truth, and
  // we are validating that what the user provided agrees with these.
  private final Map<ColumnPath, ColumnDescriptor> columnsAccordingToSchema = new HashMap<>();

  private SchemaCompatibilityValidator(MessageType schema) {

    for (ColumnDescriptor cd : schema.getColumns()) {
      ColumnPath columnPath = ColumnPath.get(cd.getPath());
      columnsAccordingToSchema.put(columnPath, cd);
    }
  }

  @Override
  public <T extends Comparable<T>> Void visit(Eq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(NotEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(Lt<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(LtEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(Gt<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(GtEq<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(In<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public <T extends Comparable<T>> Void visit(NotIn<T> pred) {
    validateColumnFilterPredicate(pred);
    return null;
  }

  @Override
  public Void visit(And and) {
    and.getLeft().accept(this);
    and.getRight().accept(this);
    return null;
  }

  @Override
  public Void visit(Or or) {
    or.getLeft().accept(this);
    or.getRight().accept(this);
    return null;
  }

  @Override
  public Void visit(Not not) {
    not.getPredicate().accept(this);
    return null;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(UserDefined<T, U> udp) {
    validateColumn(udp.getColumn());
    return null;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(LogicalNotUserDefined<T, U> udp) {
    return udp.getUserDefined().accept(this);
  }

  private <T extends Comparable<T>> void validateColumnFilterPredicate(ColumnFilterPredicate<T> pred) {
    validateColumn(pred.getColumn());
  }

  private <T extends Comparable<T>> void validateColumnFilterPredicate(SetColumnFilterPredicate<T> pred) {
    validateColumn(pred.getColumn());
  }

  private <T extends Comparable<T>> void validateColumn(Column<T> column) {
    ColumnPath path = column.getColumnPath();
    if (path == null) {
      HashSet<Type.ID> ids = new HashSet<>();
      Type.ID id = column.getColumnId();
      List<ColumnDescriptor> columnDescriptors = new ArrayList<>(columnsAccordingToSchema.values());
      for (ColumnDescriptor columnDescriptor : columnDescriptors) {
        Type.ID columnId = columnDescriptor.getId();
        if (columnId != null) {
          if (ids.contains(columnId)) {
            throw new RuntimeException("duplicate id");
          }
          ids.add(columnId);
          if (columnId.intValue() == id.intValue()) {
            column.setColumnPath(ColumnPath.get(columnDescriptor.getPath()));
            path = column.getColumnPath();
          }
        }
      }
    }

    Class<?> alreadySeen = columnTypesEncountered.get(path);
    if (alreadySeen != null && !alreadySeen.equals(column.getColumnType())) {
      throw new IllegalArgumentException("Column: "
          + path.toDotString()
          + " was provided with different types in the same predicate."
          + " Found both: (" + alreadySeen + ", " + column.getColumnType() + ")");
    }

    if (alreadySeen == null) {
      columnTypesEncountered.put(path, column.getColumnType());
    }

    ColumnDescriptor descriptor = getColumnDescriptor(path);
    if (descriptor == null) {
      // the column is missing from the schema. evaluation uses calls
      // updateNull() a value is missing, so this will be handled correctly.
      return;
    }

    if (descriptor.getMaxRepetitionLevel() > 0) {
      throw new IllegalArgumentException("FilterPredicates do not currently support repeated columns. "
          + "Column " + path.toDotString() + " is repeated.");
    }

    ValidTypeMap.assertTypeValid(column, descriptor.getType());
  }

  private ColumnDescriptor getColumnDescriptor(ColumnPath columnPath) {
    return columnsAccordingToSchema.get(columnPath);
  }
}
