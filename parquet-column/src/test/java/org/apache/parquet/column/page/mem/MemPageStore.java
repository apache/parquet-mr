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
package org.apache.parquet.column.page.mem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.UnknownColumnException;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemPageStore implements PageReadStore, PageWriteStore {
  private static final Logger LOG = LoggerFactory.getLogger(MemPageStore.class);

  private Map<ColumnDescriptor, MemPageWriter> pageWriters = new HashMap<>();

  private long rowCount;

  private boolean isReadEager;

  public MemPageStore(long rowCount) {
    this(rowCount, true);
  }

  public MemPageStore(long rowCount, boolean isReadEager) {
    super();
    this.rowCount = rowCount;
    this.isReadEager = isReadEager;
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    MemPageWriter pageWriter = pageWriters.get(path);
    if (pageWriter == null) {
      pageWriter = new MemPageWriter();
      pageWriters.put(path, pageWriter);
    }
    return pageWriter;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    MemPageWriter pageWriter = pageWriters.get(descriptor);
    if (pageWriter == null) {
      throw new UnknownColumnException(descriptor);
    }
    List<DataPage> pages = new ArrayList<>(pageWriter.getPages());
    LOG.debug("initialize page reader with {} values and {} pages", pageWriter.getTotalValueCount(), pages.size());
    return new MemPageReader(
        pageWriter.getTotalValueCount(), pages.iterator(), pageWriter.getDictionaryPage(), isReadEager);
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public void close() {
    // no-op
  }

  public void addRowCount(long count) {
    rowCount += count;
  }
}
