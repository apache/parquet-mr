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

import java.util.Iterator;
import java.util.Objects;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemPageReader implements PageReader {
  private static final Logger LOG = LoggerFactory.getLogger(MemPageReader.class);

  private final long totalValueCount;
  private final Iterator<DataPage> pages;
  private final DictionaryPage dictionaryPage;

  private final boolean isEager;

  public MemPageReader(
      long totalValueCount, Iterator<DataPage> pages, DictionaryPage dictionaryPage, boolean isEager) {
    super();
    this.pages = Objects.requireNonNull(pages, "pages cannot be null");
    this.totalValueCount = totalValueCount;
    this.dictionaryPage = dictionaryPage;
    this.isEager = isEager;
  }

  @Override
  public long getTotalValueCount() {
    if (!isEager && pages.hasNext()) {
      throw new IllegalStateException("Can't return totalValueCount until lazy iterator has been exhausted");
    }
    return totalValueCount;
  }

  @Override
  public boolean isEager() {
    return isEager || !pages.hasNext();
  }

  @Override
  public DataPage readPage() {
    if (pages.hasNext()) {
      DataPage next = pages.next();
      LOG.debug("read page {}", next);
      return next;
    } else {
      throw new ParquetDecodingException("after last page");
    }
  }

  @Override
  public DictionaryPage readDictionaryPage() {
    return dictionaryPage;
  }
}
