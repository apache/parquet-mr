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
package org.apache.parquet.column.impl;

import static java.lang.String.format;
import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

import java.io.IOException;
import java.util.Objects;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base superclass for {@link ColumnReader} implementations.
 */
abstract class ColumnReaderBase implements ColumnReader {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnReaderBase.class);

  /**
   * binds the lower level page decoder to the record converter materializing the records
   */
  private abstract static class Binding {

    /**
     * read one value from the underlying page
     */
    abstract void read();

    /**
     * skip one value from the underlying page
     */
    abstract void skip();

    /**
     * Skips n values from the underlying page
     *
     * @param n the number of values to be skipped
     */
    abstract void skip(int n);

    /**
     * write current value to converter
     */
    abstract void writeValue();

    /**
     * @return current value
     */
    public int getDictionaryId() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public int getInteger() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public boolean getBoolean() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public long getLong() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public Binary getBinary() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public float getFloat() {
      throw new UnsupportedOperationException();
    }

    /**
     * @return current value
     */
    public double getDouble() {
      throw new UnsupportedOperationException();
    }
  }

  private final ParsedVersion writerVersion;
  private final ColumnDescriptor path;
  private final PageReader pageReader;
  private final Dictionary dictionary;

  private IntIterator repetitionLevelColumn;
  private IntIterator definitionLevelColumn;
  protected ValuesReader dataColumn;
  private Encoding currentEncoding;

  private int repetitionLevel;
  private int definitionLevel;
  private int dictionaryId;

  private long endOfPageValueCount;
  private long readValues = 0;
  private int pageValueCount = 0;

  private final PrimitiveConverter converter;
  private Binding binding;
  private final int maxDefinitionLevel;

  // this is needed because we will attempt to read the value twice when filtering
  // TODO: rework that
  private boolean valueRead;

  private void bindToDictionary(final Dictionary dictionary) {
    binding = new Binding() {
      @Override
      void read() {
        dictionaryId = dataColumn.readValueDictionaryId();
      }

      @Override
      public void skip() {
        dataColumn.skip();
      }

      @Override
      void skip(int n) {
        dataColumn.skip(n);
      }

      @Override
      public int getDictionaryId() {
        return dictionaryId;
      }

      @Override
      void writeValue() {
        converter.addValueFromDictionary(dictionaryId);
      }

      @Override
      public int getInteger() {
        return dictionary.decodeToInt(dictionaryId);
      }

      @Override
      public boolean getBoolean() {
        return dictionary.decodeToBoolean(dictionaryId);
      }

      @Override
      public long getLong() {
        return dictionary.decodeToLong(dictionaryId);
      }

      @Override
      public Binary getBinary() {
        return dictionary.decodeToBinary(dictionaryId);
      }

      @Override
      public float getFloat() {
        return dictionary.decodeToFloat(dictionaryId);
      }

      @Override
      public double getDouble() {
        return dictionary.decodeToDouble(dictionaryId);
      }
    };
  }

  private void bind(PrimitiveTypeName type) {
    binding = type.convert(new PrimitiveTypeNameConverter<Binding, RuntimeException>() {
      @Override
      public Binding convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          float current;

          @Override
          void read() {
            current = dataColumn.readFloat();
          }

          @Override
          public void skip() {
            current = 0;
            dataColumn.skip();
          }

          @Override
          void skip(int n) {
            current = 0;
            dataColumn.skip(n);
          }

          @Override
          public float getFloat() {
            return current;
          }

          @Override
          void writeValue() {
            converter.addFloat(current);
          }
        };
      }

      @Override
      public Binding convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          double current;

          @Override
          void read() {
            current = dataColumn.readDouble();
          }

          @Override
          public void skip() {
            current = 0;
            dataColumn.skip();
          }

          @Override
          void skip(int n) {
            current = 0;
            dataColumn.skip(n);
          }

          @Override
          public double getDouble() {
            return current;
          }

          @Override
          void writeValue() {
            converter.addDouble(current);
          }
        };
      }

      @Override
      public Binding convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          int current;

          @Override
          void read() {
            current = dataColumn.readInteger();
          }

          @Override
          public void skip() {
            current = 0;
            dataColumn.skip();
          }

          @Override
          void skip(int n) {
            current = 0;
            dataColumn.skip(n);
          }

          @Override
          public int getInteger() {
            return current;
          }

          @Override
          void writeValue() {
            converter.addInt(current);
          }
        };
      }

      @Override
      public Binding convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          long current;

          @Override
          void read() {
            current = dataColumn.readLong();
          }

          @Override
          public void skip() {
            current = 0;
            dataColumn.skip();
          }

          @Override
          void skip(int n) {
            current = 0;
            dataColumn.skip(n);
          }

          @Override
          public long getLong() {
            return current;
          }

          @Override
          void writeValue() {
            converter.addLong(current);
          }
        };
      }

      @Override
      public Binding convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return this.convertBINARY(primitiveTypeName);
      }

      @Override
      public Binding convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return this.convertBINARY(primitiveTypeName);
      }

      @Override
      public Binding convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          boolean current;

          @Override
          void read() {
            current = dataColumn.readBoolean();
          }

          @Override
          public void skip() {
            current = false;
            dataColumn.skip();
          }

          @Override
          void skip(int n) {
            current = false;
            dataColumn.skip(n);
          }

          @Override
          public boolean getBoolean() {
            return current;
          }

          @Override
          void writeValue() {
            converter.addBoolean(current);
          }
        };
      }

      @Override
      public Binding convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
        return new Binding() {
          Binary current;

          @Override
          void read() {
            current = dataColumn.readBytes();
          }

          @Override
          public void skip() {
            current = null;
            dataColumn.skip();
          }

          @Override
          void skip(int n) {
            current = null;
            dataColumn.skip(n);
          }

          @Override
          public Binary getBinary() {
            return current;
          }

          @Override
          void writeValue() {
            converter.addBinary(current);
          }
        };
      }
    });
  }

  /**
   * creates a reader for triplets
   *
   * @param path          the descriptor for the corresponding column
   * @param pageReader    the underlying store to read from
   * @param converter     a converter that materializes the values in this column in the current record
   * @param writerVersion writer version string from the Parquet file being read
   */
  ColumnReaderBase(
      ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter, ParsedVersion writerVersion) {
    this.path = Objects.requireNonNull(path, "path cannot be null");
    this.pageReader = Objects.requireNonNull(pageReader, "pageReader cannot be null");
    this.converter = Objects.requireNonNull(converter, "converter cannot be null");
    this.writerVersion = writerVersion;
    this.maxDefinitionLevel = path.getMaxDefinitionLevel();
    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
        if (converter.hasDictionarySupport()) {
          converter.setDictionary(dictionary);
        }
      } catch (IOException e) {
        throw new ParquetDecodingException("could not decode the dictionary for " + path, e);
      }
    } else {
      this.dictionary = null;
    }
    if (pageReader.isEager() && pageReader.getTotalValueCount() <= 0) {
      throw new ParquetDecodingException("totalValueCount '" + pageReader.getTotalValueCount() + "' <= 0");
    }
  }

  boolean isFullyConsumed() {
    return pageReader.isEager() && readValues >= pageReader.getTotalValueCount();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#writeCurrentValueToConverter()
   */
  @Override
  public void writeCurrentValueToConverter() {
    readValue();
    this.binding.writeValue();
  }

  @Override
  public int getCurrentValueDictionaryID() {
    readValue();
    return binding.getDictionaryId();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getInteger()
   */
  @Override
  public int getInteger() {
    readValue();
    return this.binding.getInteger();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getBoolean()
   */
  @Override
  public boolean getBoolean() {
    readValue();
    return this.binding.getBoolean();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getLong()
   */
  @Override
  public long getLong() {
    readValue();
    return this.binding.getLong();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getBinary()
   */
  @Override
  public Binary getBinary() {
    readValue();
    return this.binding.getBinary();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getFloat()
   */
  @Override
  public float getFloat() {
    readValue();
    return this.binding.getFloat();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getDouble()
   */
  @Override
  public double getDouble() {
    readValue();
    return this.binding.getDouble();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getCurrentRepetitionLevel()
   */
  @Override
  public int getCurrentRepetitionLevel() {
    return repetitionLevel;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getDescriptor()
   */
  @Override
  public ColumnDescriptor getDescriptor() {
    return path;
  }

  /**
   * Reads the value into the binding.
   */
  public void readValue() {
    try {
      if (!valueRead) {
        binding.read();
        valueRead = true;
      }
    } catch (RuntimeException e) {
      if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, currentEncoding)
          && e instanceof ArrayIndexOutOfBoundsException) {
        // this is probably PARQUET-246, which may happen if reading data with
        // MR because this can't be detected without reading all footers
        throw new ParquetDecodingException(
            "Read failure possibly due to " + "PARQUET-246: try setting parquet.split.files to false",
            new ParquetDecodingException(
                format(
                    "Can't read value in column %s at value %d out of %d, "
                        + "%d out of %d in currentPage. repetition level: "
                        + "%d, definition level: %d",
                    path,
                    readValues,
                    pageReader.isEager()
                        ? pageReader.getTotalValueCount()
                        : -1, // @Todo print something else?
                    readValues - (endOfPageValueCount - pageValueCount),
                    pageValueCount,
                    repetitionLevel,
                    definitionLevel),
                e));
      }
      throw new ParquetDecodingException(
          format(
              "Can't read value in column %s at value %d out of %d, "
                  + "%d out of %d in currentPage. repetition level: "
                  + "%d, definition level: %d",
              path,
              readValues,
              pageReader.isEager() ? pageReader.getTotalValueCount() : -1, // @Todo print something else?
              readValues - (endOfPageValueCount - pageValueCount),
              pageValueCount,
              repetitionLevel,
              definitionLevel),
          e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#skip()
   */
  @Override
  public void skip() {
    if (!valueRead) {
      binding.skip();
      valueRead = true;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getCurrentDefinitionLevel()
   */
  @Override
  public int getCurrentDefinitionLevel() {
    return definitionLevel;
  }

  private int skipValues = 0;

  /** Reads all pages. */
  private void consumeAllPages() {
    int rl, dl;
    int skipValues = 0;
    for (; ; ) {
      if (isPageFullyConsumed()) {
        if (isFullyConsumed()) {
          LOG.debug("end reached");
          repetitionLevel = 0; // the next repetition level
          return;
        }
        readPage();
        skipValues = 0;
      }
      rl = repetitionLevelColumn.nextInt();
      dl = definitionLevelColumn.nextInt();
      ++readValues;
      if (!skipRL(rl)) {
        break;
      }
      if (dl == maxDefinitionLevel) {
        ++skipValues;
      }
    }
    binding.skip(skipValues);
    repetitionLevel = rl;
    definitionLevel = dl;
  }

  /**
   * Reads the next page.
   */
  private void consumePage() {
    int rl, dl;
    if (isPageFullyConsumed()) {
      skipValues = 0;
      if (isFullyConsumed()) {
        LOG.debug("end reached");
        repetitionLevel = 0; // the next repetition level
        return;
      }
      readPage();
    }
    rl = repetitionLevelColumn.nextInt();
    dl = definitionLevelColumn.nextInt();
    ++readValues;

    if (skipRL(rl)) {
      if (dl == maxDefinitionLevel) {
        ++skipValues;
      }
    } else {
      repetitionLevel = rl;
      definitionLevel = dl;
      binding.skip(skipValues);
    }
  }

  /*
   * Returns if current levels / value shall be skipped based on the specified repetition level.
   */
  abstract boolean skipRL(int rl);

  private void readPage() {
    LOG.debug("loading page");
    DataPage page = pageReader.readPage();
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        readPageV1(dataPageV1);
        return null;
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        readPageV2(dataPageV2);
        return null;
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount) {
    ValuesReader previousReader = this.dataColumn;

    this.currentEncoding = dataEncoding;
    this.pageValueCount = valueCount;
    this.endOfPageValueCount = readValues + pageValueCount;

    if (dataEncoding.usesDictionary()) {
      if (dictionary == null) {
        throw new ParquetDecodingException("could not read page in col " + path
            + " as the dictionary was missing for encoding " + dataEncoding);
      }
      this.dataColumn = dataEncoding.getDictionaryBasedValuesReader(path, VALUES, dictionary);
    } else {
      this.dataColumn = dataEncoding.getValuesReader(path, VALUES);
    }

    if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
      bindToDictionary(dictionary);
    } else {
      bind(path.getType());
    }

    try {
      dataColumn.initFromPage(pageValueCount, in);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page in col " + path, e);
    }

    if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding)
        && previousReader != null
        && dataColumn instanceof RequiresPreviousReader) {
      // previous reader can only be set if reading sequentially
      ((RequiresPreviousReader) dataColumn).setPreviousReader(previousReader);
    }
  }

  private void readPageV1(DataPageV1 page) {
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(path, REPETITION_LEVEL);
    ValuesReader dlReader = page.getDlEncoding().getValuesReader(path, DEFINITION_LEVEL);
    this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    int valueCount = page.getValueCount();
    try {
      BytesInput bytes = page.getBytes();
      LOG.debug("page size {} bytes and {} values", bytes.size(), valueCount);
      LOG.debug("reading repetition levels at 0");
      ByteBufferInputStream in = bytes.toInputStream();
      rlReader.initFromPage(valueCount, in);
      LOG.debug("reading definition levels at {}", in.position());
      dlReader.initFromPage(valueCount, in);
      LOG.debug("reading data at {}", in.position());
      initDataReader(page.getValueEncoding(), in, valueCount);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
    }
    newPageInitialized(page);
  }

  private void readPageV2(DataPageV2 page) {
    this.repetitionLevelColumn = newRLEIterator(path.getMaxRepetitionLevel(), page.getRepetitionLevels());
    this.definitionLevelColumn = newRLEIterator(path.getMaxDefinitionLevel(), page.getDefinitionLevels());
    int valueCount = page.getValueCount();
    LOG.debug("page data size {} bytes and {} values", page.getData().size(), valueCount);
    try {
      initDataReader(page.getDataEncoding(), page.getData().toInputStream(), valueCount);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
    }
    newPageInitialized(page);
  }

  final int getPageValueCount() {
    return pageValueCount;
  }

  abstract void newPageInitialized(DataPage page);

  private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
    try {
      if (maxLevel == 0) {
        return new NullIntIterator();
      }
      return new RLEIntIterator(new RunLengthBitPackingHybridDecoder(
          BytesUtils.getWidthFromMaxInt(maxLevel), bytes.toInputStream()));
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read levels in page for col " + path, e);
    }
  }

  boolean isPageFullyConsumed() {
    return readValues >= endOfPageValueCount;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#consume()
   */
  @Override
  public void consume() {
    if (pageReader.isEager()) {
      consumeAllPages();
    } else {
      consumePage();
    }
    valueRead = false;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.column.ColumnReader#getTotalValueCount()
   */
  @Deprecated
  @Override
  public long getTotalValueCount() {
    return pageReader.isEager() ? pageReader.getTotalValueCount() : 0; // @Todo should be -1?
  }

  abstract static class IntIterator {
    abstract int nextInt();
  }

  static class ValuesReaderIntIterator extends IntIterator {
    ValuesReader delegate;

    public ValuesReaderIntIterator(ValuesReader delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      return delegate.readInteger();
    }
  }

  static class RLEIntIterator extends IntIterator {
    RunLengthBitPackingHybridDecoder delegate;

    public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      try {
        return delegate.readInt();
      } catch (IOException e) {
        throw new ParquetDecodingException(e);
      }
    }
  }

  private static final class NullIntIterator extends IntIterator {
    @Override
    int nextInt() {
      return 0;
    }
  }
}
