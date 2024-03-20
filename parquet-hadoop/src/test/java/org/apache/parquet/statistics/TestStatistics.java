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

package org.apache.parquet.statistics;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.Float16;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.statistics.RandomValues.RandomBinaryBase;
import org.apache.parquet.statistics.RandomValues.RandomValueGenerator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStatistics {

  private static final Logger LOG = LoggerFactory.getLogger(TestStatistics.class);

  private static final int MEGABYTE = 1 << 20;
  private static final long RANDOM_SEED = 1441990701846L; // System.currentTimeMillis();

  public static class SingletonPageReader implements PageReader {
    private final DictionaryPage dict;
    private final DataPage data;

    public SingletonPageReader(DictionaryPage dict, DataPage data) {
      this.dict = dict;
      this.data = data;
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      return dict;
    }

    @Override
    public long getTotalValueCount() {
      return data.getValueCount();
    }

    @Override
    public boolean isEager() {
      return false;
    }

    @Override
    public DataPage readPage() {
      return data;
    }
  }

  private static <T extends Comparable<T>> Statistics<T> getStatisticsFromPageHeader(DataPage page) {
    return page.accept(new DataPage.Visitor<Statistics<T>>() {
      @Override
      @SuppressWarnings("unchecked")
      public Statistics<T> visit(DataPageV1 dataPageV1) {
        return (Statistics<T>) dataPageV1.getStatistics();
      }

      @Override
      @SuppressWarnings("unchecked")
      public Statistics<T> visit(DataPageV2 dataPageV2) {
        return (Statistics<T>) dataPageV2.getStatistics();
      }
    });
  }

  private static class StatsValidator<T extends Comparable<T>> {
    private final boolean hasNonNull;
    private final T min;
    private final T max;
    private final Comparator<T> comparator;

    public StatsValidator(DataPage page) {
      Statistics<T> stats = getStatisticsFromPageHeader(page);
      this.comparator = stats.comparator();
      this.hasNonNull = stats.hasNonNullValue();
      if (hasNonNull) {
        this.min = stats.genericGetMin();
        this.max = stats.genericGetMax();
      } else {
        this.min = null;
        this.max = null;
      }
    }

    public void validate(T value) {
      if (hasNonNull) {
        assertTrue("min should be <= all values", comparator.compare(min, value) <= 0);
        assertTrue("min should be >= all values", comparator.compare(max, value) >= 0);
      }
    }
  }

  private static PrimitiveConverter getValidatingConverter(final DataPage page, PrimitiveTypeName type) {
    return type.convert(new PrimitiveType.PrimitiveTypeNameConverter<PrimitiveConverter, RuntimeException>() {
      @Override
      public PrimitiveConverter convertFLOAT(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Float> validator = new StatsValidator<Float>(page);
        return new PrimitiveConverter() {
          @Override
          public void addFloat(float value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertDOUBLE(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Double> validator = new StatsValidator<Double>(page);
        return new PrimitiveConverter() {
          @Override
          public void addDouble(double value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertINT32(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Integer> validator = new StatsValidator<Integer>(page);
        return new PrimitiveConverter() {
          @Override
          public void addInt(int value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertINT64(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Long> validator = new StatsValidator<Long>(page);
        return new PrimitiveConverter() {
          @Override
          public void addLong(long value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertBOOLEAN(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Boolean> validator = new StatsValidator<Boolean>(page);
        return new PrimitiveConverter() {
          @Override
          public void addBoolean(boolean value) {
            validator.validate(value);
          }
        };
      }

      @Override
      public PrimitiveConverter convertINT96(PrimitiveTypeName primitiveTypeName) {
        return convertBINARY(primitiveTypeName);
      }

      @Override
      public PrimitiveConverter convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) {
        return convertBINARY(primitiveTypeName);
      }

      @Override
      public PrimitiveConverter convertBINARY(PrimitiveTypeName primitiveTypeName) {
        final StatsValidator<Binary> validator = new StatsValidator<Binary>(page);
        return new PrimitiveConverter() {
          @Override
          public void addBinary(Binary value) {
            validator.validate(value);
          }
        };
      }
    });
  }

  public static class PageStatsValidator {
    public void validate(MessageType schema, PageReadStore store) {
      for (ColumnDescriptor desc : schema.getColumns()) {
        PageReader reader = store.getPageReader(desc);
        DictionaryPage dict = reader.readDictionaryPage();
        DataPage page;
        while ((page = reader.readPage()) != null) {
          validateStatsForPage(page, dict, desc);
        }
      }
    }

    private void validateStatsForPage(DataPage page, DictionaryPage dict, ColumnDescriptor desc) {
      SingletonPageReader reader = new SingletonPageReader(dict, page);
      PrimitiveConverter converter = getValidatingConverter(page, desc.getType());
      Statistics<?> stats = getStatisticsFromPageHeader(page);

      assertEquals(
          "Statistics does not use the proper comparator",
          desc.getPrimitiveType().comparator().getClass(),
          stats.comparator().getClass());

      if (stats.isEmpty()) {
        // stats are empty if num nulls = 0 and there are no non-null values
        // this happens if stats are not written (e.g., when stats are too big)
        return;
      }

      long numNulls = 0;
      ColumnReaderImpl column = new ColumnReaderImpl(desc, reader, converter, null);
      for (int i = 0; i < reader.getTotalValueCount(); i += 1) {
        if (column.getCurrentDefinitionLevel() >= desc.getMaxDefinitionLevel()) {
          column.writeCurrentValueToConverter();
        } else {
          numNulls += 1;
        }
        column.consume();
      }

      Assert.assertEquals(numNulls, stats.getNumNulls());
    }
  }

  public static class DataContext extends DataGenerationContext.WriteContext {
    private static final int MAX_TOTAL_ROWS = 1000000;

    private final Random random;
    private final int recordCount;

    private final List<RandomValueGenerator<?>> randomGenerators;

    public DataContext(
        long seed,
        File path,
        int blockSize,
        int pageSize,
        boolean enableDictionary,
        ParquetProperties.WriterVersion version)
        throws IOException {
      super(path, buildSchema(seed), blockSize, pageSize, enableDictionary, true, version);

      this.random = new Random(seed);
      this.recordCount = random.nextInt(MAX_TOTAL_ROWS);

      int fixedLength = schema.getType("fixed-binary").asPrimitiveType().getTypeLength();

      randomGenerators = Arrays.<RandomValueGenerator<?>>asList(
          new RandomValues.IntGenerator(random.nextLong()),
          new RandomValues.LongGenerator(random.nextLong()),
          new RandomValues.Int96Generator(random.nextLong()),
          new RandomValues.FloatGenerator(random.nextLong()),
          new RandomValues.DoubleGenerator(random.nextLong()),
          new RandomValues.StringGenerator(random.nextLong()),
          new RandomValues.BinaryGenerator(random.nextLong()),
          new RandomValues.FixedGenerator(random.nextLong(), fixedLength),
          new RandomValues.UnconstrainedIntGenerator(random.nextLong()),
          new RandomValues.UnconstrainedLongGenerator(random.nextLong()),
          new RandomValues.UnconstrainedFloatGenerator(random.nextLong()),
          new RandomValues.UnconstrainedDoubleGenerator(random.nextLong()),
          new RandomValues.IntGenerator(random.nextLong(), Byte.MIN_VALUE, Byte.MAX_VALUE),
          new RandomValues.UIntGenerator(random.nextLong(), Byte.MIN_VALUE, Byte.MAX_VALUE),
          new RandomValues.IntGenerator(random.nextLong(), Short.MIN_VALUE, Short.MAX_VALUE),
          new RandomValues.UIntGenerator(random.nextLong(), Short.MIN_VALUE, Short.MAX_VALUE),
          new RandomValues.UnconstrainedIntGenerator(random.nextLong()),
          new RandomValues.UnconstrainedIntGenerator(random.nextLong()),
          new RandomValues.UnconstrainedLongGenerator(random.nextLong()),
          new RandomValues.UnconstrainedLongGenerator(random.nextLong()),
          new RandomValues.UnconstrainedIntGenerator(random.nextLong()),
          new RandomValues.UnconstrainedLongGenerator(random.nextLong()),
          new RandomValues.FixedGenerator(random.nextLong(), fixedLength),
          new RandomValues.BinaryGenerator(random.nextLong()),
          new RandomValues.StringGenerator(random.nextLong()),
          new RandomValues.StringGenerator(random.nextLong()),
          new RandomValues.StringGenerator(random.nextLong()),
          new RandomValues.BinaryGenerator(random.nextLong()),
          new RandomValues.IntGenerator(random.nextLong()),
          new RandomValues.IntGenerator(random.nextLong()),
          new RandomValues.LongGenerator(random.nextLong()),
          new RandomValues.LongGenerator(random.nextLong()),
          new RandomValues.LongGenerator(random.nextLong()),
          new RandomValues.FixedGenerator(random.nextLong(), 12),
          new RandomValues.FixedGenerator(random.nextLong(), 2));
    }

    private static MessageType buildSchema(long seed) {
      Random random = new Random(seed);
      int fixedBinaryLength = random.nextInt(21) + 1;
      int fixedPrecision = calculatePrecision(fixedBinaryLength);
      int fixedScale = fixedPrecision / 4;
      int binaryPrecision = calculatePrecision(16);
      int binaryScale = binaryPrecision / 4;

      return new MessageType(
          "schema",
          new PrimitiveType(OPTIONAL, INT32, "i32"),
          new PrimitiveType(OPTIONAL, INT64, "i64"),
          new PrimitiveType(OPTIONAL, INT96, "i96"),
          new PrimitiveType(OPTIONAL, FLOAT, "sngl"),
          new PrimitiveType(OPTIONAL, DOUBLE, "dbl"),
          new PrimitiveType(OPTIONAL, BINARY, "strings"),
          new PrimitiveType(OPTIONAL, BINARY, "binary"),
          new PrimitiveType(OPTIONAL, FIXED_LEN_BYTE_ARRAY, fixedBinaryLength, "fixed-binary"),
          new PrimitiveType(REQUIRED, INT32, "unconstrained-i32"),
          new PrimitiveType(REQUIRED, INT64, "unconstrained-i64"),
          new PrimitiveType(REQUIRED, FLOAT, "unconstrained-sngl"),
          new PrimitiveType(REQUIRED, DOUBLE, "unconstrained-dbl"),
          Types.optional(INT32).as(OriginalType.INT_8).named("int8"),
          Types.optional(INT32).as(OriginalType.UINT_8).named("uint8"),
          Types.optional(INT32).as(OriginalType.INT_16).named("int16"),
          Types.optional(INT32).as(OriginalType.UINT_16).named("uint16"),
          Types.optional(INT32).as(OriginalType.INT_32).named("int32"),
          Types.optional(INT32).as(OriginalType.UINT_32).named("uint32"),
          Types.optional(INT64).as(OriginalType.INT_64).named("int64"),
          Types.optional(INT64).as(OriginalType.UINT_64).named("uint64"),
          Types.optional(INT32)
              .as(OriginalType.DECIMAL)
              .precision(9)
              .scale(2)
              .named("decimal-int32"),
          Types.optional(INT64)
              .as(OriginalType.DECIMAL)
              .precision(18)
              .scale(4)
              .named("decimal-int64"),
          Types.optional(FIXED_LEN_BYTE_ARRAY)
              .length(fixedBinaryLength)
              .as(OriginalType.DECIMAL)
              .precision(fixedPrecision)
              .scale(fixedScale)
              .named("decimal-fixed"),
          Types.optional(BINARY)
              .as(OriginalType.DECIMAL)
              .precision(binaryPrecision)
              .scale(binaryScale)
              .named("decimal-binary"),
          Types.optional(BINARY).as(OriginalType.UTF8).named("utf8"),
          Types.optional(BINARY).as(OriginalType.ENUM).named("enum"),
          Types.optional(BINARY).as(OriginalType.JSON).named("json"),
          Types.optional(BINARY).as(OriginalType.BSON).named("bson"),
          Types.optional(INT32).as(OriginalType.DATE).named("date"),
          Types.optional(INT32).as(OriginalType.TIME_MILLIS).named("time-millis"),
          Types.optional(INT64).as(OriginalType.TIME_MICROS).named("time-micros"),
          Types.optional(INT64).as(OriginalType.TIMESTAMP_MILLIS).named("timestamp-millis"),
          Types.optional(INT64).as(OriginalType.TIMESTAMP_MICROS).named("timestamp-micros"),
          Types.optional(FIXED_LEN_BYTE_ARRAY)
              .length(12)
              .as(OriginalType.INTERVAL)
              .named("interval"),
          Types.optional(FIXED_LEN_BYTE_ARRAY)
              .length(2)
              .named("float16")
              .withLogicalTypeAnnotation(LogicalTypeAnnotation.float16Type()));
    }

    private static int calculatePrecision(int byteCnt) {
      String maxValue = BigInteger.valueOf(2L).pow(8 * byteCnt - 1).toString();
      return maxValue.length() - 1;
    }

    @Override
    public void write(ParquetWriter<Group> writer) throws IOException {
      for (int index = 0; index < recordCount; index++) {
        Group group = new SimpleGroup(super.schema);

        for (int column = 0, columnCnt = schema.getFieldCount(); column < columnCnt; ++column) {
          Type type = schema.getType(column);
          RandomValueGenerator<?> generator = randomGenerators.get(column);
          if (type.isRepetition(OPTIONAL) && generator.shouldGenerateNull()) {
            continue;
          }
          switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
              if (type.getLogicalTypeAnnotation()
                  instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
                Binary b = ((RandomBinaryBase<?>) generator).nextBinaryValue();
                // return smallest negative value a half-precision float when it is NaN
                Binary v = Float16.isNaN(b.get2BytesLittleEndian())
                    ? b
                    : Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0xfb});
                group.append(type.getName(), v);
                break;
              }
            case INT96:
              group.append(type.getName(), ((RandomBinaryBase<?>) generator).nextBinaryValue());
              break;
            case INT32:
              group.append(type.getName(), (Integer) generator.nextValue());
              break;
            case INT64:
              group.append(type.getName(), (Long) generator.nextValue());
              break;
            case FLOAT:
              group.append(type.getName(), (Float) generator.nextValue());
              break;
            case DOUBLE:
              group.append(type.getName(), (Double) generator.nextValue());
              break;
            case BOOLEAN:
              group.append(type.getName(), (Boolean) generator.nextValue());
              break;
          }
        }
        writer.write(group);
      }
    }

    @Override
    public void test() throws IOException {
      Configuration configuration = new Configuration();
      ParquetMetadata metadata =
          ParquetFileReader.readFooter(configuration, super.fsPath, ParquetMetadataConverter.NO_FILTER);
      try (ParquetFileReader reader = new ParquetFileReader(
          configuration,
          metadata.getFileMetaData(),
          super.fsPath,
          metadata.getBlocks(),
          metadata.getFileMetaData().getSchema().getColumns())) {

        PageStatsValidator validator = new PageStatsValidator();

        PageReadStore pageReadStore;
        while ((pageReadStore = reader.readNextRowGroup()) != null) {
          validator.validate(metadata.getFileMetaData().getSchema(), pageReadStore);
        }
      }
    }
  }

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testStatistics() throws IOException {
    File file = folder.newFile("test_file.parquet");
    file.delete();

    LOG.info(String.format("RANDOM SEED: %s", RANDOM_SEED));

    Random random = new Random(RANDOM_SEED);

    int blockSize = (random.nextInt(54) + 10) * MEGABYTE;
    int pageSize = (random.nextInt(10) + 1) * MEGABYTE;

    List<DataContext> contexts = Arrays.asList(
        new DataContext(
            random.nextLong(),
            file,
            blockSize,
            pageSize,
            false,
            ParquetProperties.WriterVersion.PARQUET_1_0),
        new DataContext(
            random.nextLong(),
            file,
            blockSize,
            pageSize,
            true,
            ParquetProperties.WriterVersion.PARQUET_1_0),
        new DataContext(
            random.nextLong(),
            file,
            blockSize,
            pageSize,
            false,
            ParquetProperties.WriterVersion.PARQUET_2_0),
        new DataContext(
            random.nextLong(),
            file,
            blockSize,
            pageSize,
            true,
            ParquetProperties.WriterVersion.PARQUET_2_0));

    for (DataContext test : contexts) {
      DataGenerationContext.writeAndTest(test);
    }
  }
}
