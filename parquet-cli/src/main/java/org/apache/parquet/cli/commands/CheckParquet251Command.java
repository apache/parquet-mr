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

package org.apache.parquet.cli.commands;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;
import org.apache.parquet.util.DynConstructors;
import org.slf4j.Logger;

@Parameters(commandDescription = "Check Parquet files for corrupt page and column stats (PARQUET-251)")
public class CheckParquet251Command extends BaseCommand {

  public CheckParquet251Command(Logger console) {
    super(console);
  }

  @Parameter(description = "<files>", required = true)
  List<String> files;

  @Override
  public int run() throws IOException {
    boolean badFiles = false;
    for (String file : files) {
      String problem = check(file);
      if (problem != null) {
        badFiles = true;
        console.info("{} has corrupt stats: {}", file, problem);
      } else {
        console.info("{} has no corrupt stats", file);
      }
    }

    return badFiles ? 1 : 0;
  }

  private String check(String file) throws IOException {
    Path path = qualifiedPath(file);
    ParquetMetadata footer = ParquetFileReader.readFooter(getConf(), path, ParquetMetadataConverter.NO_FILTER);

    FileMetaData meta = footer.getFileMetaData();
    String createdBy = meta.getCreatedBy();
    if (CorruptStatistics.shouldIgnoreStatistics(createdBy, BINARY)) {
      // create fake metadata that will read corrupt stats and return them
      FileMetaData fakeMeta =
          new FileMetaData(meta.getSchema(), meta.getKeyValueMetaData(), Version.FULL_VERSION);

      // get just the binary columns
      List<ColumnDescriptor> columns = Lists.newArrayList();
      Iterables.addAll(
          columns, Iterables.filter(meta.getSchema().getColumns(), new Predicate<ColumnDescriptor>() {
            @Override
            public boolean apply(@Nullable ColumnDescriptor input) {
              return input != null && input.getType() == BINARY;
            }
          }));

      // now check to see if the data is actually corrupt
      try (ParquetFileReader reader =
          new ParquetFileReader(getConf(), fakeMeta, path, footer.getBlocks(), columns)) {
        PageStatsValidator validator = new PageStatsValidator();
        for (PageReadStore pages = reader.readNextRowGroup();
            pages != null;
            pages = reader.readNextRowGroup()) {
          validator.validate(columns, pages);
          pages.close();
        }
      } catch (BadStatsException e) {
        return e.getMessage();
      }
    }

    return null;
  }

  @Override
  public List<String> getExamples() {
    return Arrays.asList("# Check file1.parquet for corrupt page and column stats", "file1.parquet");
  }

  public static class BadStatsException extends RuntimeException {
    public BadStatsException(String message) {
      super(message);
    }
  }

  public class SingletonPageReader implements PageReader {
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
      return true;
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

  private class StatsValidator<T extends Comparable<T>> {
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
        if (comparator.compare(min, value) > 0) {
          throw new BadStatsException("Min should be <= all values.");
        }
        if (comparator.compare(max, value) < 0) {
          throw new BadStatsException("Max should be >= all values.");
        }
      }
    }
  }

  private PrimitiveConverter getValidatingConverter(final DataPage page, PrimitiveTypeName type) {
    return type.convert(new PrimitiveTypeNameConverter<PrimitiveConverter, RuntimeException>() {
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

  private static final DynConstructors.Ctor<ColumnReader> COL_READER_CTOR = new DynConstructors.Builder(
          ColumnReader.class)
      .hiddenImpl(
          "org.apache.parquet.column.impl.ColumnReaderImpl",
          ColumnDescriptor.class,
          PageReader.class,
          PrimitiveConverter.class,
          VersionParser.ParsedVersion.class)
      .build();

  public class PageStatsValidator {
    public void validate(List<ColumnDescriptor> columns, PageReadStore store) {
      for (ColumnDescriptor desc : columns) {
        PageReader reader = store.getPageReader(desc);
        DictionaryPage dict = reader.readDictionaryPage();
        DictionaryPage reusableDict = null;
        if (dict != null) {
          try {
            reusableDict = new DictionaryPage(
                BytesInput.from(dict.getBytes().toByteArray()),
                dict.getDictionarySize(),
                dict.getEncoding());
          } catch (IOException e) {
            throw new ParquetDecodingException("Cannot read dictionary", e);
          }
        }
        DataPage page;
        while ((page = reader.readPage()) != null) {
          validateStatsForPage(page, reusableDict, desc);
        }
      }
    }

    private void validateStatsForPage(DataPage page, DictionaryPage dict, ColumnDescriptor desc) {
      SingletonPageReader reader = new SingletonPageReader(dict, page);
      PrimitiveConverter converter = getValidatingConverter(page, desc.getType());
      Statistics stats = getStatisticsFromPageHeader(page);

      long numNulls = 0;

      ColumnReader column = COL_READER_CTOR.newInstance(desc, reader, converter, null);
      for (int i = 0; i < reader.getTotalValueCount(); i += 1) {
        if (column.getCurrentDefinitionLevel() >= desc.getMaxDefinitionLevel()) {
          column.writeCurrentValueToConverter();
        } else {
          numNulls += 1;
        }
        column.consume();
      }

      if (numNulls != stats.getNumNulls()) {
        throw new BadStatsException("Number of nulls doesn't match.");
      }

      console.debug(String.format(
          "Validated stats min=%s max=%s nulls=%d for page=%s col=%s",
          stats.minAsString(),
          stats.maxAsString(),
          stats.getNumNulls(),
          page,
          Arrays.toString(desc.getPath())));
    }
  }
}
