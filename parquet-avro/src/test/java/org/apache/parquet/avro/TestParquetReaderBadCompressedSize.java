package org.apache.parquet.avro;

import com.google.common.io.Resources;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Test;

/**
 * Validates that old files which have compressed size excluding the dictionary page size are readable
 */
public class TestParquetReaderBadCompressedSize {
  private static final String STATIC_FILE_BAD_COMPRESSED_SIZE = "test-bad-compressed-size.parquet";

  @Test
  public void testBadCompressedSize() throws Exception {
    Path testFile =
        new Path(Resources.getResource(STATIC_FILE_BAD_COMPRESSED_SIZE).getFile());
    Configuration configuration = new Configuration();
    try (ParquetReader<Object> parquet = AvroParquetReader.builder(testFile)
        .disableCompatibility()
        .withDataModel(GenericData.get())
        .withConf(configuration)
        .build()) {
      parquet.read();
    }
  }
}
