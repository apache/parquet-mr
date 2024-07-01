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
package org.apache.parquet.crypto;

import static org.apache.parquet.hadoop.ParquetFileWriter.EFMAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.EF_MAGIC_STR;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.crypto.keytools.KeyToolkit;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;
import org.apache.parquet.crypto.keytools.mocks.InMemoryKMS;
import org.apache.parquet.crypto.keytools.mocks.LocalWrapInMemoryKMS;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This file contains samples for writing and reading encrypted Parquet files in different
 * encryption and decryption configurations, set using a properties-driven interface.
 *
 * The write sample produces number of parquet files, each encrypted with a different
 * encryption configuration as described below.
 * The name of each file is in the form of:
 * <encryption-configuration-name>.parquet.encrypted or
 * NO_ENCRYPTION.parquet for plaintext file.
 *
 * The read sample creates a set of decryption configurations and then uses each of them
 * to read all encrypted files in the input directory.
 *
 * The different encryption and decryption configurations are listed below.
 *
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * The write sample creates files with eight columns in the following
 * encryption configurations:
 *
 *  - ENCRYPT_COLUMNS_AND_FOOTER:   Encrypt two columns and the footer, with different
 *                                  keys.
 *  - ENCRYPT_COLUMNS_PLAINTEXT_FOOTER:   Encrypt two columns, with different keys.
 *                                  Do not encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  - ENCRYPT_COLUMNS_AND_FOOTER_CTR:   Encrypt two columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 *  - COMPLETE_COLUMN_ENCRYPTION:   Encrypt two columns and the footer, with different
 *                                  keys. Encrypt other columns with the footer key.
 *  - UNIFORM_ENCRYPTION:           Encrypt all columns and footer with the same master key.
 *  - NO_ENCRYPTION:   Do not encrypt anything
 *
 *
 *
 * The read sample uses each of the following decryption configurations to read every
 * encrypted files in the input directory:
 *
 *  - DECRYPT_WITH_KEY_RETRIEVER:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key.
 *  - NO_DECRYPTION:   Do not decrypt anything.
 */
@RunWith(Parameterized.class)
public class TestPropertiesDrivenEncryption {
  @Parameterized.Parameters(
      name =
          "Run {index}: isKeyMaterialInternalStorage={0} isDoubleWrapping={1} isWrapLocally={2} isDecryptionDirectMemory={3} isV1={4}")
  public static Collection<Object[]> data() {
    Collection<Object[]> list = new ArrayList<>(8);
    boolean[] flagValues = {false, true};
    for (boolean keyMaterialInternalStorage : flagValues) {
      for (boolean doubleWrapping : flagValues) {
        for (boolean wrapLocally : flagValues) {
          for (boolean isDecryptionDirectMemory : flagValues) {
            for (boolean isV1 : flagValues) {
              Object[] vector = {
                keyMaterialInternalStorage, doubleWrapping, wrapLocally, isDecryptionDirectMemory, isV1
              };
              list.add(vector);
            }
          }
        }
      }
    }
    return list;
  }

  @Parameterized.Parameter // first data value (0) is default
  public boolean isKeyMaterialInternalStorage;

  @Parameterized.Parameter(value = 1)
  public boolean isDoubleWrapping;

  @Parameterized.Parameter(value = 2)
  public boolean isWrapLocally;

  @Parameterized.Parameter(value = 3)
  public boolean isDecryptionDirectMemory;

  @Parameterized.Parameter(value = 4)
  public boolean isV1;

  private static final Logger LOG = LoggerFactory.getLogger(TestPropertiesDrivenEncryption.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final ErrorCollector errorCollector = new ErrorCollector();

  private static final Base64.Encoder encoder = Base64.getEncoder();
  private static final String FOOTER_MASTER_KEY =
      encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8));
  private static final String[] COLUMN_MASTER_KEYS = {
    encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("1234567890123451".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("1234567890123452".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("1234567890123453".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("1234567890123454".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("1234567890123455".getBytes(StandardCharsets.UTF_8))
  };
  private static final String UNIFORM_MASTER_KEY =
      encoder.encodeToString("0123456789012346".getBytes(StandardCharsets.UTF_8));
  private static final String[] COLUMN_MASTER_KEY_IDS = {"kc1", "kc2", "kc3", "kc4", "kc5", "kc6"};
  private static final String FOOTER_MASTER_KEY_ID = "kf";
  private static final String UNIFORM_MASTER_KEY_ID = "ku";

  private static final String KEY_LIST = new StringBuilder()
      .append(COLUMN_MASTER_KEY_IDS[0])
      .append(": ")
      .append(COLUMN_MASTER_KEYS[0])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[1])
      .append(": ")
      .append(COLUMN_MASTER_KEYS[1])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[2])
      .append(": ")
      .append(COLUMN_MASTER_KEYS[2])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[3])
      .append(": ")
      .append(COLUMN_MASTER_KEYS[3])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[4])
      .append(": ")
      .append(COLUMN_MASTER_KEYS[4])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[5])
      .append(": ")
      .append(COLUMN_MASTER_KEYS[5])
      .append(", ")
      .append(UNIFORM_MASTER_KEY_ID)
      .append(": ")
      .append(UNIFORM_MASTER_KEY)
      .append(", ")
      .append(FOOTER_MASTER_KEY_ID)
      .append(": ")
      .append(FOOTER_MASTER_KEY)
      .toString();

  private static final String NEW_FOOTER_MASTER_KEY =
      encoder.encodeToString("9123456789012345".getBytes(StandardCharsets.UTF_8));
  private static final String[] NEW_COLUMN_MASTER_KEYS = {
    encoder.encodeToString("9234567890123450".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("9234567890123451".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("9234567890123452".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("9234567890123453".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("9234567890123454".getBytes(StandardCharsets.UTF_8)),
    encoder.encodeToString("9234567890123455".getBytes(StandardCharsets.UTF_8))
  };
  private static final String NEW_UNIFORM_MASTER_KEY =
      encoder.encodeToString("9123456789012346".getBytes(StandardCharsets.UTF_8));

  private static final String NEW_KEY_LIST = new StringBuilder()
      .append(COLUMN_MASTER_KEY_IDS[0])
      .append(": ")
      .append(NEW_COLUMN_MASTER_KEYS[0])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[1])
      .append(": ")
      .append(NEW_COLUMN_MASTER_KEYS[1])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[2])
      .append(": ")
      .append(NEW_COLUMN_MASTER_KEYS[2])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[3])
      .append(": ")
      .append(NEW_COLUMN_MASTER_KEYS[3])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[4])
      .append(": ")
      .append(NEW_COLUMN_MASTER_KEYS[4])
      .append(", ")
      .append(COLUMN_MASTER_KEY_IDS[5])
      .append(": ")
      .append(NEW_COLUMN_MASTER_KEYS[5])
      .append(", ")
      .append(UNIFORM_MASTER_KEY_ID)
      .append(": ")
      .append(NEW_UNIFORM_MASTER_KEY)
      .append(", ")
      .append(FOOTER_MASTER_KEY_ID)
      .append(": ")
      .append(NEW_FOOTER_MASTER_KEY)
      .toString();

  private static final String COLUMN_KEY_MAPPING = new StringBuilder()
      .append(COLUMN_MASTER_KEY_IDS[0])
      .append(": ")
      .append(SingleRow.DOUBLE_FIELD_NAME)
      .append("; ")
      .append(COLUMN_MASTER_KEY_IDS[1])
      .append(": ")
      .append(SingleRow.FLOAT_FIELD_NAME)
      .append("; ")
      .append(COLUMN_MASTER_KEY_IDS[2])
      .append(": ")
      .append(SingleRow.BOOLEAN_FIELD_NAME)
      .append("; ")
      .append(COLUMN_MASTER_KEY_IDS[3])
      .append(": ")
      .append(SingleRow.INT32_FIELD_NAME)
      .append("; ")
      .append(COLUMN_MASTER_KEY_IDS[4])
      .append(": ")
      .append(SingleRow.BINARY_FIELD_NAME)
      .append("; ")
      .append(COLUMN_MASTER_KEY_IDS[5])
      .append(": ")
      .append(SingleRow.FIXED_LENGTH_BINARY_FIELD_NAME)
      .toString();

  private static final int NUM_THREADS = 4;
  private static final int WAIT_FOR_WRITE_TO_END_SECONDS = 5;
  private static final int WAIT_FOR_READ_TO_END_SECONDS = 5;

  private static final boolean plaintextFilesAllowed = true;

  // AesCtrDecryptor has a loop to update the cipher in chunks of  CHUNK_LENGTH (4K). Use a large
  // enough number of rows to ensure that the data generated is greater than the chunk length.
  private static final int ROW_COUNT = 50000;
  private static final List<SingleRow> DATA = Collections.unmodifiableList(SingleRow.generateRandomData(ROW_COUNT));

  public enum EncryptionConfiguration {
    ENCRYPT_COLUMNS_AND_FOOTER {
      /**
       * Encrypt two columns and the footer, with different master keys.
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        Configuration conf = getCryptoProperties(test);
        setColumnAndFooterKeys(conf);
        return conf;
      }
    },
    UNIFORM_ENCRYPTION {
      /**
       * Encrypt all columns and the footer, with same master key.
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        Configuration conf = getCryptoProperties(test);
        setUniformKey(conf);
        return conf;
      }
    },
    ENCRYPT_COLUMNS_PLAINTEXT_FOOTER {
      /**
       * Encrypt two columns, with different master keys.
       * Don't encrypt footer.
       * (plaintext footer mode, readable by legacy readers)
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        Configuration conf = getCryptoProperties(test);
        setColumnAndFooterKeys(conf);
        conf.setBoolean(PropertiesDrivenCryptoFactory.PLAINTEXT_FOOTER_PROPERTY_NAME, true);
        return conf;
      }
    },
    ENCRYPT_COLUMNS_AND_FOOTER_CTR {
      /**
       * Encrypt two columns and the footer, with different master keys.
       * Use AES_GCM_CTR_V1 algorithm.
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        Configuration conf = getCryptoProperties(test);
        setColumnAndFooterKeys(conf);
        conf.set(
            PropertiesDrivenCryptoFactory.ENCRYPTION_ALGORITHM_PROPERTY_NAME,
            ParquetCipher.AES_GCM_CTR_V1.toString());
        return conf;
      }
    },
    COMPLETE_COLUMN_ENCRYPTION {
      /**
       * Encrypt two columns and the footer, with different master keys.
       * Encrypt other columns with the footer master key.
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        Configuration conf = getCryptoProperties(test);
        setColumnAndFooterKeys(conf);
        conf.setBoolean(PropertiesDrivenCryptoFactory.COMPLETE_COLUMN_ENCRYPTION_PROPERTY_NAME, true);
        return conf;
      }
    },
    NO_ENCRYPTION {
      /**
       * Do not encrypt anything
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        return null;
      }
    };

    public abstract Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test);
  }

  public enum DecryptionConfiguration {
    DECRYPT_WITH_KEY_RETRIEVER {
      /**
       * Decrypt using key retriever callback that holds the keys
       * of two encrypted columns and the footer key.
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        Configuration conf = getCryptoProperties(test);
        return conf;
      }
    },
    NO_DECRYPTION {
      /**
       * Do not decrypt anything.
       */
      public Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test) {
        return null;
      }
    };

    public abstract Configuration getHadoopConfiguration(TestPropertiesDrivenEncryption test);
  }

  /**
   * Get Hadoop configuration with configuration properties common to all encryption modes
   */
  private static Configuration getCryptoProperties(TestPropertiesDrivenEncryption test) {
    Configuration conf = new Configuration();
    conf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        PropertiesDrivenCryptoFactory.class.getName());

    conf.set(OFF_HEAP_DECRYPT_BUFFER_ENABLED, String.valueOf(test.isDecryptionDirectMemory));

    if (test.isWrapLocally) {
      conf.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, LocalWrapInMemoryKMS.class.getName());
    } else {
      conf.set(KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME, InMemoryKMS.class.getName());
    }
    conf.set(InMemoryKMS.KEY_LIST_PROPERTY_NAME, KEY_LIST);
    conf.set(InMemoryKMS.NEW_KEY_LIST_PROPERTY_NAME, NEW_KEY_LIST);

    conf.setBoolean(KeyToolkit.KEY_MATERIAL_INTERNAL_PROPERTY_NAME, test.isKeyMaterialInternalStorage);
    conf.setBoolean(KeyToolkit.DOUBLE_WRAPPING_PROPERTY_NAME, test.isDoubleWrapping);
    return conf;
  }

  /**
   * Set configuration properties to encrypt columns and the footer with different master keys
   */
  private static void setColumnAndFooterKeys(Configuration conf) {
    conf.set(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, COLUMN_KEY_MAPPING);
    conf.set(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, FOOTER_MASTER_KEY_ID);
  }

  /**
   * Set uniform encryption configuration property
   */
  private static void setUniformKey(Configuration conf) {
    conf.set(PropertiesDrivenCryptoFactory.UNIFORM_KEY_PROPERTY_NAME, UNIFORM_MASTER_KEY_ID);
  }

  @Test
  public void testWriteReadEncryptedParquetFiles() throws IOException {
    Path rootPath = new Path(temporaryFolder.getRoot().getPath());
    LOG.info("======== testWriteReadEncryptedParquetFiles {} ========", rootPath.toString());
    LOG.info(
        "Run: isKeyMaterialInternalStorage={} isDoubleWrapping={} isWrapLocally={}",
        isKeyMaterialInternalStorage,
        isDoubleWrapping,
        isWrapLocally);
    KeyToolkit.removeCacheEntriesForAllTokens();
    ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);
    try {
      // Write using various encryption configurations.
      testWriteEncryptedParquetFiles(rootPath, DATA, threadPool);
      // Read using various decryption configurations.
      testReadEncryptedParquetFiles(rootPath, DATA, threadPool, false);
    } finally {
      threadPool.shutdown();
    }
  }

  @Test
  public void testWriteReadEncryptedParquetFilesAsync() throws IOException {
    Path rootPath = new Path(temporaryFolder.getRoot().getPath());
    LOG.info("======== testWriteReadEncryptedParquetFiles {} ========", rootPath.toString());
    LOG.info(
        "Run: isKeyMaterialInternalStorage={} isDoubleWrapping={} isWrapLocally={}",
        isKeyMaterialInternalStorage,
        isDoubleWrapping,
        isWrapLocally);
    KeyToolkit.removeCacheEntriesForAllTokens();
    ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);
    try {
      // Write using various encryption configurations.
      testWriteEncryptedParquetFiles(rootPath, DATA, threadPool);
      // Read using various decryption configurations.
      testReadEncryptedParquetFiles(rootPath, DATA, threadPool, true);
    } finally {
      threadPool.shutdown();
    }
  }

  private void testWriteEncryptedParquetFiles(Path root, List<SingleRow> data, ExecutorService threadPool)
      throws IOException {
    EncryptionConfiguration[] encryptionConfigurations = EncryptionConfiguration.values();
    for (EncryptionConfiguration encryptionConfiguration : encryptionConfigurations) {
      Path encryptionConfigurationFolderPath = new Path(root, encryptionConfiguration.name());
      Configuration conf = new Configuration();
      FileSystem fs = encryptionConfigurationFolderPath.getFileSystem(conf);
      if (fs.exists(encryptionConfigurationFolderPath)) {
        fs.delete(encryptionConfigurationFolderPath, true);
      }
      fs.mkdirs(encryptionConfigurationFolderPath);

      KeyToolkit.removeCacheEntriesForAllTokens();
      CountDownLatch latch = new CountDownLatch(NUM_THREADS);
      for (int i = 0; i < NUM_THREADS; ++i) {
        final int threadNumber = i;
        threadPool.execute(() -> {
          writeEncryptedParquetFile(
              encryptionConfigurationFolderPath, data, encryptionConfiguration, threadNumber);
          latch.countDown();
        });
      }
      try {
        latch.await(WAIT_FOR_WRITE_TO_END_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void writeEncryptedParquetFile(
      Path root, List<SingleRow> data, EncryptionConfiguration encryptionConfiguration, int threadNumber) {
    MessageType schema = SingleRow.getSchema();
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    int pageSize = data.size() / 10; // Ensure that several pages will be created
    int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created

    Path file = new Path(root, getFileName(root, encryptionConfiguration, threadNumber));
    LOG.info("\nWrite " + file.toString());
    Configuration conf = encryptionConfiguration.getHadoopConfiguration(this);
    FileEncryptionProperties fileEncryptionProperties = null;
    try {
      if (null == conf) {
        conf = new Configuration();
      } else {
        EncryptionPropertiesFactory cryptoFactory = EncryptionPropertiesFactory.loadFactory(conf);
        fileEncryptionProperties = cryptoFactory.getFileEncryptionProperties(conf, file, null);
      }
    } catch (Exception e) {
      addErrorToErrorCollectorAndLog("Failed writing " + file.toString(), e, encryptionConfiguration, null);
      return;
    }
    WriterVersion writerVersion = this.isV1 ? WriterVersion.PARQUET_1_0 : WriterVersion.PARQUET_2_0;
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withConf(conf)
        .withWriteMode(OVERWRITE)
        .withType(schema)
        .withPageSize(pageSize)
        .withRowGroupSize(rowGroupSize)
        .withEncryption(fileEncryptionProperties)
        .withWriterVersion(writerVersion)
        .build()) {

      for (SingleRow singleRow : data) {
        writer.write(f.newGroup()
            .append(SingleRow.BOOLEAN_FIELD_NAME, singleRow.boolean_field)
            .append(SingleRow.INT32_FIELD_NAME, singleRow.int32_field)
            .append(SingleRow.FLOAT_FIELD_NAME, singleRow.float_field)
            .append(SingleRow.DOUBLE_FIELD_NAME, singleRow.double_field)
            .append(SingleRow.BINARY_FIELD_NAME, Binary.fromConstantByteArray(singleRow.ba_field))
            .append(
                SingleRow.FIXED_LENGTH_BINARY_FIELD_NAME,
                Binary.fromConstantByteArray(singleRow.flba_field))
            .append(SingleRow.PLAINTEXT_INT32_FIELD_NAME, singleRow.plaintext_int32_field));
      }
    } catch (Exception e) {
      addErrorToErrorCollectorAndLog("Failed writing " + file.toString(), e, encryptionConfiguration, null);
    }
  }

  private Path getFileName(Path root, EncryptionConfiguration encryptionConfiguration, int threadNumber) {
    String suffix =
        (EncryptionConfiguration.NO_ENCRYPTION == encryptionConfiguration) ? ".parquet" : ".parquet.encrypted";
    return new Path(root, encryptionConfiguration.toString() + "_" + threadNumber + suffix);
  }

  private void testReadEncryptedParquetFiles(
      Path root, List<SingleRow> data, ExecutorService threadPool, boolean useAsyncIO) throws IOException {
    readFilesMultithreaded(root, data, threadPool, false /*keysRotated*/, useAsyncIO);

    if (isWrapLocally) {
      return; // key rotation is not supported with local key wrapping
    }

    LOG.info("--> Start master key rotation");
    Configuration hadoopConfigForRotation =
        EncryptionConfiguration.ENCRYPT_COLUMNS_AND_FOOTER.getHadoopConfiguration(this);
    hadoopConfigForRotation.set(InMemoryKMS.NEW_KEY_LIST_PROPERTY_NAME, NEW_KEY_LIST);
    InMemoryKMS.startKeyRotation(hadoopConfigForRotation);

    EncryptionConfiguration[] encryptionConfigurations = EncryptionConfiguration.values();
    for (EncryptionConfiguration encryptionConfiguration : encryptionConfigurations) {
      if (EncryptionConfiguration.NO_ENCRYPTION == encryptionConfiguration) {
        continue; // no rotation of plaintext files
      }
      Path encryptionConfigurationFolderPath = new Path(root, encryptionConfiguration.name());
      try {
        LOG.info("Rotate master keys in folder: " + encryptionConfigurationFolderPath.toString());
        KeyToolkit.rotateMasterKeys(encryptionConfigurationFolderPath.toString(), hadoopConfigForRotation);
      } catch (UnsupportedOperationException e) {
        if (isKeyMaterialInternalStorage || isWrapLocally) {
          LOG.info("Key material file not found, as expected");
        } else {
          errorCollector.addError(e);
        }
        return; // No use in continuing reading if rotation wasn't successful
      } catch (Exception e) {
        errorCollector.addError(e);
        return; // No use in continuing reading if rotation wasn't successful
      }
    }

    InMemoryKMS.finishKeyRotation();
    LOG.info("--> Finish master key rotation");

    LOG.info("--> Read files again with new keys");
    readFilesMultithreaded(root, data, threadPool, true /*keysRotated*/, useAsyncIO);
  }

  private void readFilesMultithreaded(
      Path root, List<SingleRow> data, ExecutorService threadPool, boolean keysRotated, boolean useAsyncIO) {
    DecryptionConfiguration[] decryptionConfigurations = DecryptionConfiguration.values();
    for (DecryptionConfiguration decryptionConfiguration : decryptionConfigurations) {
      LOG.info("\n\n");
      LOG.info("==> Decryption configuration {}\n", decryptionConfiguration);
      Configuration hadoopConfig = decryptionConfiguration.getHadoopConfiguration(this);
      if (null != hadoopConfig) {
        KeyToolkit.removeCacheEntriesForAllTokens();
      }

      EncryptionConfiguration[] encryptionConfigurations = EncryptionConfiguration.values();
      for (EncryptionConfiguration encryptionConfiguration : encryptionConfigurations) {
        Path encryptionConfigurationFolderPath = new Path(root, encryptionConfiguration.name());

        CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        for (int i = 0; i < NUM_THREADS; ++i) {
          final int threadNumber = i;
          threadPool.execute(() -> {
            Path file =
                getFileName(encryptionConfigurationFolderPath, encryptionConfiguration, threadNumber);
            LOG.info("--> Read file {} {}", file.toString(), encryptionConfiguration);
            readFileAndCheckResult(
                hadoopConfig,
                encryptionConfiguration,
                decryptionConfiguration,
                data,
                file,
                keysRotated,
                useAsyncIO);

            latch.countDown();
          });
        }
        try {
          latch.await(WAIT_FOR_READ_TO_END_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void readFileAndCheckResult(
      Configuration hadoopConfig,
      EncryptionConfiguration encryptionConfiguration,
      DecryptionConfiguration decryptionConfiguration,
      List<SingleRow> data,
      Path file,
      boolean keysRotated,
      boolean useAsyncIO) {
    FileDecryptionProperties fileDecryptionProperties = null;
    if (null == hadoopConfig) {
      hadoopConfig = new Configuration();
    } else {
      DecryptionPropertiesFactory cryptoFactory = DecryptionPropertiesFactory.loadFactory(hadoopConfig);
      fileDecryptionProperties = cryptoFactory.getFileDecryptionProperties(hadoopConfig, file);
    }

    // Set schema to only point to the non-encrypted columns
    if ((decryptionConfiguration == DecryptionConfiguration.NO_DECRYPTION)
        && (encryptionConfiguration == EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
      hadoopConfig.set(
          "parquet.read.schema",
          Types.buildMessage()
              .optional(INT32)
              .named(SingleRow.PLAINTEXT_INT32_FIELD_NAME)
              .named("FormatTestObject")
              .toString());
    }
    if ((encryptionConfiguration != EncryptionConfiguration.NO_ENCRYPTION)
        && (encryptionConfiguration != EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
      byte[] magic = new byte[MAGIC.length];
      try (InputStream is = new FileInputStream(file.toString())) {
        if (is.read(magic) != magic.length) {
          throw new RuntimeException("ERROR");
        }
        if (!Arrays.equals(EFMAGIC, magic)) {
          addErrorToErrorCollectorAndLog(
              "File doesn't start with " + EF_MAGIC_STR,
              encryptionConfiguration,
              decryptionConfiguration);
        }
      } catch (IOException e) {
        addErrorToErrorCollectorAndLog(
            "Failed to read magic string at the beginning of file",
            e,
            encryptionConfiguration,
            decryptionConfiguration);
      }
    }

    if (keysRotated && (null != hadoopConfig.get(InMemoryKMS.KEY_LIST_PROPERTY_NAME))) {
      hadoopConfig.set(InMemoryKMS.KEY_LIST_PROPERTY_NAME, NEW_KEY_LIST);
    }

    if (useAsyncIO) {
      hadoopConfig.set(ParquetInputFormat.ENABLE_ASYNC_IO_READER, "true");
      hadoopConfig.set(ParquetInputFormat.ENABLE_PARALLEL_COLUMN_READER, "true");
    }
    ExecutorService parquetIOThreadPool = Executors.newFixedThreadPool(NUM_THREADS);
    ExecutorService parquetProcessThreadPool = Executors.newFixedThreadPool(NUM_THREADS);

    int rowNum = 0;
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(
            this.isDecryptionDirectMemory
                ? new DirectByteBufferAllocator()
                : new HeapByteBufferAllocator());
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
            .withConf(hadoopConfig)
            .withAllocator(allocator)
            .withDecryption(fileDecryptionProperties)
            .withIOThreadPool(parquetIOThreadPool)
            .withProcessThreadPool(parquetProcessThreadPool)
            .build()) {
      for (Group group = reader.read(); group != null; group = reader.read()) {
        SingleRow rowExpected = data.get(rowNum++);

        // plaintext columns
        if (rowExpected.plaintext_int32_field != group.getInteger(SingleRow.PLAINTEXT_INT32_FIELD_NAME, 0)) {
          addErrorToErrorCollectorAndLog("Wrong int", encryptionConfiguration, decryptionConfiguration);
        }
        // encrypted columns
        if (decryptionConfiguration != DecryptionConfiguration.NO_DECRYPTION) {
          if (rowExpected.boolean_field != group.getBoolean(SingleRow.BOOLEAN_FIELD_NAME, 0)) {
            addErrorToErrorCollectorAndLog("Wrong bool", encryptionConfiguration, decryptionConfiguration);
          }
          if (rowExpected.int32_field != group.getInteger(SingleRow.INT32_FIELD_NAME, 0)) {
            addErrorToErrorCollectorAndLog("Wrong int", encryptionConfiguration, decryptionConfiguration);
          }
          if (rowExpected.float_field != group.getFloat(SingleRow.FLOAT_FIELD_NAME, 0)) {
            addErrorToErrorCollectorAndLog("Wrong float", encryptionConfiguration, decryptionConfiguration);
          }
          if (rowExpected.double_field != group.getDouble(SingleRow.DOUBLE_FIELD_NAME, 0)) {
            addErrorToErrorCollectorAndLog(
                "Wrong double", encryptionConfiguration, decryptionConfiguration);
          }
          if ((null != rowExpected.ba_field)
              && !Arrays.equals(
                  rowExpected.ba_field,
                  group.getBinary(SingleRow.BINARY_FIELD_NAME, 0)
                      .getBytes())) {
            addErrorToErrorCollectorAndLog(
                "Wrong byte array", encryptionConfiguration, decryptionConfiguration);
          }
          if (!Arrays.equals(
              rowExpected.flba_field,
              group.getBinary(SingleRow.FIXED_LENGTH_BINARY_FIELD_NAME, 0)
                  .getBytes())) {
            addErrorToErrorCollectorAndLog(
                "Wrong fixed-length byte array", encryptionConfiguration, decryptionConfiguration);
          }
        }
      }
    } catch (Exception e) {
      checkResult(file.getName(), decryptionConfiguration, e);
    } finally {
      parquetProcessThreadPool.shutdown();
      parquetIOThreadPool.shutdown();
    }
    hadoopConfig.unset("parquet.read.schema");
  }

  /**
   * Check that the decryption result is as expected.
   */
  private void checkResult(String file, DecryptionConfiguration decryptionConfiguration, Exception exception) {
    String errorMessage = exception.getMessage();
    String exceptionMsg = (null == errorMessage ? exception.getClass().getName() : errorMessage);
    // Extract encryptionConfigurationNumber from the parquet file name.
    EncryptionConfiguration encryptionConfiguration = getEncryptionConfigurationFromFilename(file);

    if (!plaintextFilesAllowed) {
      // Encryption_configuration null encryptor, so parquet is plaintext.
      // An exception is expected to be thrown if the file is being decrypted.
      if (encryptionConfiguration == EncryptionConfiguration.NO_ENCRYPTION) {
        if (decryptionConfiguration == DecryptionConfiguration.DECRYPT_WITH_KEY_RETRIEVER) {
          if (!exceptionMsg.endsWith("Applying decryptor on plaintext file")) {
            addErrorToErrorCollectorAndLog(
                "Expecting exception Applying decryptor on plaintext file",
                exceptionMsg,
                encryptionConfiguration,
                decryptionConfiguration);
          } else {
            LOG.info("Exception as expected: " + exceptionMsg);
          }
          return;
        }
      }
    }
    // Decryption configuration is null, so only plaintext file can be read. An exception is expected to
    // be thrown if the file is encrypted.
    if (decryptionConfiguration == DecryptionConfiguration.NO_DECRYPTION) {
      if ((encryptionConfiguration != EncryptionConfiguration.NO_ENCRYPTION
          && encryptionConfiguration != EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
        if (!exceptionMsg.endsWith("No encryption key list") && !exceptionMsg.endsWith("No keys available")) {
          addErrorToErrorCollectorAndLog(
              "Expecting  No keys available exception",
              exceptionMsg,
              encryptionConfiguration,
              decryptionConfiguration);
        } else {
          LOG.info("Exception as expected: " + exceptionMsg);
        }
        return;
      }
    }
    exception.printStackTrace();
    addErrorToErrorCollectorAndLog(
        "Didn't expect an exception", exceptionMsg, encryptionConfiguration, decryptionConfiguration);
  }

  private EncryptionConfiguration getEncryptionConfigurationFromFilename(String file) {
    if (!file.endsWith(".parquet.encrypted")) {
      return null;
    }
    String fileNamePrefix = file.replaceFirst("(.*)_[0-9]+.parquet.encrypted", "$1");
    ;
    try {
      return EncryptionConfiguration.valueOf(fileNamePrefix.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOG.error("File name doesn't match any known encryption configuration: " + file);
      synchronized (errorCollector) {
        errorCollector.addError(e);
      }
      return null;
    }
  }

  private void addErrorToErrorCollectorAndLog(
      String errorMessage,
      String exceptionMessage,
      EncryptionConfiguration encryptionConfiguration,
      DecryptionConfiguration decryptionConfiguration) {
    String fullErrorMessage = String.format(
        "%s - %s Error: %s, but got [%s]",
        encryptionConfiguration, decryptionConfiguration, errorMessage, exceptionMessage);

    synchronized (errorCollector) {
      errorCollector.addError(new Throwable(fullErrorMessage));
    }
    LOG.error(fullErrorMessage);
  }

  private void addErrorToErrorCollectorAndLog(
      String errorMessage,
      EncryptionConfiguration encryptionConfiguration,
      DecryptionConfiguration decryptionConfiguration) {
    String fullErrorMessage =
        String.format("%s - %s Error: %s", encryptionConfiguration, decryptionConfiguration, errorMessage);

    synchronized (errorCollector) {
      errorCollector.addError(new Throwable(fullErrorMessage));
    }
    LOG.error(fullErrorMessage);
  }

  private void addErrorToErrorCollectorAndLog(
      String errorMessage,
      Throwable exception,
      EncryptionConfiguration encryptionConfiguration,
      DecryptionConfiguration decryptionConfiguration) {
    String errorMessageWithExceptionDetails =
        String.format("%s %s %s", errorMessage, exception.getClass().getName(), exception.getMessage());
    addErrorToErrorCollectorAndLog(
        errorMessageWithExceptionDetails, encryptionConfiguration, decryptionConfiguration);
    exception.printStackTrace();
  }
}
