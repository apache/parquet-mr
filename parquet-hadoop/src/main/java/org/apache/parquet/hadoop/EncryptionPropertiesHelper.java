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
package org.apache.parquet.hadoop;

import java.net.URI;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

final class EncryptionPropertiesHelper {
  static FileEncryptionProperties createEncryptionProperties(
      ParquetConfiguration fileParquetConfig, Path tempFilePath, WriteSupport.WriteContext fileWriteContext) {
    EncryptionPropertiesFactory cryptoFactory = EncryptionPropertiesFactory.loadFactory(fileParquetConfig);
    if (null == cryptoFactory) {
      return null;
    }

    Configuration hadoopConf = ConfigurationUtil.createHadoopConfiguration(fileParquetConfig);
    URI path = tempFilePath == null ? null : tempFilePath.toUri();
    return cryptoFactory.getFileEncryptionProperties(
        hadoopConf, path == null ? null : new org.apache.hadoop.fs.Path(path), fileWriteContext);
  }

  static FileEncryptionProperties createEncryptionProperties(
      Configuration fileHadoopConfig,
      org.apache.hadoop.fs.Path tempFilePath,
      WriteSupport.WriteContext fileWriteContext) {
    EncryptionPropertiesFactory cryptoFactory = EncryptionPropertiesFactory.loadFactory(fileHadoopConfig);
    if (null == cryptoFactory) {
      return null;
    }
    return cryptoFactory.getFileEncryptionProperties(fileHadoopConfig, tempFilePath, fileWriteContext);
  }
}
