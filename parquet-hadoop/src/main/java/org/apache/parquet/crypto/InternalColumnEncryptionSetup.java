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

import java.util.Arrays;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.EncryptionWithFooterKey;

public class InternalColumnEncryptionSetup {

  private final ColumnEncryptionProperties encryptionProperties;
  private final BlockCipher.Encryptor metadataEncryptor;
  private final BlockCipher.Encryptor dataEncryptor;
  private final ColumnCryptoMetaData columnCryptoMetaData;
  private final int ordinal;

  InternalColumnEncryptionSetup(
      ColumnEncryptionProperties encryptionProperties,
      int ordinal,
      BlockCipher.Encryptor dataEncryptor,
      BlockCipher.Encryptor metaDataEncryptor) {
    this.encryptionProperties = encryptionProperties;
    this.dataEncryptor = dataEncryptor;
    this.metadataEncryptor = metaDataEncryptor;
    this.ordinal = ordinal;

    if (encryptionProperties.isEncrypted()) {
      if (encryptionProperties.isEncryptedWithFooterKey()) {
        columnCryptoMetaData = ColumnCryptoMetaData.ENCRYPTION_WITH_FOOTER_KEY(new EncryptionWithFooterKey());
      } else {
        EncryptionWithColumnKey withColumnKeyStruct = new EncryptionWithColumnKey(
            encryptionProperties.getPath().toList());
        if (null != encryptionProperties.getKeyMetaData()) {
          withColumnKeyStruct.setKey_metadata(encryptionProperties.getKeyMetaData());
        }
        columnCryptoMetaData = ColumnCryptoMetaData.ENCRYPTION_WITH_COLUMN_KEY(withColumnKeyStruct);
      }
    } else {
      columnCryptoMetaData = null;
    }
  }

  public boolean isEncrypted() {
    return encryptionProperties.isEncrypted();
  }

  public BlockCipher.Encryptor getMetaDataEncryptor() {
    return metadataEncryptor;
  }

  public BlockCipher.Encryptor getDataEncryptor() {
    return dataEncryptor;
  }

  public ColumnCryptoMetaData getColumnCryptoMetaData() {
    return columnCryptoMetaData;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public boolean isEncryptedWithFooterKey() {
    return encryptionProperties.isEncryptedWithFooterKey();
  }
}
