/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop.util;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class EncryptionTestFile {
  private final String fileName;
  private final SimpleGroup[] fileContent;

  public EncryptionTestFile(String fileName, SimpleGroup[] fileContent) {
    this.fileName = fileName;
    this.fileContent = fileContent;
  }

  public String getFileName() {
    return this.fileName;
  }

  public SimpleGroup[] getFileContent() {
    return this.fileContent;
  }
}
