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

package org.apache.parquet.cli.util;

/**
 * Exception to signal that a record could not be read or written.
 */
public class RecordException extends RuntimeException {
  public RecordException(String message) {
    super(message);
  }

  public RecordException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Precondition-style validation that throws a {@link RecordException}.
   *
   * @param isValid {@code true} if valid, {@code false} if an exception should be
   *                thrown
   * @param message A String message for the exception.
   * @param args    Args to fill into the message using String.format
   */
  public static void check(boolean isValid, String message, Object... args) {
    if (!isValid) {
      String[] argStrings = new String[args.length];
      for (int i = 0; i < args.length; i += 1) {
        argStrings[i] = String.valueOf(args[i]);
      }
      throw new RecordException(String.format(String.valueOf(message), (Object[]) argStrings));
    }
  }
}
