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
package org.apache.parquet;

import java.util.Objects;

/**
 * Utility for parameter validation
 */
public final class Preconditions {
  private Preconditions() { }

  /**
   * @param o the param to check
   * @param name the name of the param for the error message
   * @param <T> the type of the object
   * @return the validated o
   * @throws NullPointerException if o is null
   * @deprecated Use JDK {@link Objects#requireNonNull(Object, String)}
   */
  @Deprecated
  public static <T> T checkNotNull(T o, String name) throws NullPointerException {
    if (o == null) {
      throw new NullPointerException(name + " should not be null");
    }
    return o;
  }

  /**
   * Precondition-style validation that throws {@link IllegalArgumentException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @throws IllegalArgumentException if {@code isValid} is false
   */
  public static void checkArgument(boolean isValid, String message) throws IllegalArgumentException {
    if (!isValid) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalArgumentException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param arg0
   *          First parameter of the message string template
   * @throws IllegalArgumentException if {@code isValid} is false
   */
  public static void checkArgument(boolean isValid, String message, Object arg0) throws IllegalArgumentException {
    if (!isValid) {
      throw new IllegalArgumentException(
          String.format(String.valueOf(message), strings(arg0)));
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalArgumentException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param arg0
   *          First parameter of the message string template
   * @param arg1
   *          Second parameter of the message string template
   * @throws IllegalArgumentException if {@code isValid} is false
   */
  public static void checkArgument(boolean isValid, String message, Object arg0, Object arg1) throws IllegalArgumentException {
    if (!isValid) {
      throw new IllegalArgumentException(
          String.format(String.valueOf(message), strings(arg0, arg1)));
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalArgumentException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param arg0
   *          First parameter of the message string template
   * @param arg1
   *          Second parameter of the message string template
   * @param arg2
   *          Third parameter of the message string template
   * @throws IllegalArgumentException if {@code isValid} is false
   */
  public static void checkArgument(boolean isValid, String message, Object arg0, Object arg1, Object arg2) throws IllegalArgumentException {
    if (!isValid) {
      throw new IllegalArgumentException(
          String.format(String.valueOf(message), strings(arg0, arg1, arg2)));
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalArgumentException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param args
   *          Objects used to fill in {@code %s} placeholders in the message
   * @throws IllegalArgumentException if {@code isValid} is false
   */
  public static void checkArgument(boolean isValid, String message, Object... args)
      throws IllegalArgumentException {
    if (!isValid) {
      throw new IllegalArgumentException(
          String.format(String.valueOf(message), strings(args)));
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalStateException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @throws IllegalStateException if {@code isValid} is false
   */
  public static void checkState(boolean isValid, String message) throws IllegalStateException {
    if (!isValid) {
      throw new IllegalStateException(message);
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalStateException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param arg0
   *          First parameter of the message string template
   * @throws IllegalStateException if {@code isValid} is false
   */
  public static void checkState(boolean isValid, String message, Object arg0) throws IllegalStateException {
    if (!isValid) {
      throw new IllegalStateException(
          String.format(String.valueOf(message), strings(arg0)));
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalStateException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param arg0
   *          First parameter of the message string template
   * @param arg1
   *          Second parameter of the message string template
   * @throws IllegalStateException if {@code isValid} is false
   */
  public static void checkState(boolean isValid, String message, Object arg0, Object arg1) throws IllegalStateException {
    if (!isValid) {
      throw new IllegalStateException(
          String.format(String.valueOf(message), strings(arg0, arg1)));
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalStateException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param arg0
   *          First parameter of the message string template
   * @param arg1
   *          Second parameter of the message string template
   * @param arg2
   *          Third parameter of the message string template
   * @throws IllegalStateException if {@code isValid} is false
   */
  public static void checkState(boolean isValid, String message, Object arg0, Object arg1, Object arg2) throws IllegalStateException {
    if (!isValid) {
      throw new IllegalStateException(
          String.format(String.valueOf(message), strings(arg0, arg1, arg2)));
    }
  }

  /**
   * Precondition-style validation that throws {@link IllegalStateException}.
   *
   * @param isValid
   *          {@code true} if valid, {@code false} if an exception should be
   *          thrown
   * @param message
   *          A String message for the exception.
   * @param args
   *          Objects used to fill in {@code %s} placeholders in the message
   * @throws IllegalStateException if {@code isValid} is false
   */
  public static void checkState(boolean isValid, String message, Object... args)
      throws IllegalStateException {
    if (!isValid) {
      throw new IllegalStateException(
          String.format(String.valueOf(message), strings(args)));
    }
  }

  private static String[] strings(Object... objects) {
    String[] strings = new String[objects.length];
    for (int i = 0; i < objects.length; i += 1) {
      strings[i] = String.valueOf(objects[i]);
    }
    return strings;
  }

}
