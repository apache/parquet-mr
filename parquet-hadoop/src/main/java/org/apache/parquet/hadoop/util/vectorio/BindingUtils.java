/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util.vectorio;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.parquet.util.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binding utils.
 */
public final class BindingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BindingUtils.class);

  private BindingUtils() {}

  /**
   * Given a future, evaluate it.
   * <p>
   * Any exception generated in the future is
   * extracted and rethrown.
   * </p>
   *
   * @param future future to evaluate
   * @param timeout timeout to wait
   * @param unit time unit.
   * @param <T> type of the result.
   *
   * @return the result, if all went well.
   *
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   * @throws TimeoutException the future timed out.
   */
  public static <T> T awaitFuture(final Future<T> future, final long timeout, final TimeUnit unit)
      throws InterruptedIOException, IOException, RuntimeException, TimeoutException {
    try {
      LOG.debug("Awaiting future");
      return future.get(timeout, unit);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString()).initCause(e);
    } catch (ExecutionException e) {
      return raiseInnerCause(e);
    }
  }

  /**
   * From the inner cause of an execution exception, extract the inner cause
   * if it is an IOE or RTE.
   * This will always raise an exception, either the inner IOException,
   * an inner RuntimeException, or a new IOException wrapping the raised
   * exception.
   *
   * @param e exception.
   * @param <T> type of return value.
   *
   * @return nothing, ever.
   *
   * @throws IOException either the inner IOException, or a wrapper around
   * any non-Runtime-Exception
   * @throws RuntimeException if that is the inner cause.
   */
  public static <T> T raiseInnerCause(final ExecutionException e) throws IOException {
    throw unwrapInnerException(e);
  }

  /**
   * From the inner cause of an execution exception, extract the inner cause
   * to an IOException, raising RuntimeExceptions and Errors immediately.
   * <ol>
   *   <li> If it is an IOE: Return.</li>
   *   <li> If it is a {@link UncheckedIOException}: return the cause</li>
   *   <li> Completion/Execution Exceptions: extract and repeat</li>
   *   <li> If it is an RTE or Error: throw.</li>
   *   <li> Any other type: wrap in an IOE</li>
   * </ol>
   *
   * Recursively handles wrapped Execution and Completion Exceptions in
   * case something very complicated has happened.
   *
   * @param e exception.
   *
   * @return an IOException extracted or built from the cause.
   *
   * @throws RuntimeException if that is the inner cause.
   * @throws Error if that is the inner cause.
   */
  @SuppressWarnings("ChainOfInstanceofChecks")
  public static IOException unwrapInnerException(final Throwable e) {
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
      return (IOException) cause;
    } else if (cause instanceof UncheckedIOException) {
      // this is always an IOException
      return ((UncheckedIOException) cause).getCause();
    } else if (cause instanceof CompletionException || cause instanceof ExecutionException) {
      return unwrapInnerException(cause);
    } else if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else if (cause instanceof Error) {
      throw (Error) cause;
    } else if (cause != null) {
      // other type: wrap with a new IOE
      return new IOException(cause);
    } else {
      // this only happens if there was no cause.
      return new IOException(e);
    }
  }

  /**
   * Get an invocation from the source class, which will be unavailable() if
   * the class is null or the method isn't found.
   *
   * @param <T> return type
   * @param source source. If null, the method is a no-op.
   * @param returnType return type class (unused)
   * @param name method name
   * @param parameterTypes parameters
   *
   * @return the method or "unavailable"
   */
  static <T> DynMethods.UnboundMethod loadInvocation(
      Class<?> source, Class<? extends T> returnType, String name, Class<?>... parameterTypes) {

    if (source != null) {
      final DynMethods.UnboundMethod m = new DynMethods.Builder(name)
          .impl(source, name, parameterTypes)
          .orNoop()
          .build();
      if (m.isNoop()) {
        // this is a sign of a mismatch between this class's expected
        // signatures and actual ones.
        // log at debug.
        LOG.debug("Failed to load method {} from {}", name, source);
      } else {
        LOG.debug("Found method {} from {}", name, source);
      }
      return m;
    } else {
      return noop(name);
    }
  }

  /**
   * Create a no-op method.
   *
   * @param name method name
   *
   * @return a no-op method.
   */
  static DynMethods.UnboundMethod noop(final String name) {
    return new DynMethods.Builder(name).orNoop().build();
  }

  /**
   * Given a sequence of methods, verify that they are all available.
   *
   * @param methods methods
   *
   * @return true if they are all implemented
   */
  static boolean implemented(DynMethods.UnboundMethod... methods) {
    for (DynMethods.UnboundMethod method : methods) {
      if (method.isNoop()) {
        return false;
      }
    }
    return true;
  }
}
