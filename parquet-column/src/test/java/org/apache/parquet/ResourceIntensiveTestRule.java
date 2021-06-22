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

import static org.junit.Assume.assumeTrue;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A test rule for resource intensive tests that are not executed on every environment (e.g. CI). It is managed by the
 * system property {@code enableResourceIntensiveTests}. If the value is {@code true} the related tests are executed,
 * skipped otherwise.
 */
public class ResourceIntensiveTestRule implements TestRule {

  private static final TestRule INSTANCE = new ResourceIntensiveTestRule(
      Boolean.getBoolean("enableResourceIntensiveTests"));

  public static TestRule get() {
    return INSTANCE;
  }

  private final boolean enabled;

  private ResourceIntensiveTestRule(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return enabled ? base : new Statement() {
      @Override
      public void evaluate() {
        assumeTrue("Resource intensive test is not executed due to the limitations of the environment", enabled);
      }
    };
  }

}
