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
package org.apache.parquet.hadoop.util;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

/**
 * Proxy pattern for class <code>PathOutputCommitter</code>
 */
public class DelegatePathOutputCommitter extends PathOutputCommitter {

  protected final PathOutputCommitter delegate;

  public DelegatePathOutputCommitter(Path outputPath, TaskAttemptContext context, PathOutputCommitter delegate)
      throws IOException {
    super(outputPath, context);
    this.delegate = delegate;
  }

  public DelegatePathOutputCommitter(Path outputPath, JobContext context, PathOutputCommitter delegate)
      throws IOException {
    super(outputPath, context);
    this.delegate = delegate;
  }

  @Override
  public Path getOutputPath() {
    return delegate.getOutputPath();
  }

  @Override
  public boolean hasOutputPath() {
    return delegate.hasOutputPath();
  }

  @Override
  public Path getWorkPath() throws IOException {
    return delegate.getWorkPath();
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    delegate.setupJob(jobContext);
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException {
    delegate.cleanupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    delegate.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    delegate.abortJob(jobContext, state);
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    delegate.setupTask(taskAttemptContext);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return delegate.needsTaskCommit(taskAttemptContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    delegate.commitTask(taskAttemptContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    delegate.abortTask(taskAttemptContext);
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return delegate.isRecoverySupported();
  }

  @Override
  public boolean isCommitJobRepeatable(JobContext jobContext) throws IOException {
    return delegate.isCommitJobRepeatable(jobContext);
  }

  @Override
  public boolean isRecoverySupported(JobContext jobContext) throws IOException {
    return delegate.isRecoverySupported(jobContext);
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    delegate.recoverTask(taskContext);
  }
}
