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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * decorator class for PathOutputCommitter, for adding optional parquet metadata file,
 * overriding all methods to delegate to an instance of <code>PathOutputCommitterFactory.getCommitterFactory(..)</code>
 * <p>
 * For internal reason (hard-coded 'instanceof FileOutputCommitter' in spark source code),
 * this class still temporarily <code>extends FileOutputCommitter</code> instead of <code>PathOutputCommitter</code>
 */
public class ParquetOutputCommitter extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetOutputCommitter.class);

  private final Path outputPath;

  private final FileOutputCommitter delegate;

  public ParquetOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
    this.delegate = (FileOutputCommitter)
        PathOutputCommitterFactory.getCommitterFactory(outputPath, context.getConfiguration())
            .createOutputCommitter(outputPath, context);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    delegate.commitJob(jobContext);
    Configuration configuration = ContextUtil.getConfiguration(jobContext);
    writeMetaDataFile(configuration, outputPath);
  }

  // TODO: This method should propagate errors, and we should clean up
  // TODO: all the catching of Exceptions below -- see PARQUET-383
  public static void writeMetaDataFile(Configuration configuration, Path outputPath) {
    JobSummaryLevel level = ParquetOutputFormat.getJobSummaryLevel(configuration);
    if (level == JobSummaryLevel.NONE) {
      return;
    }

    try {
      final FileSystem fileSystem = outputPath.getFileSystem(configuration);
      FileStatus outputStatus = fileSystem.getFileStatus(outputPath);
      List<Footer> footers;

      switch (level) {
        case ALL:
          footers = ParquetFileReader.readAllFootersInParallel(
              configuration, outputStatus, false); // don't skip row groups
          break;
        case COMMON_ONLY:
          footers = ParquetFileReader.readAllFootersInParallel(
              configuration, outputStatus, true); // skip row groups
          break;
        default:
          throw new IllegalArgumentException("Unrecognized job summary level: " + level);
      }

      // If there are no footers, _metadata file cannot be written since there is no way to determine schema!
      // Onus of writing any summary files lies with the caller in this case.
      if (footers.isEmpty()) {
        return;
      }

      try {
        ParquetFileWriter.writeMetadataFile(configuration, outputPath, footers, level);
      } catch (Exception e) {
        LOG.warn("could not write summary file(s) for " + outputPath, e);

        final Path metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE);

        try {
          if (fileSystem.exists(metadataPath)) {
            fileSystem.delete(metadataPath, true);
          }
        } catch (Exception e2) {
          LOG.warn("could not delete metadata file" + outputPath, e2);
        }

        try {
          final Path commonMetadataPath =
              new Path(outputPath, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE);
          if (fileSystem.exists(commonMetadataPath)) {
            fileSystem.delete(commonMetadataPath, true);
          }
        } catch (Exception e2) {
          LOG.warn("could not delete metadata file" + outputPath, e2);
        }
      }
    } catch (Exception e) {
      LOG.warn("could not write summary file for " + outputPath, e);
    }
  }

  // override class PathOutputCommitter, delegate to field <code>delegate</code>
  // ---------------------------------------------------------------------------------------------

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

  // override class FileOutputCommitter, delegate to field <code>delegate</code>
  // ---------------------------------------------------------------------------------------------

  @Override
  public Path getOutputPath() {
    return outputPath; // idem: delegate.getOutputPath()
  }

  @Override
  public Path getJobAttemptPath(JobContext context) {
    return delegate.getJobAttemptPath(context);
  }

  @Override
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return delegate.getTaskAttemptPath(context);
  }

  @Override
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return delegate.getCommittedTaskPath(context);
  }

  @Override
  public Path getWorkPath() throws IOException {
    return delegate.getWorkPath();
  }

  @Override
  @InterfaceAudience.Private
  public void commitTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    delegate.commitTask(context, taskAttemptPath);
  }

  @Override
  @InterfaceAudience.Private
  public void abortTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    delegate.abortTask(context, taskAttemptPath);
  }

  @Override
  @InterfaceAudience.Private
  public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    return delegate.needsTaskCommit(context, taskAttemptPath);
  }
}
