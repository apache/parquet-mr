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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetOutputCommitter extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetOutputCommitter.class);

  private final Path outputPath;

  public ParquetOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
  }

  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
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
        // Delete output dir in case write empty records.
        if (configuration.getBoolean(ParquetOutputFormat.SKIP_EMPTY_FILE, true)) {
          fileSystem.delete(outputPath, true);
        }
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
}
