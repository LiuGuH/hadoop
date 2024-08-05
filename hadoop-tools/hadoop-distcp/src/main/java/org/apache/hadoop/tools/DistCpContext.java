/**
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

package org.apache.hadoop.tools;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the context of the distcp at runtime.
 *
 * It has the immutable {@link DistCpOptions} and mutable runtime status.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DistCpContext {
  static final Logger LOG = LoggerFactory.getLogger(DistCpContext.class);
  private final DistCpOptions options;

  /** The source paths can be set at runtime via snapshots. */
  private List<Path> sourcePaths;

  /** This is a derived field, it's initialized in the beginning of distcp. */
  private boolean targetPathExists = true;

  /** Indicate that raw.* xattrs should be preserved if true. */
  private boolean preserveRawXattrs = false;

  public DistCpContext(DistCpOptions options) throws IOException {
    this.options = options;
    this.sourcePaths = options.getSourcePaths();
    checkSourceAndTargetPath();
  }

  public void setSourcePaths(List<Path> sourcePaths) {
    this.sourcePaths = sourcePaths;
  }

  /**
   * @return the sourcePaths. Please note this method does not directly delegate
   * to the {@link #options}.
   */
  public List<Path> getSourcePaths() {
    return sourcePaths;
  }

  public Path getSourceFileListing() {
    return options.getSourceFileListing();
  }

  public Path getTargetPath() {
    return options.getTargetPath();
  }

  public boolean shouldAtomicCommit() {
    return options.shouldAtomicCommit();
  }

  public boolean shouldSyncFolder() {
    return options.shouldSyncFolder();
  }

  public boolean shouldDeleteMissing() {
    return options.shouldDeleteMissing();
  }

  public boolean shouldIgnoreFailures() {
    return options.shouldIgnoreFailures();
  }

  public boolean shouldOverwrite() {
    return options.shouldOverwrite();
  }

  public boolean shouldAppend() {
    return options.shouldAppend();
  }

  public boolean shouldSkipCRC() {
    return options.shouldSkipCRC();
  }

  public boolean shouldUseFastCopy() {
    return options.shouldUseFastCopy();
  }

  public boolean shouldBlock() {
    return options.shouldBlock();
  }

  public boolean shouldUseDiff() {
    return options.shouldUseDiff();
  }

  public boolean shouldUseRdiff() {
    return options.shouldUseRdiff();
  }

  public boolean shouldUseSnapshotDiff() {
    return options.shouldUseSnapshotDiff();
  }

  public String getFromSnapshot() {
    return options.getFromSnapshot();
  }

  public String getToSnapshot() {
    return options.getToSnapshot();
  }

  public final String getFiltersFile() {
    return options.getFiltersFile();
  }

  public int getNumListstatusThreads() {
    return options.getNumListstatusThreads();
  }

  public int getMaxMaps() {
    return options.getMaxMaps();
  }

  public float getMapBandwidth() {
    return options.getMapBandwidth();
  }

  public Set<FileAttribute> getPreserveAttributes() {
    return options.getPreserveAttributes();
  }

  public boolean shouldPreserve(FileAttribute attribute) {
    return options.shouldPreserve(attribute);
  }

  public boolean shouldPreserveRawXattrs() {
    return preserveRawXattrs;
  }

  public void setPreserveRawXattrs(boolean preserveRawXattrs) {
    this.preserveRawXattrs = preserveRawXattrs;
  }

  public Path getAtomicWorkPath() {
    return options.getAtomicWorkPath();
  }

  public Path getLogPath() {
    return options.getLogPath();
  }

  public String getCopyStrategy() {
    return options.getCopyStrategy();
  }

  public int getBlocksPerChunk() {
    return options.getBlocksPerChunk();
  }

  public boolean shouldUseIterator() {
    return options.shouldUseIterator();
  }

  public final boolean splitLargeFile() {
    return options.getBlocksPerChunk() > 0;
  }

  public int getCopyBufferSize() {
    return options.getCopyBufferSize();
  }

  public boolean shouldDirectWrite() {
    return options.shouldDirectWrite();
  }

  public void setTargetPathExists(boolean targetPathExists) {
    this.targetPathExists = targetPathExists;
  }

  public boolean isTargetPathExists() {
    return targetPathExists;
  }

  public void appendToConf(Configuration conf) {
    options.appendToConf(conf);
  }

  public void checkSourceAndTargetPath() throws IOException {
    boolean sourceEC = true;
    boolean isFirst = true;
    for (Path path : sourcePaths) {
      FileSystem srcFileSystem = path.getFileSystem(new Configuration());
      if (srcFileSystem instanceof DistributedFileSystem) {
        ErasureCodingPolicy erasureCodingPolicy =
            ((DistributedFileSystem) srcFileSystem).getErasureCodingPolicy(path);
        if (isFirst) {
          isFirst = false;
          sourceEC = erasureCodingPolicy != null;
        } else {
          boolean flag = erasureCodingPolicy != null;
          if (sourceEC != flag) {
            options.setUseFastCopy(false);
            LOG.info(
                "SourcePaths have different storage strategy, both erasureCoding and replication exist. FastCopy will be ignored.");
            return;
          }
        }
      } else {
        options.setUseFastCopy(false);
        LOG.info("{} is not HDFS path. FastCopy will be ignored.", path);
        return;
      }
    }

    boolean targetEC = true;
    Path targetPath = getTargetPath();
    FileSystem targetFilesystem = targetPath.getFileSystem(new Configuration());
    while (targetPath != null) {
      if (!targetFilesystem.exists(targetPath)) {
        targetPath = targetPath.getParent();
      } else {
        break;
      }
    }

    if (targetFilesystem instanceof DistributedFileSystem) {
      ErasureCodingPolicy erasureCodingPolicy =
          ((DistributedFileSystem) targetFilesystem).getErasureCodingPolicy(targetPath);
      targetEC = erasureCodingPolicy != null;
    } else {
      options.setUseFastCopy(false);
      LOG.info("{} is not HDFS path. FastCopy will be ignored.", targetPath);
      return;
    }

    if (sourceEC != targetEC) {
      options.setUseFastCopy(false);
      LOG.info(
          "SourcePaths and targetPath has different storage strategy, both erasureCoding and replication exist. FastCopy will be ignored.");
    }
  }

  @Override
  public String toString() {
    return options.toString() +
        ", sourcePaths=" + sourcePaths +
        ", targetPathExists=" + targetPathExists +
        ", preserveRawXattrs=" + preserveRawXattrs;
  }

}
