/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.translation.spark.source.partition.micro;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager;

import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CheckpointDataLogManager {
    public static final String SUFFIX_PREPARED = ".prepared";
    public static final String SUFFIX_COMMITTED = ".committed";
    private final CheckpointFileManager checkpointFileManager;

    public CheckpointDataLogManager(CheckpointFileManager checkpointFileManager) {
        this.checkpointFileManager = checkpointFileManager;
    }

    public CheckpointFileManager getCheckpointFileManager() {
        return this.checkpointFileManager;
    }

    public Path logDataPath(String root, int batchId) {
        return new Path(root, String.valueOf(batchId));
    }

    public Path logDataPath(Path root, int batchId) {
        return new Path(root, String.valueOf(batchId));
    }

    public Path subTaskLogDataPath(String root, int batchId, int subTaskId) {
        return new Path(logDataPath(root, batchId), String.valueOf(subTaskId));
    }

    public Path subTaskLogDataPath(Path root, int batchId, int subTaskId) {
        return new Path(logDataPath(root, batchId), String.valueOf(subTaskId));
    }

    public Path logFilePath(String root, int batchId, int subTaskId, int checkpointId) {
        return new Path(subTaskLogDataPath(root, batchId, subTaskId), String.valueOf(checkpointId));
    }

    public Path logFilePath(Path root, int batchId, int subTaskId, int checkpointId) {
        return new Path(subTaskLogDataPath(root, batchId, subTaskId), String.valueOf(checkpointId));
    }

    public Path preparedCommittedFilePath(Path root, int batchId, int subTaskId, int checkpointId) {
        return new Path(
                subTaskLogDataPath(root, batchId, subTaskId),
                String.valueOf(checkpointId) + SUFFIX_PREPARED);
    }

    public Path committedFilePath(Path root, int batchId, int subTaskId, int checkpointId) {
        return new Path(
                subTaskLogDataPath(root, batchId, subTaskId),
                String.valueOf(checkpointId) + SUFFIX_COMMITTED);
    }

    public Path preparedCommittedFilePath(
            String root, int batchId, int subTaskId, int checkpointId) {
        return new Path(
                subTaskLogDataPath(root, batchId, subTaskId),
                String.valueOf(checkpointId) + SUFFIX_PREPARED);
    }

    public Path committedFilePath(String root, int batchId, int subTaskId, int checkpointId) {
        return new Path(
                subTaskLogDataPath(root, batchId, subTaskId),
                String.valueOf(checkpointId) + SUFFIX_COMMITTED);
    }

    public int maxNum(Path path, Optional<String> suffix) {
        Pattern pattern = Pattern.compile("^\\d+$");
        if (checkpointFileManager.exists(path)) {
            FileStatus[] fileStatuses = checkpointFileManager.list(path);
            Stream<String> nameStream =
                    Arrays.stream(fileStatuses)
                            .map((FileStatus fileStatus) -> fileStatus.getPath().getName().trim());
            if (suffix.isPresent()) {
                String suffixVal = suffix.get();
                nameStream =
                        nameStream
                                .filter((String name) -> name.endsWith(suffixVal))
                                .map((String name) -> name.split(suffixVal)[0]);
            }

            return nameStream.map(Integer::parseInt).max(Integer::compare).orElse(-1);
        }
        return -1;
    }
}
