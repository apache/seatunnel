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

package org.apache.seatunnel.translation.spark.execution;

import java.util.HashMap;
import java.util.Map;

public class CheckpointMetadata {

    public static final String CHECKPOINT = "checkpoint";
    public static final String CHECKPOINT_ID = "checkpointId";
    public static final String BATCH_ID = "batchId";
    public static final String LOCATION = "location";
    public static final String SUBTASK_ID = "subTaskId";

    public static Map<String, String> create(
            String location, int batchId, int subTaskId, int checkpointId) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(CHECKPOINT, String.valueOf(true));
        metadata.put(CHECKPOINT_ID, String.valueOf(checkpointId));
        metadata.put(BATCH_ID, String.valueOf(batchId));
        metadata.put(LOCATION, location);
        metadata.put(SUBTASK_ID, String.valueOf(subTaskId));
        return metadata;
    }

    private final boolean checkpoint;
    private final String location;
    private final int checkpointId;
    private final int batchId;
    private final int subTaskId;

    private CheckpointMetadata(
            boolean checkpoint, String location, int batchId, int subTaskId, int checkpointId) {
        this.checkpoint = checkpoint;
        this.location = location;
        this.batchId = batchId;
        this.checkpointId = checkpointId;
        this.subTaskId = subTaskId;
    }

    public static CheckpointMetadata of(Map<String, String> data) {
        boolean isCheckpoint = Boolean.parseBoolean(data.getOrDefault(CHECKPOINT, "false"));
        String location = data.getOrDefault(LOCATION, "");
        int batchId = Integer.parseInt(data.getOrDefault(BATCH_ID, "-1"));
        int subTaskId = Integer.parseInt(data.getOrDefault(SUBTASK_ID, "-1"));
        int checkpointId = Integer.parseInt(data.getOrDefault(CHECKPOINT_ID, "-1"));
        return new CheckpointMetadata(isCheckpoint, location, batchId, subTaskId, checkpointId);
    }

    public static CheckpointMetadata of(
            String location, int batchId, int subTaskId, int checkpointId) {
        return of(create(location, batchId, subTaskId, checkpointId));
    }

    public boolean isCheckpoint() {
        return this.checkpoint;
    }

    public String location() {
        return this.location;
    }

    public int checkpointId() {
        return this.checkpointId;
    }

    public int batchId() {
        return this.batchId;
    }

    public int subTaskId() {
        return this.subTaskId;
    }
}
