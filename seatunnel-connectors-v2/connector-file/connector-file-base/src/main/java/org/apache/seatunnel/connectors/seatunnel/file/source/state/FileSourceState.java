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

package org.apache.seatunnel.connectors.seatunnel.file.source.state;

import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileSourceState implements Serializable {
    private static final long serialVersionUID = 9208369906513934611L;
    private Set<FileSourceSplit> assignedSplit;
    private long discoveryStartTimeMillis;
    private Map<Long, List<FileSourceOperationState>> pendingOpsByCheckpoint;
    private Map<String, Long> retentionLastRunMillisByPath;
    private Map<String, Long> processedFileOffsets;
    private Map<String, FileTailState> fileTailStates;
    private boolean textTailingInitialScanComplete;

    public FileSourceState(Set<FileSourceSplit> assignedSplit) {
        this(
                assignedSplit,
                0L,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public FileSourceState(Set<FileSourceSplit> assignedSplit, long discoveryStartTimeMillis) {
        this(
                assignedSplit,
                discoveryStartTimeMillis,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public FileSourceState(
            Set<FileSourceSplit> assignedSplit,
            long discoveryStartTimeMillis,
            Map<Long, List<FileSourceOperationState>> pendingOpsByCheckpoint,
            Map<String, Long> retentionLastRunMillisByPath) {
        this(
                assignedSplit,
                discoveryStartTimeMillis,
                pendingOpsByCheckpoint,
                retentionLastRunMillisByPath,
                Collections.emptyMap(),
                Collections.emptyMap(),
                false);
    }

    public FileSourceState(
            Set<FileSourceSplit> assignedSplit,
            long discoveryStartTimeMillis,
            Map<Long, List<FileSourceOperationState>> pendingOpsByCheckpoint,
            Map<String, Long> retentionLastRunMillisByPath,
            Map<String, Long> processedFileOffsets) {
        this(
                assignedSplit,
                discoveryStartTimeMillis,
                pendingOpsByCheckpoint,
                retentionLastRunMillisByPath,
                processedFileOffsets,
                Collections.emptyMap(),
                processedFileOffsets != null && !processedFileOffsets.isEmpty());
    }

    public FileSourceState(
            Set<FileSourceSplit> assignedSplit,
            long discoveryStartTimeMillis,
            Map<Long, List<FileSourceOperationState>> pendingOpsByCheckpoint,
            Map<String, Long> retentionLastRunMillisByPath,
            Map<String, Long> processedFileOffsets,
            Map<String, FileTailState> fileTailStates,
            boolean textTailingInitialScanComplete) {
        this.assignedSplit = assignedSplit;
        this.discoveryStartTimeMillis = discoveryStartTimeMillis;
        this.pendingOpsByCheckpoint = pendingOpsByCheckpoint;
        this.retentionLastRunMillisByPath = retentionLastRunMillisByPath;
        this.processedFileOffsets = processedFileOffsets;
        this.fileTailStates = fileTailStates;
        this.textTailingInitialScanComplete = textTailingInitialScanComplete;
    }

    public Set<FileSourceSplit> getAssignedSplit() {
        return assignedSplit;
    }

    public long getDiscoveryStartTimeMillis() {
        return discoveryStartTimeMillis;
    }

    public Map<Long, List<FileSourceOperationState>> getPendingOpsByCheckpoint() {
        return pendingOpsByCheckpoint;
    }

    public Map<String, Long> getRetentionLastRunMillisByPath() {
        return retentionLastRunMillisByPath;
    }

    public Map<String, Long> getProcessedFileOffsets() {
        return processedFileOffsets;
    }

    public Map<String, FileTailState> getFileTailStates() {
        return fileTailStates;
    }

    public boolean isTextTailingInitialScanComplete() {
        return textTailingInitialScanComplete;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (assignedSplit == null) {
            assignedSplit = new HashSet<>();
        }
        if (pendingOpsByCheckpoint == null) {
            pendingOpsByCheckpoint = new HashMap<>();
        }
        if (retentionLastRunMillisByPath == null) {
            retentionLastRunMillisByPath = new HashMap<>();
        }
        if (processedFileOffsets == null) {
            processedFileOffsets = new HashMap<>();
        }
        if (fileTailStates == null) {
            fileTailStates = new HashMap<>();
        }
        if (!processedFileOffsets.isEmpty()) {
            textTailingInitialScanComplete = true;
        }
    }
}
