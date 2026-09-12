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

import org.apache.seatunnel.connectors.seatunnel.file.config.FilePostSyncAction;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

/**
 * Serializable post-sync operation record persisted in source checkpoint state.
 *
 * <p>The record keeps source file version (len/mtime) so delete/backup can be guarded against stale
 * operations after restore.
 */
@Getter
public class FileSourceOperationState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tableId;
    private final String splitId;
    private final String sourcePath;
    private final long sourceLength;
    private final long sourceModificationTime;
    private final FilePostSyncAction action;
    private final String backupTargetPath;
    private final String sourceContentFingerprint;
    private int retryCount;

    public FileSourceOperationState(
            String tableId,
            String splitId,
            String sourcePath,
            long sourceLength,
            long sourceModificationTime,
            FilePostSyncAction action,
            String backupTargetPath) {
        this(
                tableId,
                splitId,
                sourcePath,
                sourceLength,
                sourceModificationTime,
                action,
                backupTargetPath,
                null);
    }

    public FileSourceOperationState(
            String tableId,
            String splitId,
            String sourcePath,
            long sourceLength,
            long sourceModificationTime,
            FilePostSyncAction action,
            String backupTargetPath,
            String sourceContentFingerprint) {
        this.tableId = tableId;
        this.splitId = splitId;
        this.sourcePath = sourcePath;
        this.sourceLength = sourceLength;
        this.sourceModificationTime = sourceModificationTime;
        this.action = action;
        this.backupTargetPath = backupTargetPath;
        this.sourceContentFingerprint = sourceContentFingerprint;
        this.retryCount = 0;
    }

    public void increaseRetryCount() {
        retryCount++;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSourceOperationState that = (FileSourceOperationState) o;
        return sourceLength == that.sourceLength
                && sourceModificationTime == that.sourceModificationTime
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(splitId, that.splitId)
                && Objects.equals(sourcePath, that.sourcePath)
                && action == that.action
                && Objects.equals(backupTargetPath, that.backupTargetPath)
                && Objects.equals(sourceContentFingerprint, that.sourceContentFingerprint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableId,
                splitId,
                sourcePath,
                sourceLength,
                sourceModificationTime,
                action,
                backupTargetPath,
                sourceContentFingerprint);
    }
}
