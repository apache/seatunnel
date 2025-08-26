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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.Getter;

import java.util.Objects;

public class FileSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    @Getter private final String tableId;
    @Getter private final String filePath;
    @Getter private final long startOffset;
    @Getter private final long length;
    @Getter private final boolean isFirstSplit;

    public FileSourceSplit(String splitId) {
        this.filePath = splitId;
        this.tableId = null;
        this.startOffset = 0L;
        this.length = -1L;
        this.isFirstSplit = true;
    }

    public FileSourceSplit(String tableId, String filePath) {
        this.tableId = tableId;
        this.filePath = filePath;
        this.startOffset = 0L;
        this.length = -1L;
        this.isFirstSplit = true;
    }

    /**
     * Constructor for file split with range
     *
     * @param tableId the table identifier
     * @param filePath the file path
     * @param startOffset the start byte offset in the file
     * @param length the length of the split in bytes (-1 means read to end)
     * @param isFirstSplit whether this is the first split of the file (affects header handling)
     */
    public FileSourceSplit(
            String tableId, String filePath, long startOffset, long length, boolean isFirstSplit) {
        this.tableId = tableId;
        this.filePath = filePath;
        this.startOffset = startOffset;
        this.length = length;
        this.isFirstSplit = isFirstSplit;
    }

    /** Check if this split represents a complete file (not a range) */
    public boolean isCompleteFile() {
        return startOffset == 0L && length == -1L;
    }

    @Override
    public String splitId() {
        // In order to be compatible with the split before the upgrade, when tableId is null,
        // filePath is directly returned
        if (tableId == null) {
            if (isCompleteFile()) {
                return filePath;
            } else {
                return filePath + "_" + startOffset + "_" + length;
            }
        }
        if (isCompleteFile()) {
            return tableId + "_" + filePath;
        } else {
            return tableId + "_" + filePath + "_" + startOffset + "_" + length;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSourceSplit that = (FileSourceSplit) o;
        return startOffset == that.startOffset
                && length == that.length
                && isFirstSplit == that.isFirstSplit
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, filePath, startOffset, length, isFirstSplit);
    }
}
