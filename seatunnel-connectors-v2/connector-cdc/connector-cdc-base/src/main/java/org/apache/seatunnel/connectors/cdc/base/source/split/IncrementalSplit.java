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

package org.apache.seatunnel.connectors.cdc.base.source.split;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.relational.TableId;
import lombok.Getter;

import java.util.List;

@Getter
public class IncrementalSplit extends SourceSplitBase {

    /**
     * All the tables that this incremental split needs to capture.
     */
    private final List<TableId> tableIds;

    /**
     * Minimum watermark for SnapshotSplits for all tables in this IncrementalSplit
     */
    private final Offset startupOffset;

    /**
     * Obtained by configuration, may not end
     */
    private final Offset stopOffset;

    /**
     * SnapshotSplit information for all tables in this IncrementalSplit.
     * <br> Used to support Exactly-Once.
     */
    private final List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos;

    public IncrementalSplit(
            String splitId,
            List<TableId> capturedTables,
            Offset startupOffset,
            Offset stopOffset,
            List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos) {
        super(splitId);
        this.tableIds = capturedTables;
        this.startupOffset = startupOffset;
        this.stopOffset = stopOffset;
        this.completedSnapshotSplitInfos = completedSnapshotSplitInfos;
    }
}
