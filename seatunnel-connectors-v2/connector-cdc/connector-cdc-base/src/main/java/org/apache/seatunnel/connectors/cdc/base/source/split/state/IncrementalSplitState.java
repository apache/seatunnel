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

package org.apache.seatunnel.connectors.cdc.base.source.split.state;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import io.debezium.relational.TableId;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * The state of split to describe the change log of table(s).
 */
@Getter
@Setter
public class IncrementalSplitState extends SourceSplitStateBase {

    private  List<TableId> tableIds;

    /**
     * Minimum watermark for SnapshotSplits for all tables in this IncrementalSplit
     */
    private Offset startupOffset;

    /**
     * Obtained by configuration, may not end
     */
    private  Offset stopOffset;

    public IncrementalSplitState(IncrementalSplit split) {
        super(split);
        this.tableIds = split.getTableIds();
        this.startupOffset = split.getStartupOffset();
        this.stopOffset = split.getStopOffset();
    }

    @Override
    public IncrementalSplit toSourceSplit() {
        final IncrementalSplit incrementalSplit = split.asIncrementalSplit();
        return new IncrementalSplit(
            incrementalSplit.splitId(),
            getTableIds(),
            getStartupOffset(),
            getStopOffset(),
            incrementalSplit.getCompletedSnapshotSplitInfos()
        );
    }
}
