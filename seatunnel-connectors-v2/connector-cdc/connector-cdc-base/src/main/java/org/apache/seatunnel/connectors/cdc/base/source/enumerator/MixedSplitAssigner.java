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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.MixedPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;

import io.debezium.relational.TableId;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * Hybrid assigner for a source where only selected tables are snapshotted before incremental
 * reading starts.
 *
 * <p>The final incremental split captures every table. Tables without a snapshot keep their own
 * configured lower bound, while snapshot tables reuse the normal high-watermark filtering.
 */
public class MixedSplitAssigner<C extends SourceConfig> extends HybridSplitAssigner<C> {

    private final Set<TableId> snapshotTables;
    private final Map<TableId, Offset> tableStartOffsets;

    public MixedSplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            int incrementalParallelism,
            Set<TableId> snapshotTables,
            Map<TableId, Offset> tableStartOffsets,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        super(
                new SnapshotSplitAssigner<>(
                        context,
                        currentParallelism,
                        new ArrayList<>(snapshotTables),
                        isTableIdCaseSensitive,
                        dialect),
                new IncrementalSplitAssigner<>(
                        context, incrementalParallelism, offsetFactory, tableStartOffsets));
        this.snapshotTables = snapshotTables;
        this.tableStartOffsets = tableStartOffsets;
    }

    public MixedSplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            int incrementalParallelism,
            MixedPendingSplitsState checkpoint,
            DataSourceDialect<C> dialect,
            OffsetFactory offsetFactory) {
        super(
                new SnapshotSplitAssigner<>(
                        context, currentParallelism, checkpoint.getSnapshotPhaseState(), dialect),
                new IncrementalSplitAssigner<>(
                        context,
                        incrementalParallelism,
                        offsetFactory,
                        checkpoint.getIncrementalPhaseState(),
                        checkpoint.getTableStartOffsets()));
        this.snapshotTables = checkpoint.getSnapshotTables();
        this.tableStartOffsets = checkpoint.getTableStartOffsets();
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new MixedPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId),
                incrementalSplitAssigner.snapshotState(checkpointId),
                snapshotTables,
                tableStartOffsets);
    }
}
