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
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import io.debezium.relational.TableId;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Assigner for bounded snapshot-only CDC jobs. */
public class SnapshotOnlySplitAssigner<C extends SourceConfig> implements SplitAssigner {

    private final SnapshotSplitAssigner<C> snapshotSplitAssigner;

    public SnapshotOnlySplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            DataSourceDialect<C> dialect) {
        this.snapshotSplitAssigner =
                new SnapshotSplitAssigner<>(
                        context,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        dialect);
    }

    public SnapshotOnlySplitAssigner(
            SplitAssigner.Context<C> context,
            int currentParallelism,
            SnapshotPhaseState checkpoint,
            DataSourceDialect<C> dialect) {
        this.snapshotSplitAssigner =
                new SnapshotSplitAssigner<>(context, currentParallelism, checkpoint, dialect);
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<SourceSplitBase> getNext() {
        return snapshotSplitAssigner.getNext();
    }

    @Override
    public boolean waitingForCompletedSplits() {
        return snapshotSplitAssigner.waitingForCompletedSplits();
    }

    @Override
    public void onCompletedSplits(List<SnapshotSplitWatermark> completedSplitWatermarks) {
        snapshotSplitAssigner.onCompletedSplits(completedSplitWatermarks);
    }

    @Override
    public void addSplits(Collection<SourceSplitBase> splits) {
        snapshotSplitAssigner.addSplits(splits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return snapshotSplitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }
}
