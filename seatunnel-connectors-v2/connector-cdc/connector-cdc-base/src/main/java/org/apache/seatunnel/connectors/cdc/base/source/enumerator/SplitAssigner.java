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

import org.apache.seatunnel.api.state.CheckpointListener;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import io.debezium.relational.TableId;
import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The {@code SplitAssigner} is responsible for deciding what split should be processed. It
 * determines split processing order.
 */
public interface SplitAssigner {

    /**
     * Called to open the assigner to acquire any resources, like threads or network connections.
     */
    void open();

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers completed their current splits.
     */
    Optional<SourceSplitBase> getNext();

    /**
     * Whether the split assigner is still waiting for callback of completed splits, i.e. {@link #onCompletedSplits}.
     */
    boolean waitingForCompletedSplits();

    /**
     * Callback to handle the completed splits with completed change log offset. This is useful for
     * determine when to generate incremental split and what incremental split to generate.
     */
    void onCompletedSplits(List<SnapshotSplitWatermark> completedSplitWatermarks);

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added.
     */
    void addSplits(Collection<SourceSplitBase> splits);

    /**
     * Creates a snapshot of the state of this split assigner, to be stored in a checkpoint.
     *
     * <p>The snapshot should contain the latest state of the assigner: It should assume that all
     * operations that happened before the snapshot have successfully completed. For example all
     * splits assigned to readers via {@link #getNext()} don't need to be included in the snapshot
     * anymore.
     *
     * <p>This method takes the ID of the checkpoint for which the state is snapshotted. Most
     * implementations should be able to ignore this parameter, because for the contents of the
     * snapshot, it doesn't matter for which checkpoint it gets created. This parameter can be
     * interesting for source connectors with external systems where those systems are themselves
     * aware of checkpoints; for example in cases where the enumerator notifies that system about a
     * specific checkpoint being triggered.
     *
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return an object containing the state of the split enumerator.
     */
    PendingSplitsState snapshotState(long checkpointId);

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    void notifyCheckpointComplete(long checkpointId);

    /**
     * Called to close the assigner, in case it holds on to any resources, like threads or network
     * connections.
     */
    default void close() {
    }

    @Data
    final class Context<C extends SourceConfig> {
        private final C sourceConfig;

        private final Set<TableId> capturedTables;

        private final Map<String, SnapshotSplit> assignedSnapshotSplit;

        /**
         * key: SnapshotSplit id
         */
        private final Map<String, Offset> splitCompletedOffsets;
    }
}
