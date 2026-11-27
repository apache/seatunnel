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

package org.apache.seatunnel.engine.e2e.savepointfailover;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Writer of {@link SavepointBarrierHoldSink}.
 *
 * <p>Every row is counted and discarded; the interesting method is {@link #snapshotState(long)},
 * which is called for each checkpoint barrier after the task received it and before the task
 * acknowledges it to the master. Parking there keeps the savepoint checkpoint {@code
 * PendingCheckpoint} on the master with at least one unacknowledged subtask, which is exactly the
 * window the test needs to switch the active master.
 */
@Slf4j
public class SavepointBarrierHoldSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final String holdKey;

    private final int subtaskIndex;

    private long writtenRows;

    public SavepointBarrierHoldSinkWriter(SinkWriter.Context context, String holdKey) {
        this.holdKey = holdKey;
        this.subtaskIndex = context.getIndexOfSubtask();
    }

    @Override
    public void write(SeaTunnelRow element) {
        writtenRows++;
        SavepointBarrierHoldGate.onRow(holdKey);
    }

    /**
     * Holds the barrier acknowledgement on the armed gate.
     *
     * <p>The returned state list stays empty: the test asserts the savepoint lifecycle, not sink
     * state, and a stateful writer would add a second failure mode to the scenario.
     */
    @Override
    public List<Void> snapshotState(long checkpointId) {
        // WARN on purpose: the module's test logging keeps the root logger at WARN.
        log.warn(
                "Writer {} received the barrier of checkpoint {} after {} rows, holding snapshotState on key {}",
                subtaskIndex,
                checkpointId,
                writtenRows,
                holdKey);
        SavepointBarrierHoldGate.awaitRelease(holdKey, checkpointId);
        log.warn(
                "Writer {} released checkpoint {} from the snapshotState hold on key {}",
                subtaskIndex,
                checkpointId,
                holdKey);
        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
        // WARN on purpose: the module's test logging keeps the root logger at WARN.
        log.warn("Writer {} closed after {} rows", subtaskIndex, writtenRows);
    }
}
