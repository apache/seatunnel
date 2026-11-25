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

package org.apache.seatunnel.engine.e2e.savepointcancel;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * Writer of {@link SavepointCancelHoldSink}.
 *
 * <p>It consumes every row (the count is published through the gate so the test can prove that the
 * job was processing data before the savepoint) and, on the configured snapshot number, parks
 * inside {@link #snapshotState(long)} until the test releases {@link SavepointCancelGate}. Because
 * the barrier acknowledgement is sent only after {@code snapshotState} returns, the coordinator
 * keeps that savepoint pending for exactly as long as the test needs it to be pending.
 */
@Slf4j
public class SavepointCancelHoldSinkWriter extends AbstractSinkWriter<SeaTunnelRow, String> {

    private final String holdKey;

    private final int blockOnSnapshot;

    public SavepointCancelHoldSinkWriter(String holdKey, int blockOnSnapshot) {
        this.holdKey = holdKey;
        this.blockOnSnapshot = blockOnSnapshot;
    }

    @Override
    public void write(SeaTunnelRow element) {
        SavepointCancelGate.addRows(holdKey, 1L);
    }

    @Override
    public List<String> snapshotState(long checkpointId) {
        if (SavepointCancelGate.holdSnapshot(holdKey, blockOnSnapshot, checkpointId)) {
            // WARN on purpose: the module's test logging keeps the root logger at WARN.
            log.warn(
                    "Sink writer released the held snapshot of checkpoint {} for key {}",
                    checkpointId,
                    holdKey);
        }
        return Collections.singletonList(SavepointCancelHoldSink.STATE_PREFIX + checkpointId);
    }

    @Override
    public void close() {
        // Nothing to flush: this sink exists only to hold a snapshot barrier.
    }
}
