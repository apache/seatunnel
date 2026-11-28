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

package org.apache.seatunnel.engine.e2e.cancelcheckpoint;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

/**
 * Writer of {@link CheckpointInFlightHoldSink}.
 *
 * <p>The writer keeps no state and discards rows. It parks inside {@link #snapshotState(long)},
 * which is called when the checkpoint barrier reaches this task: the checkpoint is then fully
 * distributed but cannot be acknowledged, which is the deterministic window used to cancel the job
 * (or fail its master over) while the checkpoint is in flight.
 */
@Slf4j
public class CheckpointInFlightHoldSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final String holdKey;

    private long writtenRows;

    public CheckpointInFlightHoldSinkWriter(String holdKey) {
        this.holdKey = holdKey;
    }

    @Override
    public void write(SeaTunnelRow element) {
        writtenRows++;
    }

    @Override
    public List<Void> snapshotState(long checkpointId) {
        log.warn(
                "Writer with key {} reached checkpoint {} after {} rows, holding the snapshot",
                holdKey,
                checkpointId,
                writtenRows);
        CheckpointInFlightHoldGate.awaitRelease(holdKey, checkpointId);
        return Collections.emptyList();
    }

    @Override
    public void close() {}
}
