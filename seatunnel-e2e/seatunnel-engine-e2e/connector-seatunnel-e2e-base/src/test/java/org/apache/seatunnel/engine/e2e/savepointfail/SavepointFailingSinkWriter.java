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

package org.apache.seatunnel.engine.e2e.savepointfail;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Writer that counts rows and rejects every snapshot, simulating a sink that cannot produce a
 * consistent snapshot of its writer state (for example a failed object-store upload or a commit
 * token that cannot be obtained).
 *
 * <p>The Zeta cluster of the engine E2E tests runs inside the test JVM, so the static counters let
 * the test prove that the savepoint barrier actually reached the writer and that the injected
 * failure was triggered instead of the request being rejected by an unrelated precondition.
 */
public class SavepointFailingSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    /** Rows delivered to the writer. */
    public static final AtomicLong WRITTEN_ROWS = new AtomicLong();

    /** Snapshot attempts, i.e. barriers that reached {@link #snapshotState(long)}. */
    public static final AtomicLong SNAPSHOT_ATTEMPTS = new AtomicLong();

    private final SinkWriter.Context context;

    public SavepointFailingSinkWriter(SinkWriter.Context context) {
        this.context = context;
    }

    @Override
    public void write(SeaTunnelRow element) {
        WRITTEN_ROWS.incrementAndGet();
    }

    @Override
    public java.util.List<Void> snapshotState(long checkpointId) {
        SNAPSHOT_ATTEMPTS.incrementAndGet();
        throw new IllegalStateException(
                "Injected sink snapshot failure for checkpoint "
                        + checkpointId
                        + " on subtask "
                        + context.getIndexOfSubtask());
    }

    @Override
    public void close() {
        // Nothing to release: the sink keeps no external resources.
    }
}
