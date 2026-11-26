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

package org.apache.seatunnel.engine.e2e.savepointworker;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Writer of {@link SavepointWorkerHoldSink}.
 *
 * <p>On the configured snapshot it publishes the checkpoint id in the {@code .held} marker file and
 * then parks before returning from {@link #snapshotState(long)}. The barrier acknowledgement is
 * sent only after that method returns, so the coordinator keeps the checkpoint pending for exactly
 * as long as the test needs it to be. The test ends the hold by creating the {@code .release}
 * marker file.
 *
 * <p>Interrupts are deliberately ignored while parked. A worker shutdown interrupts its task
 * threads, but the saved window only exists if the coordinator still sees an unacknowledged barrier
 * at the instant the worker disappears.
 */
@Slf4j
public class SavepointWorkerHoldSinkWriter extends AbstractSinkWriter<SeaTunnelRow, String> {

    /** Upper bound for one hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);

    /** How often the parked writer checks for the release marker. */
    private static final long RELEASE_POLL_MILLIS = 50L;

    private final Path holdDir;

    private final String holdKey;

    private final int blockOnSnapshot;

    private long snapshotCount;

    public SavepointWorkerHoldSinkWriter(String holdDir, String holdKey, int blockOnSnapshot) {
        this.holdDir = Paths.get(holdDir);
        this.holdKey = holdKey;
        this.blockOnSnapshot = blockOnSnapshot;
    }

    @Override
    public void write(SeaTunnelRow element) {
        // Every row is consumed; the engine effects under test come from the held barrier.
    }

    @Override
    public List<String> snapshotState(long checkpointId) {
        snapshotCount++;
        if (snapshotCount == blockOnSnapshot) {
            holdSnapshot(checkpointId);
        }
        return Collections.singletonList(SavepointWorkerHoldSink.STATE_PREFIX + checkpointId);
    }

    private void holdSnapshot(long checkpointId) {
        Path releaseMarker = holdDir.resolve(holdKey + ".release");
        if (Files.exists(releaseMarker)) {
            return;
        }
        Path heldMarker = holdDir.resolve(holdKey + ".held");
        try {
            Files.createDirectories(holdDir);
            Path temporaryMarker = holdDir.resolve(holdKey + ".held.tmp");
            Files.write(
                    temporaryMarker, Long.toString(checkpointId).getBytes(StandardCharsets.UTF_8));
            Files.move(
                    temporaryMarker,
                    heldMarker,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to publish the savepoint hold marker " + heldMarker, e);
        }

        boolean interrupted = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_HOLD_MILLIS);
        try {
            while (!Files.exists(releaseMarker)) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    log.warn(
                            "Savepoint snapshot hold for key {} was not released within {} ms,"
                                    + " giving up the hold",
                            holdKey,
                            MAX_HOLD_MILLIS);
                    return;
                }
                try {
                    Thread.sleep(Math.min(RELEASE_POLL_MILLIS, remainingNanos / 1_000_000L));
                } catch (InterruptedException e) {
                    // Keep holding: worker teardown interrupts task threads, but the barrier has to
                    // stay unacknowledged until the test has killed the worker.
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        // Nothing to flush: this sink exists only to hold a snapshot barrier.
    }
}
