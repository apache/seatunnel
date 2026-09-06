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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.PendingCheckpoint;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.seatunnel.engine.core.checkpoint.CheckpointType.CHECKPOINT_TYPE;

/** Benchmark-only test bridge for explicitly triggering a regular checkpoint. */
final class CheckpointBenchmarkTrigger {

    private CheckpointBenchmarkTrigger() {}

    @SuppressWarnings("unchecked")
    static PassiveCompletableFuture<CompletedCheckpoint> trigger(
            CheckpointCoordinator coordinator) {
        // This bridge bypasses the coordinator trigger lock, so callers must keep periodic
        // checkpoints effectively disabled and use a single JMH thread to prevent concurrent
        // checkpoint triggers.
        CompletableFuture<PendingCheckpoint> pendingCheckpoint =
                (CompletableFuture<PendingCheckpoint>)
                        ReflectionUtils.invoke(
                                coordinator,
                                "createPendingCheckpoint",
                                new Class<?>[] {long.class, CheckpointType.class},
                                new Object[] {Instant.now().toEpochMilli(), CHECKPOINT_TYPE});
        ReflectionUtils.invoke(
                coordinator,
                "startTriggerPendingCheckpoint",
                new Class<?>[] {CompletableFuture.class},
                new Object[] {pendingCheckpoint});
        return pendingCheckpoint.join().getCompletableFuture();
    }

    static boolean hasPendingCheckpoint(CheckpointCoordinator coordinator) {
        AtomicInteger pendingCounter =
                (AtomicInteger)
                        ReflectionUtils.getField(coordinator, "pendingCounter")
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Checkpoint pending counter is unavailable"));
        return pendingCounter.get() > 0;
    }
}
