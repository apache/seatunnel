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

package org.apache.seatunnel.engine.e2e.storagefail;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Checkpoint storage that rejects a configurable number of writes and then delegates to the real
 * local-file storage.
 *
 * <p>The failure is injected exactly at {@link #storeCheckPoint(PipelineState)}, the point where
 * the coordinator turns a fully acknowledged checkpoint into a durable recovery point. The rejected
 * writes never touch the underlying storage, so a failed checkpoint cannot leave a readable file
 * behind: the test can prove that only successfully stored checkpoints exist.
 */
public class FailingCheckpointStorage extends LocalFileStorage {

    private static final AtomicInteger REMAINING_FAILURES = new AtomicInteger();

    private static final AtomicInteger STORE_ATTEMPTS = new AtomicInteger();

    private static final List<Long> REJECTED_CHECKPOINT_IDS = new CopyOnWriteArrayList<>();

    private static final List<Long> STORED_CHECKPOINT_IDS = new CopyOnWriteArrayList<>();

    public FailingCheckpointStorage(Map<String, String> configuration) {
        super(configuration);
    }

    /** Resets the static injection state before a test starts. */
    public static void reset(int failuresToInject) {
        REMAINING_FAILURES.set(failuresToInject);
        STORE_ATTEMPTS.set(0);
        REJECTED_CHECKPOINT_IDS.clear();
        STORED_CHECKPOINT_IDS.clear();
    }

    public static int storeAttempts() {
        return STORE_ATTEMPTS.get();
    }

    public static List<Long> rejectedCheckpointIds() {
        return REJECTED_CHECKPOINT_IDS;
    }

    public static List<Long> storedCheckpointIds() {
        return STORED_CHECKPOINT_IDS;
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        STORE_ATTEMPTS.incrementAndGet();
        int failuresLeft = REMAINING_FAILURES.getAndUpdate(value -> Math.max(0, value - 1));
        if (failuresLeft > 0) {
            REJECTED_CHECKPOINT_IDS.add(state.getCheckpointId());
            throw new CheckpointStorageException(
                    "Injected checkpoint storage write failure for job "
                            + state.getJobId()
                            + ", pipeline "
                            + state.getPipelineId()
                            + ", checkpoint "
                            + state.getCheckpointId());
        }
        String location = super.storeCheckPoint(state);
        STORED_CHECKPOINT_IDS.add(state.getCheckpointId());
        return location;
    }
}
