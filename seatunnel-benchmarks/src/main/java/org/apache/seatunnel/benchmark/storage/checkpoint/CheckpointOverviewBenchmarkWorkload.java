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

package org.apache.seatunnel.benchmark.storage.checkpoint;

import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import static org.apache.seatunnel.benchmark.storage.checkpoint.CheckpointStorageBenchmarkFixture.CHECKPOINT_OPERATIONS_PER_INVOCATION;
import static org.apache.seatunnel.benchmark.storage.checkpoint.CheckpointStorageBenchmarkFixture.CheckpointOperation;

/** Executes and verifies completed-checkpoint overview updates. */
@State(Scope.Thread)
public class CheckpointOverviewBenchmarkWorkload {

    private CheckpointStorageBenchmarkFixture fixture;
    private CheckpointOperation[] operations;
    private CompletedCheckpoint[] completedCheckpoints;
    private int preparedOperationCount;
    private boolean invoked;

    @Setup(Level.Trial)
    public void setUp(CheckpointStorageBenchmarkFixture fixture) {
        this.fixture = fixture;
    }

    @Setup(Level.Iteration)
    public void prepareIteration() throws Exception {
        invoked = false;
        preparedOperationCount = 0;
        operations = fixture.createOperations();
        completedCheckpoints = new CompletedCheckpoint[CHECKPOINT_OPERATIONS_PER_INVOCATION];

        for (int index = 0; index < CHECKPOINT_OPERATIONS_PER_INVOCATION; index++) {
            preparedOperationCount = index + 1;
            fixture.prepareOverview(operations[index]);
            completedCheckpoints[index] = fixture.createCompletedCheckpoint(operations[index]);
        }
    }

    public void updateCheckpointOverview() {
        invoked = true;
        for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
            fixture.updateOverview(completedCheckpoint);
        }
    }

    @TearDown(Level.Iteration)
    public void validateAndCleanIteration() {
        try {
            requireCompletedInvocation();
            fixture.reloadOverviewSamples(operations);
            for (CheckpointOperation operation : operations) {
                fixture.validateOverview(operation);
            }
        } finally {
            for (int index = 0; index < preparedOperationCount; index++) {
                fixture.removeOverview(operations[index]);
            }
            preparedOperationCount = 0;
            invoked = false;
        }
    }

    private void requireCompletedInvocation() {
        if (!invoked) {
            throw new IllegalStateException(
                    "No checkpoint overview benchmark operation was recorded");
        }
        if (preparedOperationCount != CHECKPOINT_OPERATIONS_PER_INVOCATION) {
            throw new IllegalStateException("Checkpoint overview phase was not fully prepared");
        }
    }
}
