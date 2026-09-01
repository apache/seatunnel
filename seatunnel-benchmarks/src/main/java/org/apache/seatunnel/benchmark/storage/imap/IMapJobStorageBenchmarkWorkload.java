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

package org.apache.seatunnel.benchmark.storage.imap;

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.map.IMap;

import java.util.LinkedHashSet;
import java.util.Set;

/** Stateful task-group transition workload for the IMap job-storage JMH entry point. */
@State(Scope.Thread)
public class IMapJobStorageBenchmarkWorkload {

    public static final int TRANSITION_OPERATIONS_PER_INVOCATION = 100;

    private static final long PRESSURE_KEY_BASE = Long.MIN_VALUE + 1_000_000L;
    private static final long TRANSITION_KEY_BASE = Long.MIN_VALUE + 3_000_000L;

    @Param({"0", "1000"})
    public int storedTaskGroupCount;

    private IMap<Object, Object> runningJobStateMap;
    private IMap<Object, Long[]> runningJobStateTimestampsMap;

    private TaskGroupLocation[] taskGroupLocations;
    private long transitionBatchSequence;

    /** Seeds the configured number of running task groups before measuring transitions. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) {
        runningJobStateMap = environmentMap(environment, Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateTimestampsMap = environmentMap(environment, Constant.IMAP_STATE_TIMESTAMPS);
        preloadStoragePressure();
    }

    @Setup(Level.Iteration)
    public void prepareTransitionBatch() {
        taskGroupLocations = new TaskGroupLocation[TRANSITION_OPERATIONS_PER_INVOCATION];
        long batchJobId =
                TRANSITION_KEY_BASE
                        + transitionBatchSequence++ * TRANSITION_OPERATIONS_PER_INVOCATION;
        // Keep fixture generation single-threaded. IMap.putAll fans entries out across partition
        // threads, while the file-backed WAL currently uses a single producer.
        for (int index = 0; index < TRANSITION_OPERATIONS_PER_INVOCATION; index++) {
            TaskGroupLocation location = new TaskGroupLocation(batchJobId + index, 1, index);
            Long[] timestamps = new Long[ExecutionState.values().length];
            timestamps[ExecutionState.CREATED.ordinal()] = System.currentTimeMillis();

            taskGroupLocations[index] = location;
            runningJobStateMap.put(location, ExecutionState.CREATED);
            runningJobStateTimestampsMap.put(location, timestamps);
        }
    }

    @TearDown(Level.Iteration)
    public void verifyAndCleanTransitionBatch() {
        try {
            verifyResidentTransitions();
            verifyDurableTransitions();
        } finally {
            cleanTransitionBatch();
        }
    }

    /** Executes one fixed-size phase of the timestamp and state operations used by Zeta. */
    public ExecutionState transitionTaskGroupStateBatch() {
        ExecutionState lastState = null;
        for (TaskGroupLocation taskGroupLocation : taskGroupLocations) {
            Long[] stateTimestamps = runningJobStateTimestampsMap.get(taskGroupLocation);
            stateTimestamps[ExecutionState.RUNNING.ordinal()] = System.currentTimeMillis();
            runningJobStateTimestampsMap.set(taskGroupLocation, stateTimestamps);
            if (runningJobStateMap.get(taskGroupLocation) != null) {
                runningJobStateMap.set(taskGroupLocation, ExecutionState.RUNNING);
            }
            lastState = (ExecutionState) runningJobStateMap.get(taskGroupLocation);
        }
        return lastState;
    }

    private void verifyResidentTransitions() {
        for (TaskGroupLocation taskGroupLocation : taskGroupLocations) {
            verifyTransitionValue(taskGroupLocation, "resident");
        }
    }

    private void verifyDurableTransitions() {
        Set<Object> sampledLocations = new LinkedHashSet<>();
        sampledLocations.add(taskGroupLocations[0]);
        sampledLocations.add(taskGroupLocations[TRANSITION_OPERATIONS_PER_INVOCATION / 2]);
        sampledLocations.add(taskGroupLocations[TRANSITION_OPERATIONS_PER_INVOCATION - 1]);
        for (Object sampledLocation : sampledLocations) {
            runningJobStateMap.evict(sampledLocation);
            runningJobStateTimestampsMap.evict(sampledLocation);
        }
        runningJobStateMap.loadAll(sampledLocations, true);
        runningJobStateTimestampsMap.loadAll(sampledLocations, true);
        for (Object sampledLocation : sampledLocations) {
            verifyTransitionValue((TaskGroupLocation) sampledLocation, "durably persisted");
        }
    }

    private void verifyTransitionValue(TaskGroupLocation taskGroupLocation, String location) {
        Long[] timestamps = runningJobStateTimestampsMap.get(taskGroupLocation);
        if (runningJobStateMap.get(taskGroupLocation) != ExecutionState.RUNNING
                || timestamps == null
                || timestamps[ExecutionState.RUNNING.ordinal()] == null) {
            throw new IllegalStateException(
                    "The task-group transition was not " + location + ": " + taskGroupLocation);
        }
    }

    private void cleanTransitionBatch() {
        if (taskGroupLocations == null) {
            return;
        }
        for (TaskGroupLocation taskGroupLocation : taskGroupLocations) {
            runningJobStateMap.delete(taskGroupLocation);
            runningJobStateTimestampsMap.delete(taskGroupLocation);
        }
    }

    private void preloadStoragePressure() {
        // Keep fixture generation single-threaded for the file-backed WAL.
        for (int index = 0; index < storedTaskGroupCount; index++) {
            long jobId = PRESSURE_KEY_BASE + index;
            TaskGroupLocation location = new TaskGroupLocation(jobId, 1, index);
            Long[] timestamps = new Long[ExecutionState.values().length];
            timestamps[ExecutionState.RUNNING.ordinal()] = System.currentTimeMillis();

            runningJobStateMap.put(location, ExecutionState.RUNNING);
            runningJobStateTimestampsMap.put(location, timestamps);
        }
    }

    private static <K, V> IMap<K, V> environmentMap(
            SeaTunnelStorageEnvironmentContext environment, String mapName) {
        return environment.getServer().getNodeEngine().getHazelcastInstance().getMap(mapName);
    }
}
