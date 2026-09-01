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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Stateful task-group transition workload for the IMap job-storage JMH entry point. */
@State(Scope.Thread)
public class IMapJobStorageBenchmarkWorkload {

    private static final long PRESSURE_KEY_BASE = Long.MIN_VALUE + 1_000_000L;

    @Param({"0", "1000"})
    public int storedTaskGroupCount;

    private final AtomicLong sequence = new AtomicLong();

    private IMap<Object, Object> runningJobStateMap;
    private IMap<Object, Long[]> runningJobStateTimestampsMap;

    private TaskGroupLocation taskGroupLocation;

    /** Seeds the configured number of running task groups before measuring transitions. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) {
        runningJobStateMap = environmentMap(environment, Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateTimestampsMap = environmentMap(environment, Constant.IMAP_STATE_TIMESTAMPS);
        preloadStoragePressure();
    }

    @Setup(Level.Invocation)
    public void prepareInvocation() {
        long invocation = sequence.incrementAndGet();
        taskGroupLocation = new TaskGroupLocation(Long.MAX_VALUE - invocation, 1, 1L);

        Long[] stateTimestamps = new Long[ExecutionState.values().length];
        stateTimestamps[ExecutionState.CREATED.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsMap.put(taskGroupLocation, stateTimestamps);
        runningJobStateMap.put(taskGroupLocation, ExecutionState.CREATED);
    }

    @TearDown(Level.Invocation)
    public void cleanInvocation() {
        runningJobStateMap.delete(taskGroupLocation);
        runningJobStateTimestampsMap.delete(taskGroupLocation);
    }

    public ExecutionState transitionTaskGroupState() {
        Long[] stateTimestamps = runningJobStateTimestampsMap.get(taskGroupLocation);
        stateTimestamps[ExecutionState.RUNNING.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsMap.set(taskGroupLocation, stateTimestamps);
        if (runningJobStateMap.get(taskGroupLocation) != null) {
            runningJobStateMap.set(taskGroupLocation, ExecutionState.RUNNING);
        }
        return (ExecutionState) runningJobStateMap.get(taskGroupLocation);
    }

    private void preloadStoragePressure() {
        Map<Object, Object> runningJobStates = new HashMap<>(storedTaskGroupCount);
        Map<Object, Long[]> stateTimestamps = new HashMap<>(storedTaskGroupCount);
        for (int index = 0; index < storedTaskGroupCount; index++) {
            long jobId = PRESSURE_KEY_BASE + index;
            TaskGroupLocation location = new TaskGroupLocation(jobId, 1, index);
            Long[] timestamps = new Long[ExecutionState.values().length];
            timestamps[ExecutionState.RUNNING.ordinal()] = System.currentTimeMillis();

            runningJobStates.put(location, ExecutionState.RUNNING);
            stateTimestamps.put(location, timestamps);
        }
        runningJobStateMap.putAll(runningJobStates);
        runningJobStateTimestampsMap.putAll(stateTimestamps);
    }

    private static <K, V> IMap<K, V> environmentMap(
            SeaTunnelStorageEnvironmentContext environment, String mapName) {
        return environment.getServer().getNodeEngine().getHazelcastInstance().getMap(mapName);
    }
}
