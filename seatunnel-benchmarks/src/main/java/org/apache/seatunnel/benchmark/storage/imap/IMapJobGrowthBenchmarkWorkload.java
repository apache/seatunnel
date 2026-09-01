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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.benchmark.storage.StorageLifecycleFixtureJob;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.map.IMap;

import java.util.HashMap;
import java.util.Map;

/** Stateful job-lifecycle workloads whose persisted cardinality grows throughout a JMH trial. */
@State(Scope.Thread)
public class IMapJobGrowthBenchmarkWorkload {

    private static final long PRESSURE_KEY_BASE = Long.MIN_VALUE + 2_000_000L;
    private static final long GROWTH_KEY_BASE = Long.MIN_VALUE + 4_000_000L;

    @Param({"0", "1000"})
    public int initialStoredJobCount;

    private StorageLifecycleFixtureJob fixtureJob;
    private JobInfo runningJobInfo;
    private JobHistoryService.JobState finishedJobState;
    private JobMetrics finishedJobMetrics;
    private JobHistoryService jobHistoryService;

    private IMap<Long, JobInfo> runningJobInfoMap;
    private IMap<Object, Object> runningJobStateMap;
    private IMap<Object, Long[]> runningJobStateTimestampsMap;
    private IMap<Long, JobHistoryService.JobState> finishedJobStateMap;
    private IMap<Long, JobMetrics> finishedJobMetricsMap;

    private long sequence;
    private long jobId;
    private TaskGroupLocation taskGroupLocation;
    private Long[] stateTimestamps;
    private JobHistoryService.JobState invocationFinishedJobState;

    /** Captures real Zeta lifecycle values and seeds the requested initial storage pressure. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) throws Exception {
        fixtureJob = new StorageLifecycleFixtureJob(environment);
        fixtureJob.start();
        runningJobInfo = fixtureJob.runningJobInfo();
        fixtureJob.finish();
        finishedJobState = fixtureJob.finishedState();
        finishedJobMetrics = fixtureJob.finishedMetrics();

        runningJobInfoMap = environmentMap(environment, Constant.IMAP_RUNNING_JOB_INFO);
        runningJobStateMap = environmentMap(environment, Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateTimestampsMap = environmentMap(environment, Constant.IMAP_STATE_TIMESTAMPS);
        finishedJobStateMap = environmentMap(environment, Constant.IMAP_FINISHED_JOB_STATE);
        finishedJobMetricsMap = environmentMap(environment, Constant.IMAP_FINISHED_JOB_METRICS);
        jobHistoryService = environment.getServer().getCoordinatorService().getJobHistoryService();

        preloadStoragePressure();
    }

    /** Prepares the next unique job payload without performing storage I/O. */
    @Setup(Level.Invocation)
    public void prepareInvocation() {
        jobId = GROWTH_KEY_BASE + sequence++;
        taskGroupLocation = new TaskGroupLocation(jobId, 1, 1L);
        stateTimestamps = new Long[ExecutionState.values().length];
        stateTimestamps[ExecutionState.RUNNING.ordinal()] = System.currentTimeMillis();
        invocationFinishedJobState =
                new JobHistoryService.JobState(
                        jobId,
                        finishedJobState.getJobName(),
                        finishedJobState.getJobStatus(),
                        finishedJobState.getSubmitTime(),
                        finishedJobState.getStartTime(),
                        finishedJobState.getFinishTime(),
                        finishedJobState.getPipelineStateMapperMap(),
                        finishedJobState.getErrorMessage());
    }

    /** Adds one concurrently running job and retains it so every invocation increases pressure. */
    public long appendRunningJob() {
        runningJobInfoMap.put(jobId, runningJobInfo);
        runningJobStateMap.put(taskGroupLocation, ExecutionState.RUNNING);
        runningJobStateTimestampsMap.put(taskGroupLocation, stateTimestamps);
        return jobId;
    }

    /**
     * Persists one completed job's history and removes its transient running state. Finished state
     * and metrics remain until their production TTL expires, so history grows across invocations.
     */
    public long appendCompletedJobLifecycle() {
        runningJobInfoMap.put(jobId, runningJobInfo);
        runningJobStateMap.put(taskGroupLocation, ExecutionState.RUNNING);
        runningJobStateTimestampsMap.put(taskGroupLocation, stateTimestamps);

        jobHistoryService.storeFinishedPipelineMetrics(jobId, finishedJobMetrics);
        jobHistoryService.storeFinishedPipelineMetrics(jobId, finishedJobMetrics);
        jobHistoryService.storeFinishedJobState(invocationFinishedJobState);

        runningJobInfoMap.delete(jobId);
        runningJobStateMap.delete(taskGroupLocation);
        runningJobStateTimestampsMap.delete(taskGroupLocation);
        return jobId;
    }

    int runningJobCount() {
        return runningJobInfoMap.size();
    }

    int finishedJobCount() {
        return finishedJobStateMap.size();
    }

    int finishedJobMetricsCount() {
        return finishedJobMetricsMap.size();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (fixtureJob != null) {
            fixtureJob.close();
        }
    }

    private void preloadStoragePressure() {
        Map<Long, JobInfo> runningJobInfos = new HashMap<>(initialStoredJobCount);
        Map<Object, Object> runningJobStates = new HashMap<>(initialStoredJobCount);
        Map<Object, Long[]> stateTimestampsValues = new HashMap<>(initialStoredJobCount);
        Map<Long, JobHistoryService.JobState> finishedJobStates =
                new HashMap<>(initialStoredJobCount);
        Map<Long, JobMetrics> finishedJobMetricsValues = new HashMap<>(initialStoredJobCount);
        for (int index = 0; index < initialStoredJobCount; index++) {
            long pressureJobId = PRESSURE_KEY_BASE + index;
            TaskGroupLocation location = new TaskGroupLocation(pressureJobId, 1, index);
            Long[] timestamps = new Long[ExecutionState.values().length];
            timestamps[ExecutionState.RUNNING.ordinal()] = finishedJobState.getStartTime();

            runningJobInfos.put(pressureJobId, runningJobInfo);
            runningJobStates.put(location, ExecutionState.RUNNING);
            stateTimestampsValues.put(location, timestamps);
            finishedJobStates.put(pressureJobId, finishedJobState);
            finishedJobMetricsValues.put(pressureJobId, finishedJobMetrics);
        }
        runningJobInfoMap.putAll(runningJobInfos);
        runningJobStateMap.putAll(runningJobStates);
        runningJobStateTimestampsMap.putAll(stateTimestampsValues);
        finishedJobStateMap.putAll(finishedJobStates);
        finishedJobMetricsMap.putAll(finishedJobMetricsValues);
    }

    private static <K, V> IMap<K, V> environmentMap(
            SeaTunnelStorageEnvironmentContext environment, String mapName) {
        return environment.getServer().getNodeEngine().getHazelcastInstance().getMap(mapName);
    }
}
