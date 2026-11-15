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

import java.util.Collections;

/** Fixed-size job-lifecycle growth phases that start from controlled IMap cardinalities. */
@State(Scope.Thread)
public class IMapJobGrowthBenchmarkWorkload {

    public static final int GROWTH_OPERATIONS_PER_INVOCATION = 100;

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

    private long[] batchJobIds;
    private TaskGroupLocation[] batchTaskGroupLocations;
    private Long[][] batchStateTimestamps;
    private JobHistoryService.JobState[] batchFinishedJobStates;
    private int baselineRunningJobCount;
    private int baselineFinishedJobCount;
    private int baselineFinishedJobMetricsCount;
    private GrowthPhase growthPhase = GrowthPhase.NONE;

    /** Captures real Zeta lifecycle values and seeds the requested initial storage pressure. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) throws Exception {
        fixtureJob = new StorageLifecycleFixtureJob(environment);
        fixtureJob.start();
        try {
            runningJobInfo = fixtureJob.runningJobInfo();
            fixtureJob.finish();
            finishedJobState = fixtureJob.finishedState();
            finishedJobMetrics = fixtureJob.finishedMetrics();

            runningJobInfoMap = environmentMap(environment, Constant.IMAP_RUNNING_JOB_INFO);
            runningJobStateMap = environmentMap(environment, Constant.IMAP_RUNNING_JOB_STATE);
            runningJobStateTimestampsMap =
                    environmentMap(environment, Constant.IMAP_STATE_TIMESTAMPS);
            finishedJobStateMap = environmentMap(environment, Constant.IMAP_FINISHED_JOB_STATE);
            finishedJobMetricsMap = environmentMap(environment, Constant.IMAP_FINISHED_JOB_METRICS);
            jobHistoryService =
                    environment.getServer().getCoordinatorService().getJobHistoryService();

            preloadStoragePressure();
            baselineRunningJobCount = runningJobInfoMap.size();
            baselineFinishedJobCount = finishedJobStateMap.size();
            baselineFinishedJobMetricsCount = finishedJobMetricsMap.size();
        } catch (Exception setupFailure) {
            closeFixtureAfterFailedSetup(setupFailure);
            throw setupFailure;
        }
    }

    /** Restores the requested pressure and builds one deterministic growth batch off the clock. */
    @Setup(Level.Iteration)
    public void prepareGrowthPhase() {
        cleanPreviousGrowthPhase();
        prepareGrowthBatch();
        growthPhase = GrowthPhase.NONE;
    }

    /** Adds a fixed phase of running jobs while retaining every entry created within that phase. */
    public long appendRunningJobBatch() {
        growthPhase = GrowthPhase.RUNNING;
        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            runningJobInfoMap.put(batchJobIds[index], runningJobInfo);
            runningJobStateMap.put(batchTaskGroupLocations[index], ExecutionState.RUNNING);
            runningJobStateTimestampsMap.put(
                    batchTaskGroupLocations[index], batchStateTimestamps[index]);
        }
        return batchJobIds[GROWTH_OPERATIONS_PER_INVOCATION - 1];
    }

    /**
     * Persists a completed-job growth phase and removes every transient running state. Finished
     * state and metrics remain until the next non-timed phase reset or their production TTL.
     */
    public long appendCompletedJobLifecycleBatch() {
        growthPhase = GrowthPhase.COMPLETED;
        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = batchJobIds[index];
            TaskGroupLocation taskGroupLocation = batchTaskGroupLocations[index];
            runningJobInfoMap.put(jobId, runningJobInfo);
            runningJobStateMap.put(taskGroupLocation, ExecutionState.RUNNING);
            runningJobStateTimestampsMap.put(taskGroupLocation, batchStateTimestamps[index]);

            jobHistoryService.storeFinishedPipelineMetrics(jobId, finishedJobMetrics);
            jobHistoryService.storeFinishedJobState(batchFinishedJobStates[index]);

            runningJobInfoMap.delete(jobId);
            runningJobStateMap.delete(taskGroupLocation);
            runningJobStateTimestampsMap.delete(taskGroupLocation);
        }
        return batchJobIds[GROWTH_OPERATIONS_PER_INVOCATION - 1];
    }

    /** Verifies that the non-timed fixture pressure grew by exactly one controlled phase. */
    @TearDown(Level.Iteration)
    public void verifyGrowthPhase() {
        if (growthPhase == GrowthPhase.RUNNING) {
            if (runningJobInfoMap.size()
                    != baselineRunningJobCount + GROWTH_OPERATIONS_PER_INVOCATION) {
                throw new IllegalStateException(
                        "The running-job growth phase did not retain every entry");
            }
            verifyLastRunningJobDurability();
        } else if (growthPhase == GrowthPhase.COMPLETED) {
            if (finishedJobStateMap.size()
                            != baselineFinishedJobCount + GROWTH_OPERATIONS_PER_INVOCATION
                    || finishedJobMetricsMap.size()
                            != baselineFinishedJobMetricsCount + GROWTH_OPERATIONS_PER_INVOCATION) {
                throw new IllegalStateException(
                        "The completed-job growth phase did not retain every entry");
            }
            verifyLastCompletedJobDurability();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (fixtureJob != null) {
            fixtureJob.close();
        }
    }

    private void preloadStoragePressure() {
        // Keep fixture generation single-threaded. IMap.putAll fans entries out across partition
        // threads, while the file-backed WAL currently uses a single producer.
        for (int index = 0; index < initialStoredJobCount; index++) {
            long pressureJobId = PRESSURE_KEY_BASE + index;
            TaskGroupLocation location = new TaskGroupLocation(pressureJobId, 1, index);
            Long[] timestamps = new Long[ExecutionState.values().length];
            timestamps[ExecutionState.RUNNING.ordinal()] = finishedJobState.getStartTime();

            runningJobInfoMap.put(pressureJobId, runningJobInfo);
            runningJobStateMap.put(location, ExecutionState.RUNNING);
            runningJobStateTimestampsMap.put(location, timestamps);
            finishedJobStateMap.put(pressureJobId, finishedJobState);
            finishedJobMetricsMap.put(pressureJobId, finishedJobMetrics);
        }
    }

    private void prepareGrowthBatch() {
        batchJobIds = new long[GROWTH_OPERATIONS_PER_INVOCATION];
        batchTaskGroupLocations = new TaskGroupLocation[GROWTH_OPERATIONS_PER_INVOCATION];
        batchStateTimestamps = new Long[GROWTH_OPERATIONS_PER_INVOCATION][];
        batchFinishedJobStates = new JobHistoryService.JobState[GROWTH_OPERATIONS_PER_INVOCATION];
        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = GROWTH_KEY_BASE + index;
            TaskGroupLocation location = new TaskGroupLocation(jobId, 1, index);
            Long[] timestamps = new Long[ExecutionState.values().length];
            timestamps[ExecutionState.RUNNING.ordinal()] = finishedJobState.getStartTime();
            batchJobIds[index] = jobId;
            batchTaskGroupLocations[index] = location;
            batchStateTimestamps[index] = timestamps;
            batchFinishedJobStates[index] =
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
    }

    private void cleanPreviousGrowthPhase() {
        if (batchJobIds == null) {
            return;
        }
        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = batchJobIds[index];
            TaskGroupLocation location = batchTaskGroupLocations[index];
            if (growthPhase == GrowthPhase.RUNNING) {
                runningJobInfoMap.delete(jobId);
                runningJobStateMap.delete(location);
                runningJobStateTimestampsMap.delete(location);
            } else if (growthPhase == GrowthPhase.COMPLETED) {
                finishedJobStateMap.delete(jobId);
                finishedJobMetricsMap.delete(jobId);
            }
        }
    }

    private void verifyLastRunningJobDurability() {
        int lastIndex = GROWTH_OPERATIONS_PER_INVOCATION - 1;
        long jobId = batchJobIds[lastIndex];
        TaskGroupLocation taskGroupLocation = batchTaskGroupLocations[lastIndex];

        reloadFromMapStore(runningJobInfoMap, jobId);
        reloadFromMapStore(runningJobStateMap, taskGroupLocation);
        reloadFromMapStore(runningJobStateTimestampsMap, taskGroupLocation);

        Long[] timestamps = runningJobStateTimestampsMap.get(taskGroupLocation);
        if (runningJobInfoMap.get(jobId) == null
                || runningJobStateMap.get(taskGroupLocation) != ExecutionState.RUNNING
                || timestamps == null
                || timestamps[ExecutionState.RUNNING.ordinal()] == null) {
            throw new IllegalStateException(
                    "The last running-job growth entry was not durably persisted");
        }
    }

    private void verifyLastCompletedJobDurability() {
        int lastIndex = GROWTH_OPERATIONS_PER_INVOCATION - 1;
        long jobId = batchJobIds[lastIndex];
        TaskGroupLocation taskGroupLocation = batchTaskGroupLocations[lastIndex];

        reloadFromMapStore(finishedJobStateMap, jobId);
        reloadFromMapStore(finishedJobMetricsMap, jobId);

        if (runningJobInfoMap.get(jobId) != null
                || runningJobStateMap.get(taskGroupLocation) != null
                || runningJobStateTimestampsMap.get(taskGroupLocation) != null
                || finishedJobStateMap.get(jobId) == null
                || finishedJobMetricsMap.get(jobId) == null) {
            throw new IllegalStateException(
                    "The last completed-job lifecycle was not durably persisted");
        }
    }

    private static <K, V> void reloadFromMapStore(IMap<K, V> map, K key) {
        map.evict(key);
        map.loadAll(Collections.singleton(key), true);
    }

    private void closeFixtureAfterFailedSetup(Exception setupFailure) {
        try {
            fixtureJob.close();
        } catch (Exception cleanupFailure) {
            setupFailure.addSuppressed(cleanupFailure);
        } finally {
            fixtureJob = null;
        }
    }

    private static <K, V> IMap<K, V> environmentMap(
            SeaTunnelStorageEnvironmentContext environment, String mapName) {
        return environment.getServer().getNodeEngine().getHazelcastInstance().getMap(mapName);
    }

    private enum GrowthPhase {
        NONE,
        RUNNING,
        COMPLETED
    }
}
