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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Fixed-size job-lifecycle growth phases that start from controlled IMap cardinalities.
 *
 * <p>Each iteration verifies resident IMap growth. FileMapStore durability is sampled on a light
 * cadence (first iteration and every {@link #DURABLE_SAMPLE_INTERVAL} iterations) and again for the
 * full last growth batch at trial tear-down. Sampling avoids replaying the full WAL between every
 * SingleShot sample while still failing the fixture when MapStore persistence did not happen.
 */
@State(Scope.Thread)
public class IMapJobGrowthBenchmarkWorkload {

    public static final int GROWTH_OPERATIONS_PER_INVOCATION = 100;

    /**
     * How often iteration tear-down reloads the current growth batch from MapStore. {@code
     * FileMapStore.loadAll} always replays the full WAL, so denser sampling reintroduces the CV
     * noise this fixture is meant to remove.
     */
    private static final int DURABLE_SAMPLE_INTERVAL = 4;

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
    private long growthBatchSequence;
    private int growthIterationIndex;
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

    /**
     * Verifies that the non-timed fixture pressure grew by exactly one controlled phase. Most
     * iterations only check resident IMap state; durability is sampled lightly because {@code
     * FileMapStore.loadAll} always replays the full WAL.
     */
    @TearDown(Level.Iteration)
    public void verifyGrowthPhase() {
        if (growthPhase == GrowthPhase.RUNNING) {
            if (runningJobInfoMap.size()
                    != baselineRunningJobCount + GROWTH_OPERATIONS_PER_INVOCATION) {
                throw new IllegalStateException(
                        "The running-job growth phase did not retain every entry");
            }
            verifyRunningJobBatchResident();
        } else if (growthPhase == GrowthPhase.COMPLETED) {
            if (finishedJobStateMap.size()
                            != baselineFinishedJobCount + GROWTH_OPERATIONS_PER_INVOCATION
                    || finishedJobMetricsMap.size()
                            != baselineFinishedJobMetricsCount + GROWTH_OPERATIONS_PER_INVOCATION) {
                throw new IllegalStateException(
                        "The completed-job growth phase did not retain every entry");
            }
            verifyCompletedJobBatchResident();
        }
        if (shouldSampleGrowthDurability()) {
            verifyGrowthBatchDurability();
        }
        growthIterationIndex++;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Exception durabilityFailure = null;
        try {
            verifyGrowthBatchDurability();
        } catch (Exception failure) {
            durabilityFailure = failure;
        }
        try {
            cleanPreviousGrowthPhase();
        } catch (Exception cleanupFailure) {
            if (durabilityFailure != null) {
                durabilityFailure.addSuppressed(cleanupFailure);
            } else {
                durabilityFailure = cleanupFailure;
            }
        }
        try {
            if (fixtureJob != null) {
                fixtureJob.close();
            }
        } catch (Exception closeFailure) {
            if (durabilityFailure != null) {
                durabilityFailure.addSuppressed(closeFailure);
            } else {
                throw closeFailure;
            }
        } finally {
            fixtureJob = null;
        }
        if (durabilityFailure != null) {
            throw durabilityFailure;
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
        // Unique keys per iteration avoid delete/re-put churn on the same WAL keys and match the
        // transition / DAG store fixtures.
        long batchJobIdBase =
                GROWTH_KEY_BASE + growthBatchSequence++ * GROWTH_OPERATIONS_PER_INVOCATION;
        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = batchJobIdBase + index;
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

    /** Deletes the previous measured growth batch so the next iteration starts from baseline. */
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
        growthPhase = GrowthPhase.NONE;
    }

    private boolean shouldSampleGrowthDurability() {
        return growthPhase != GrowthPhase.NONE
                && (growthIterationIndex == 0
                        || growthIterationIndex % DURABLE_SAMPLE_INTERVAL == 0);
    }

    /** Checks every running-job growth entry is present in memory without a MapStore reload. */
    private void verifyRunningJobBatchResident() {
        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = batchJobIds[index];
            TaskGroupLocation taskGroupLocation = batchTaskGroupLocations[index];
            Long[] timestamps = runningJobStateTimestampsMap.get(taskGroupLocation);
            if (runningJobInfoMap.get(jobId) == null
                    || runningJobStateMap.get(taskGroupLocation) != ExecutionState.RUNNING
                    || timestamps == null
                    || timestamps[ExecutionState.RUNNING.ordinal()] == null) {
                throw new IllegalStateException(
                        "A running-job growth entry was not retained in memory");
            }
        }
    }

    /**
     * Checks every completed-job lifecycle left finished entries resident and cleared running
     * state.
     */
    private void verifyCompletedJobBatchResident() {
        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = batchJobIds[index];
            TaskGroupLocation taskGroupLocation = batchTaskGroupLocations[index];
            if (runningJobInfoMap.get(jobId) != null
                    || runningJobStateMap.get(taskGroupLocation) != null
                    || runningJobStateTimestampsMap.get(taskGroupLocation) != null
                    || finishedJobStateMap.get(jobId) == null
                    || finishedJobMetricsMap.get(jobId) == null) {
                throw new IllegalStateException(
                        "A completed-job lifecycle was not retained correctly");
            }
        }
    }

    /**
     * Reloads the current growth batch from MapStore. One {@code loadAll} already replays the full
     * WAL, so the sample verifies every key in the batch at the same durability cost as a single
     * key.
     */
    private void verifyGrowthBatchDurability() {
        if (batchJobIds == null || growthPhase == GrowthPhase.NONE) {
            return;
        }
        if (growthPhase == GrowthPhase.RUNNING) {
            verifyRunningJobBatchDurability();
        } else if (growthPhase == GrowthPhase.COMPLETED) {
            verifyCompletedJobBatchDurability();
        }
    }

    private void verifyRunningJobBatchDurability() {
        Collection<Long> jobIds = toLongKeyCollection(batchJobIds);
        Collection<Object> locations = batchLocationsAsObjects();

        reloadFromMapStore(runningJobInfoMap, jobIds);
        reloadFromMapStore(runningJobStateMap, locations);
        reloadFromMapStore(runningJobStateTimestampsMap, locations);

        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = batchJobIds[index];
            TaskGroupLocation taskGroupLocation = batchTaskGroupLocations[index];
            Long[] timestamps = runningJobStateTimestampsMap.get(taskGroupLocation);
            if (runningJobInfoMap.get(jobId) == null
                    || runningJobStateMap.get(taskGroupLocation) != ExecutionState.RUNNING
                    || timestamps == null
                    || timestamps[ExecutionState.RUNNING.ordinal()] == null) {
                throw new IllegalStateException(
                        "A running-job growth entry was not durably persisted");
            }
        }
    }

    private void verifyCompletedJobBatchDurability() {
        Collection<Long> jobIds = toLongKeyCollection(batchJobIds);

        reloadFromMapStore(finishedJobStateMap, jobIds);
        reloadFromMapStore(finishedJobMetricsMap, jobIds);

        for (int index = 0; index < GROWTH_OPERATIONS_PER_INVOCATION; index++) {
            long jobId = batchJobIds[index];
            TaskGroupLocation taskGroupLocation = batchTaskGroupLocations[index];
            if (runningJobInfoMap.get(jobId) != null
                    || runningJobStateMap.get(taskGroupLocation) != null
                    || runningJobStateTimestampsMap.get(taskGroupLocation) != null
                    || finishedJobStateMap.get(jobId) == null
                    || finishedJobMetricsMap.get(jobId) == null) {
                throw new IllegalStateException(
                        "A completed-job lifecycle was not durably persisted");
            }
        }
    }

    private static Collection<Long> toLongKeyCollection(long[] jobIds) {
        List<Long> keys = new ArrayList<>(jobIds.length);
        for (long jobId : jobIds) {
            keys.add(jobId);
        }
        return keys;
    }

    private Collection<Object> batchLocationsAsObjects() {
        List<Object> locations = new ArrayList<>(batchTaskGroupLocations.length);
        for (TaskGroupLocation location : batchTaskGroupLocations) {
            locations.add(location);
        }
        return locations;
    }

    private static <K, V> void reloadFromMapStore(IMap<K, V> map, Collection<? extends K> keys) {
        Set<K> keySet = new LinkedHashSet<>(keys);
        for (K key : keySet) {
            map.evict(key);
        }
        map.loadAll(keySet, true);
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
