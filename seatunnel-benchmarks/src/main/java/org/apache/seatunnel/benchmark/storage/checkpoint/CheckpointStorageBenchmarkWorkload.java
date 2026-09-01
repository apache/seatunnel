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

import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.benchmark.storage.StorageLifecycleFixtureJob;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.HdfsStorage;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.map.IMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** Real checkpoint fixtures and isolated storage operations for the JMH entry point. */
@State(Scope.Thread)
public class CheckpointStorageBenchmarkWorkload {

    public static final int CHECKPOINT_OPERATIONS_PER_INVOCATION = 100;

    private static final Duration FIXTURE_CHECKPOINT_INTERVAL = Duration.ofSeconds(1);

    private final AtomicLong sequence = new AtomicLong();
    private final ProtoStuffSerializer serializer = new ProtoStuffSerializer();

    private StorageLifecycleFixtureJob fixtureJob;
    private PipelineState fixtureState;
    private CompletedCheckpoint fixtureCheckpoint;
    private CheckpointOverview fixtureOverview;
    private CheckpointStorage checkpointStorage;
    private CounterStateStore<String> counterStore;
    private CheckpointOverviewStateStore overviewStore;
    private CheckpointMonitorService monitorService;
    private IMap<String, Long> checkpointCounterMap;
    private IMap<Long, CheckpointOverview> checkpointOverviewMap;

    private long[] benchmarkJobIds;
    private long[] benchmarkCheckpointIds;
    private String[] benchmarkCounterKeys;
    private CompletedCheckpoint[] benchmarkCompletedCheckpoints;
    private StateStoreCheckpointIDCounter[] checkpointCounters;
    private long[] allocatedCheckpointIds;
    private int preparedOperationCount;
    private InvocationOperation invocationOperation = InvocationOperation.NONE;
    private long expectedCompletedCount;

    /** Generates all fixtures through a real Zeta job and one completed checkpoint. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) throws Exception {
        BenchmarkCheckpointStorageFactory.clearFixture();
        fixtureJob = new StorageLifecycleFixtureJob(environment, FIXTURE_CHECKPOINT_INTERVAL);
        try {
            fixtureJob.start();
            fixtureJob.awaitCompletedCheckpoint();
            fixtureState = BenchmarkCheckpointStorageFactory.latestFixture();
            fixtureCheckpoint = BenchmarkCheckpointStorageFactory.latestCompletedCheckpoint();
            fixtureOverview =
                    environment
                            .getServer()
                            .getCheckpointMonitorService()
                            .getOverview(fixtureJob.getJobId())
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "The checkpoint overview fixture is unavailable"));
            fixtureJob.finish();

            Map<String, String> checkpointPluginConfig =
                    new HashMap<>(
                            environment
                                    .storageConfig()
                                    .getEngineConfig()
                                    .getCheckpointConfig()
                                    .getStorage()
                                    .getStoragePluginConfig());
            checkpointStorage = new HdfsStorage(checkpointPluginConfig);
            counterStore = environment.getStateStores().checkpointCounterStore();
            overviewStore = environment.getStateStores().checkpointOverviewStateStore();
            monitorService = environment.getServer().getCheckpointMonitorService();
            checkpointCounterMap =
                    environment
                            .getServer()
                            .getNodeEngine()
                            .getHazelcastInstance()
                            .getMap(EngineStateStoreNames.CHECKPOINT_ID);
            checkpointOverviewMap =
                    environment
                            .getServer()
                            .getNodeEngine()
                            .getHazelcastInstance()
                            .getMap(EngineStateStoreNames.CHECKPOINT_MONITOR);
        } catch (Exception setupFailure) {
            try {
                fixtureJob.close();
            } catch (Exception cleanupFailure) {
                setupFailure.addSuppressed(cleanupFailure);
            }
            throw setupFailure;
        }
    }

    /** Prepares one fixed-size phase of unique, coordinator-produced checkpoint values. */
    @Setup(Level.Iteration)
    public void prepareIteration() throws Exception {
        invocationOperation = InvocationOperation.NONE;
        preparedOperationCount = 0;
        benchmarkJobIds = new long[CHECKPOINT_OPERATIONS_PER_INVOCATION];
        benchmarkCheckpointIds = new long[CHECKPOINT_OPERATIONS_PER_INVOCATION];
        benchmarkCounterKeys = new String[CHECKPOINT_OPERATIONS_PER_INVOCATION];
        benchmarkCompletedCheckpoints =
                new CompletedCheckpoint[CHECKPOINT_OPERATIONS_PER_INVOCATION];
        checkpointCounters =
                new StateStoreCheckpointIDCounter[CHECKPOINT_OPERATIONS_PER_INVOCATION];
        allocatedCheckpointIds = new long[CHECKPOINT_OPERATIONS_PER_INVOCATION];
        Arrays.fill(allocatedCheckpointIds, Long.MIN_VALUE);

        long firstInvocation = sequence.getAndAdd(CHECKPOINT_OPERATIONS_PER_INVOCATION) + 1L;
        for (int index = 0; index < CHECKPOINT_OPERATIONS_PER_INVOCATION; index++) {
            long invocation = firstInvocation + index;
            long benchmarkJobId = Long.MAX_VALUE - invocation;
            long benchmarkCheckpointId = fixtureState.getCheckpointId() + invocation * 2L;
            String benchmarkCounterKey =
                    StateStoreCheckpointIDCounter.convertLongIntToBase64(
                            benchmarkJobId, fixtureState.getPipelineId());

            StateStoreCheckpointIDCounter checkpointCounter =
                    new StateStoreCheckpointIDCounter(
                            benchmarkJobId, fixtureState.getPipelineId(), counterStore);
            checkpointCounter.start();
            checkpointCounter.setCount(benchmarkCheckpointId);

            CheckpointOverview overview = deepCopy(fixtureOverview);
            overview.setJobId(benchmarkJobId);
            PipelineCheckpointOverview pipelineOverview =
                    overview.getPipelines().get(fixtureState.getPipelineId());
            if (pipelineOverview == null) {
                throw new IllegalStateException(
                        "The checkpoint overview fixture does not contain pipeline "
                                + fixtureState.getPipelineId());
            }
            expectedCompletedCount = pipelineOverview.getCounts().getCompleted() + 1L;
            overviewStore.put(benchmarkJobId, overview);

            benchmarkJobIds[index] = benchmarkJobId;
            benchmarkCheckpointIds[index] = benchmarkCheckpointId;
            benchmarkCounterKeys[index] = benchmarkCounterKey;
            checkpointCounters[index] = checkpointCounter;
            benchmarkCompletedCheckpoints[index] =
                    copyCompletedCheckpoint(benchmarkJobId, benchmarkCheckpointId);
            checkpointStorage.storeCheckPoint(
                    toPipelineState(
                            copyCompletedCheckpoint(benchmarkJobId, benchmarkCheckpointId - 1L)));
            preparedOperationCount++;
        }
    }

    /** Validates production storage effects outside measured time, then removes the fixed phase. */
    @TearDown(Level.Iteration)
    public void validateAndCleanIteration() throws Exception {
        try {
            validateInvocation();
        } finally {
            for (int index = 0; index < preparedOperationCount; index++) {
                checkpointStorage.deleteCheckpoint(Long.toString(benchmarkJobIds[index]));
                overviewStore.remove(benchmarkJobIds[index]);
                counterStore.remove(benchmarkCounterKeys[index]);
            }
            invocationOperation = InvocationOperation.NONE;
            preparedOperationCount = 0;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (fixtureJob != null) {
            fixtureJob.close();
        }
    }

    public long incrementCheckpointId() throws Exception {
        invocationOperation = InvocationOperation.CHECKPOINT_ID_INCREMENT;
        for (int index = 0; index < CHECKPOINT_OPERATIONS_PER_INVOCATION; index++) {
            allocatedCheckpointIds[index] = checkpointCounters[index].getAndIncrement();
        }
        return allocatedCheckpointIds[CHECKPOINT_OPERATIONS_PER_INVOCATION - 1];
    }

    public void updateCheckpointOverview() {
        invocationOperation = InvocationOperation.CHECKPOINT_OVERVIEW_UPDATE;
        for (int index = 0; index < CHECKPOINT_OPERATIONS_PER_INVOCATION; index++) {
            updateCheckpointOverviewInternal(index);
        }
    }

    private void updateCheckpointOverviewInternal(int index) {
        CompletedCheckpoint checkpoint = benchmarkCompletedCheckpoints[index];
        monitorService.onCheckpointCompleted(
                checkpoint, CheckpointMonitorService.calculateStateSize(checkpoint));
    }

    /**
     * Serializes the completed checkpoint and persists it through the production storage plugin.
     */
    private void storeCheckpointResult(int index) throws Exception {
        checkpointStorage.storeCheckPoint(toPipelineState(benchmarkCompletedCheckpoints[index]));
    }

    public void persistCheckpointStorageTransaction() throws Exception {
        invocationOperation = InvocationOperation.CHECKPOINT_STORAGE_TRANSACTION;
        for (int index = 0; index < CHECKPOINT_OPERATIONS_PER_INVOCATION; index++) {
            allocatedCheckpointIds[index] = checkpointCounters[index].getAndIncrement();
            storeCheckpointResult(index);
            updateCheckpointOverviewInternal(index);
        }
    }

    private void validateInvocation() throws Exception {
        if (invocationOperation == InvocationOperation.NONE) {
            throw new IllegalStateException(
                    "No checkpoint storage benchmark operation was recorded");
        }
        if (preparedOperationCount != CHECKPOINT_OPERATIONS_PER_INVOCATION) {
            throw new IllegalStateException(
                    "Checkpoint storage benchmark phase was not fully prepared");
        }
        reloadDurableSamples();
        for (int index = 0; index < CHECKPOINT_OPERATIONS_PER_INVOCATION; index++) {
            if (invocationOperation.validatesCounter()) {
                validateCheckpointCounter(index);
            }
            if (invocationOperation.validatesCheckpointStorage()) {
                validateStoredCheckpoint(index);
            }
            if (invocationOperation.validatesOverview()) {
                validateCheckpointOverview(index);
            }
        }
    }

    private void reloadDurableSamples() {
        Set<String> counterKeys = new LinkedHashSet<>();
        Set<Long> overviewKeys = new LinkedHashSet<>();
        for (int index : sampleIndexes()) {
            if (invocationOperation.validatesCounter()) {
                counterKeys.add(benchmarkCounterKeys[index]);
                checkpointCounterMap.evict(benchmarkCounterKeys[index]);
            }
            if (invocationOperation.validatesOverview()) {
                overviewKeys.add(benchmarkJobIds[index]);
                checkpointOverviewMap.evict(benchmarkJobIds[index]);
            }
        }
        if (!counterKeys.isEmpty()) {
            checkpointCounterMap.loadAll(counterKeys, true);
        }
        if (!overviewKeys.isEmpty()) {
            checkpointOverviewMap.loadAll(overviewKeys, true);
        }
    }

    private static int[] sampleIndexes() {
        return new int[] {
            0, CHECKPOINT_OPERATIONS_PER_INVOCATION / 2, CHECKPOINT_OPERATIONS_PER_INVOCATION - 1
        };
    }

    private void validateCheckpointCounter(int index) {
        long benchmarkCheckpointId = benchmarkCheckpointIds[index];
        long allocatedCheckpointId = allocatedCheckpointIds[index];
        if (allocatedCheckpointId != benchmarkCheckpointId) {
            throw new IllegalStateException(
                    "Checkpoint counter returned "
                            + allocatedCheckpointId
                            + " instead of "
                            + benchmarkCheckpointId);
        }

        Long storedCounter = counterStore.get(benchmarkCounterKeys[index]);
        long expectedStoredCounter = benchmarkCheckpointId + 1L;
        if (storedCounter == null || storedCounter != expectedStoredCounter) {
            throw new IllegalStateException(
                    "Checkpoint counter stored "
                            + storedCounter
                            + " instead of "
                            + expectedStoredCounter);
        }
    }

    private void validateStoredCheckpoint(int index) throws Exception {
        long benchmarkJobId = benchmarkJobIds[index];
        long benchmarkCheckpointId = benchmarkCheckpointIds[index];
        PipelineState storedState =
                checkpointStorage.getCheckpoint(
                        Long.toString(benchmarkJobId),
                        Integer.toString(fixtureState.getPipelineId()),
                        Long.toString(benchmarkCheckpointId));
        if (storedState == null
                || !Long.toString(benchmarkJobId).equals(storedState.getJobId())
                || storedState.getPipelineId() != fixtureState.getPipelineId()
                || storedState.getCheckpointId() != benchmarkCheckpointId) {
            throw new IllegalStateException(
                    "Stored checkpoint metadata does not match checkpoint "
                            + benchmarkCheckpointId);
        }

        CompletedCheckpoint storedCheckpoint =
                serializer.deserialize(storedState.getStates(), CompletedCheckpoint.class);
        if (storedCheckpoint.getJobId() != benchmarkJobId
                || storedCheckpoint.getPipelineId() != fixtureState.getPipelineId()
                || storedCheckpoint.getCheckpointId() != benchmarkCheckpointId) {
            throw new IllegalStateException(
                    "Stored checkpoint payload does not match checkpoint " + benchmarkCheckpointId);
        }
    }

    private void validateCheckpointOverview(int index) {
        long benchmarkJobId = benchmarkJobIds[index];
        long benchmarkCheckpointId = benchmarkCheckpointIds[index];
        CheckpointOverview storedOverview = overviewStore.get(benchmarkJobId);
        if (storedOverview == null || storedOverview.getJobId() != benchmarkJobId) {
            throw new IllegalStateException(
                    "Checkpoint overview was not stored for job " + benchmarkJobId);
        }

        PipelineCheckpointOverview pipelineOverview =
                storedOverview.getPipelines().get(fixtureState.getPipelineId());
        if (pipelineOverview == null
                || pipelineOverview.getCounts().getCompleted() != expectedCompletedCount) {
            throw new IllegalStateException(
                    "Checkpoint overview completed count was not incremented for checkpoint "
                            + benchmarkCheckpointId);
        }

        CheckpointInfo latestCompleted = pipelineOverview.getLatestCompleted();
        CheckpointHistoryEntry latestHistory = pipelineOverview.getHistory().peekFirst();
        if (latestCompleted == null
                || latestCompleted.getCheckpointId() != benchmarkCheckpointId
                || latestHistory == null
                || latestHistory.getJobId() != benchmarkJobId
                || latestHistory.getPipelineId() != fixtureState.getPipelineId()
                || latestHistory.getCheckpointInfo() == null
                || latestHistory.getCheckpointInfo().getCheckpointId() != benchmarkCheckpointId) {
            throw new IllegalStateException(
                    "Checkpoint overview does not contain completed checkpoint "
                            + benchmarkCheckpointId);
        }
    }

    private CompletedCheckpoint copyCompletedCheckpoint(long jobId, long checkpointId) {
        return new CompletedCheckpoint(
                jobId,
                fixtureCheckpoint.getPipelineId(),
                checkpointId,
                fixtureCheckpoint.getCheckpointTimestamp(),
                fixtureCheckpoint.getCheckpointType(),
                fixtureCheckpoint.getCompletedTimestamp(),
                fixtureCheckpoint.getTaskStates(),
                fixtureCheckpoint.getTaskStatistics());
    }

    private PipelineState toPipelineState(CompletedCheckpoint checkpoint) {
        return PipelineState.builder()
                .jobId(Long.toString(checkpoint.getJobId()))
                .pipelineId(checkpoint.getPipelineId())
                .checkpointId(checkpoint.getCheckpointId())
                .states(serializer.serialize(checkpoint))
                .build();
    }

    private enum InvocationOperation {
        NONE(false, false, false),
        CHECKPOINT_STORAGE_TRANSACTION(true, true, true),
        CHECKPOINT_ID_INCREMENT(true, false, false),
        CHECKPOINT_OVERVIEW_UPDATE(false, false, true);

        private final boolean validatesCounter;
        private final boolean validatesCheckpointStorage;
        private final boolean validatesOverview;

        InvocationOperation(
                boolean validatesCounter,
                boolean validatesCheckpointStorage,
                boolean validatesOverview) {
            this.validatesCounter = validatesCounter;
            this.validatesCheckpointStorage = validatesCheckpointStorage;
            this.validatesOverview = validatesOverview;
        }

        private boolean validatesCounter() {
            return validatesCounter;
        }

        private boolean validatesCheckpointStorage() {
            return validatesCheckpointStorage;
        }

        private boolean validatesOverview() {
            return validatesOverview;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T deepCopy(T value) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            return (T) input.readObject();
        }
    }
}
