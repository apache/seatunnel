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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/** Creates real checkpoint fixtures and exposes shared storage operations outside measured time. */
@State(Scope.Thread)
public class CheckpointStorageBenchmarkFixture {

    public static final int CHECKPOINT_OPERATIONS_PER_INVOCATION = 100;

    private static final Duration FIXTURE_CHECKPOINT_INTERVAL = Duration.ofSeconds(1);
    private static final int[] DURABILITY_SAMPLE_INDEXES = {
        0, CHECKPOINT_OPERATIONS_PER_INVOCATION / 2, CHECKPOINT_OPERATIONS_PER_INVOCATION - 1
    };

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
    private long expectedCompletedCount;

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
            PipelineCheckpointOverview pipelineOverview =
                    requiredPipelineOverview(fixtureOverview, fixtureState.getPipelineId());
            expectedCompletedCount = pipelineOverview.getCounts().getCompleted() + 1L;
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
                tearDown();
            } catch (Exception cleanupFailure) {
                setupFailure.addSuppressed(cleanupFailure);
            }
            throw setupFailure;
        }
    }

    CheckpointOperation[] createOperations() {
        CheckpointOperation[] operations =
                new CheckpointOperation[CHECKPOINT_OPERATIONS_PER_INVOCATION];
        long firstInvocation = sequence.getAndAdd(CHECKPOINT_OPERATIONS_PER_INVOCATION) + 1L;
        for (int index = 0; index < CHECKPOINT_OPERATIONS_PER_INVOCATION; index++) {
            long invocation = firstInvocation + index;
            long jobId = Long.MAX_VALUE - invocation;
            long checkpointId = fixtureState.getCheckpointId() + invocation * 2L;
            operations[index] =
                    new CheckpointOperation(
                            jobId,
                            checkpointId,
                            StateStoreCheckpointIDCounter.convertLongIntToBase64(
                                    jobId, fixtureState.getPipelineId()));
        }
        return operations;
    }

    StateStoreCheckpointIDCounter prepareCounter(CheckpointOperation operation) throws Exception {
        StateStoreCheckpointIDCounter counter =
                new StateStoreCheckpointIDCounter(
                        operation.jobId, fixtureState.getPipelineId(), counterStore);
        counter.start();
        counter.setCount(operation.checkpointId);
        return counter;
    }

    void prepareOverview(CheckpointOperation operation) throws Exception {
        CheckpointOverview overview = deepCopy(fixtureOverview);
        overview.setJobId(operation.jobId);
        requiredPipelineOverview(overview, fixtureState.getPipelineId());
        overviewStore.put(operation.jobId, overview);
    }

    CompletedCheckpoint createCompletedCheckpoint(CheckpointOperation operation) {
        return copyCompletedCheckpoint(operation.jobId, operation.checkpointId);
    }

    void storePreviousCheckpoint(CheckpointOperation operation) throws Exception {
        checkpointStorage.storeCheckPoint(
                toPipelineState(
                        copyCompletedCheckpoint(operation.jobId, operation.checkpointId - 1L)));
    }

    void storeCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
        checkpointStorage.storeCheckPoint(toPipelineState(checkpoint));
    }

    void updateOverview(CompletedCheckpoint checkpoint) {
        monitorService.onCheckpointCompleted(
                checkpoint, CheckpointMonitorService.calculateStateSize(checkpoint));
    }

    void reloadCounterSamples(CheckpointOperation[] operations) {
        Set<String> keys = new LinkedHashSet<>();
        for (int index : DURABILITY_SAMPLE_INDEXES) {
            keys.add(operations[index].counterKey);
            checkpointCounterMap.evict(operations[index].counterKey);
        }
        checkpointCounterMap.loadAll(keys, true);
    }

    void reloadOverviewSamples(CheckpointOperation[] operations) {
        Set<Long> keys = new LinkedHashSet<>();
        for (int index : DURABILITY_SAMPLE_INDEXES) {
            keys.add(operations[index].jobId);
            checkpointOverviewMap.evict(operations[index].jobId);
        }
        checkpointOverviewMap.loadAll(keys, true);
    }

    void validateCounter(CheckpointOperation operation, long allocatedCheckpointId) {
        if (allocatedCheckpointId != operation.checkpointId) {
            throw new IllegalStateException(
                    "Checkpoint counter returned "
                            + allocatedCheckpointId
                            + " instead of "
                            + operation.checkpointId);
        }

        Long storedCounter = counterStore.get(operation.counterKey);
        long expectedStoredCounter = operation.checkpointId + 1L;
        if (storedCounter == null || storedCounter != expectedStoredCounter) {
            throw new IllegalStateException(
                    "Checkpoint counter stored "
                            + storedCounter
                            + " instead of "
                            + expectedStoredCounter);
        }
    }

    void validateStoredCheckpoint(CheckpointOperation operation) throws Exception {
        PipelineState storedState =
                checkpointStorage.getCheckpoint(
                        Long.toString(operation.jobId),
                        Integer.toString(fixtureState.getPipelineId()),
                        Long.toString(operation.checkpointId));
        if (storedState == null
                || !Long.toString(operation.jobId).equals(storedState.getJobId())
                || storedState.getPipelineId() != fixtureState.getPipelineId()
                || storedState.getCheckpointId() != operation.checkpointId) {
            throw new IllegalStateException(
                    "Stored checkpoint metadata does not match checkpoint "
                            + operation.checkpointId);
        }

        CompletedCheckpoint storedCheckpoint =
                serializer.deserialize(storedState.getStates(), CompletedCheckpoint.class);
        if (storedCheckpoint.getJobId() != operation.jobId
                || storedCheckpoint.getPipelineId() != fixtureState.getPipelineId()
                || storedCheckpoint.getCheckpointId() != operation.checkpointId) {
            throw new IllegalStateException(
                    "Stored checkpoint payload does not match checkpoint "
                            + operation.checkpointId);
        }
    }

    void validateOverview(CheckpointOperation operation) {
        CheckpointOverview storedOverview = overviewStore.get(operation.jobId);
        if (storedOverview == null || storedOverview.getJobId() != operation.jobId) {
            throw new IllegalStateException(
                    "Checkpoint overview was not stored for job " + operation.jobId);
        }

        PipelineCheckpointOverview pipelineOverview =
                requiredPipelineOverview(storedOverview, fixtureState.getPipelineId());
        if (pipelineOverview.getCounts().getCompleted() != expectedCompletedCount) {
            throw new IllegalStateException(
                    "Checkpoint overview completed count was not incremented for checkpoint "
                            + operation.checkpointId);
        }

        CheckpointInfo latestCompleted = pipelineOverview.getLatestCompleted();
        CheckpointHistoryEntry latestHistory = pipelineOverview.getHistory().peekFirst();
        if (latestCompleted == null
                || latestCompleted.getCheckpointId() != operation.checkpointId
                || latestHistory == null
                || latestHistory.getJobId() != operation.jobId
                || latestHistory.getPipelineId() != fixtureState.getPipelineId()
                || latestHistory.getCheckpointInfo() == null
                || latestHistory.getCheckpointInfo().getCheckpointId() != operation.checkpointId) {
            throw new IllegalStateException(
                    "Checkpoint overview does not contain completed checkpoint "
                            + operation.checkpointId);
        }
    }

    void deleteCheckpoint(CheckpointOperation operation) {
        checkpointStorage.deleteCheckpoint(Long.toString(operation.jobId));
    }

    void removeOverview(CheckpointOperation operation) {
        overviewStore.remove(operation.jobId);
    }

    void removeCounter(CheckpointOperation operation) {
        counterStore.remove(operation.counterKey);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (fixtureJob != null) {
            fixtureJob.close();
            fixtureJob = null;
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

    private static PipelineCheckpointOverview requiredPipelineOverview(
            CheckpointOverview overview, int pipelineId) {
        PipelineCheckpointOverview pipelineOverview = overview.getPipelines().get(pipelineId);
        if (pipelineOverview == null) {
            throw new IllegalStateException(
                    "The checkpoint overview fixture does not contain pipeline " + pipelineId);
        }
        return pipelineOverview;
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

    static final class CheckpointOperation {

        private final long jobId;
        private final long checkpointId;
        private final String counterKey;

        private CheckpointOperation(long jobId, long checkpointId, String counterKey) {
            this.jobId = jobId;
            this.checkpointId = checkpointId;
            this.counterKey = counterKey;
        }
    }
}
