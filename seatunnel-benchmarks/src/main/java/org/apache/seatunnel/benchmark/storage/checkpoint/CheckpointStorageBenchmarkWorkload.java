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
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.common.statestore.checkpoint.CheckpointOverviewStateStore;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Real checkpoint fixtures and isolated storage operations for the JMH entry point. */
@State(Scope.Thread)
public class CheckpointStorageBenchmarkWorkload {

    private static final Duration FIXTURE_CHECKPOINT_INTERVAL = Duration.ofSeconds(1);

    private final AtomicLong sequence = new AtomicLong();

    private StorageLifecycleFixtureJob fixtureJob;
    private PipelineState fixtureState;
    private CompletedCheckpoint fixtureCheckpoint;
    private CheckpointOverview fixtureOverview;
    private CheckpointStorage checkpointStorage;
    private CounterStateStore<String> counterStore;
    private CheckpointOverviewStateStore overviewStore;
    private CheckpointMonitorService monitorService;

    private long benchmarkJobId;
    private long benchmarkCheckpointId;
    private String benchmarkCounterKey;
    private PipelineState benchmarkState;
    private CompletedCheckpoint benchmarkCompletedCheckpoint;
    private StateStoreCheckpointIDCounter checkpointCounter;

    /** Generates all fixtures through a real Zeta job and one completed checkpoint. */
    @Setup(Level.Trial)
    public void setUp(SeaTunnelStorageEnvironmentContext environment) throws Exception {
        BenchmarkCheckpointStorageFactory.clearFixture();
        fixtureJob = new StorageLifecycleFixtureJob(environment, FIXTURE_CHECKPOINT_INTERVAL);
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
    }

    /** Prepares unique keys and real-value copies before each measured storage operation. */
    @Setup(Level.Invocation)
    public void prepareInvocation() throws Exception {
        long invocation = sequence.incrementAndGet();
        benchmarkJobId = Long.MAX_VALUE - invocation;
        benchmarkCheckpointId = fixtureState.getCheckpointId() + invocation * 2;
        benchmarkCounterKey =
                StateStoreCheckpointIDCounter.convertLongIntToBase64(
                        benchmarkJobId, fixtureState.getPipelineId());

        checkpointCounter =
                new StateStoreCheckpointIDCounter(
                        benchmarkJobId, fixtureState.getPipelineId(), counterStore);
        checkpointCounter.start();
        checkpointCounter.setCount(benchmarkCheckpointId);

        CheckpointOverview overview = deepCopy(fixtureOverview);
        overview.setJobId(benchmarkJobId);
        overviewStore.put(benchmarkJobId, overview);

        benchmarkState =
                PipelineState.builder()
                        .jobId(Long.toString(benchmarkJobId))
                        .pipelineId(fixtureState.getPipelineId())
                        .checkpointId(benchmarkCheckpointId)
                        .states(fixtureState.getStates().clone())
                        .build();
        checkpointStorage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(benchmarkState.getJobId())
                        .pipelineId(benchmarkState.getPipelineId())
                        .checkpointId(benchmarkCheckpointId - 1L)
                        .states(benchmarkState.getStates().clone())
                        .build());
        benchmarkCompletedCheckpoint =
                new CompletedCheckpoint(
                        benchmarkJobId,
                        fixtureCheckpoint.getPipelineId(),
                        benchmarkCheckpointId,
                        fixtureCheckpoint.getCheckpointTimestamp(),
                        fixtureCheckpoint.getCheckpointType(),
                        fixtureCheckpoint.getCompletedTimestamp(),
                        fixtureCheckpoint.getTaskStates(),
                        fixtureCheckpoint.getTaskStatistics());
    }

    @TearDown(Level.Invocation)
    public void cleanInvocation() {
        checkpointStorage.deleteCheckpoint(benchmarkState.getJobId());
        overviewStore.remove(benchmarkJobId);
        counterStore.remove(benchmarkCounterKey);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (fixtureJob != null) {
            fixtureJob.close();
        }
    }

    public long incrementCheckpointId() throws Exception {
        return checkpointCounter.getAndIncrement();
    }

    public void updateCheckpointOverview() {
        monitorService.onCheckpointCompleted(
                benchmarkCompletedCheckpoint,
                CheckpointMonitorService.calculateStateSize(benchmarkCompletedCheckpoint));
    }

    public String storeCheckpointResult() throws Exception {
        return checkpointStorage.storeCheckPoint(benchmarkState);
    }

    public void persistCheckpointStorageTransaction() throws Exception {
        long checkpointId = incrementCheckpointId();
        if (checkpointId != benchmarkCheckpointId) {
            throw new IllegalStateException("Unexpected checkpoint counter value " + checkpointId);
        }
        updateCheckpointOverview();
        storeCheckpointResult();
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
