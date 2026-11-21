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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.InMemoryCdcRestoreTestFixture;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.SavePointFailedException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.internal.serialization.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
public class SavePointTest extends AbstractSeaTunnelServerTest<SavePointTest> {
    public static String STREAM_CONF_PATH = "stream_fakesource_to_file_savepoint.conf";
    public static String STREAM_CONF_WITH_ERROR_PATH = "stream_fake_to_inmemory_with_error.conf";
    public static String STREAM_CONF_WITH_SLEEP_PATH = "stream_fake_to_inmemory_with_sleep.conf";
    public static String BATCH_CONF_PATH = "batch_fakesource_to_file.conf";

    @Test
    public void testSavePoint() throws InterruptedException {
        savePointAndRestore(false);
    }

    @Test
    public void testSavePointWithNotExistedJob() {
        CompletionException exception =
                Assertions.assertThrows(
                        CompletionException.class,
                        () -> server.getCoordinatorService().savePoint(1L).join());
        Assertions.assertInstanceOf(SavePointFailedException.class, exception.getCause());
        Assertions.assertEquals(
                "The job with id '1' not running, save point failed",
                exception.getCause().getMessage());
    }

    @Test
    public void testSavePointButJobGoingToFail() throws InterruptedException {
        long jobId = System.currentTimeMillis();
        startJob(jobId, STREAM_CONF_WITH_ERROR_PATH, false);
        Thread.sleep(2000L);
        PassiveCompletableFuture<Void> savepoint1 = server.getCoordinatorService().savePoint(jobId);
        PassiveCompletableFuture<Void> savepoint2 = server.getCoordinatorService().savePoint(jobId);
        PassiveCompletableFuture<Void> savepoint3 = server.getCoordinatorService().savePoint(jobId);
        int errorCount = 0;
        try {
            savepoint1.join();
        } catch (Exception e) {
            errorCount++;
        }
        try {
            savepoint2.join();
        } catch (Exception e) {
            errorCount++;
        }
        try {
            savepoint3.join();
        } catch (Exception e) {
            errorCount++;
        }
        Assertions.assertEquals(3, errorCount);
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.FAILED));
    }

    @Test
    public void testSavePointWithMultiTimeRequest() throws InterruptedException {
        long jobId = System.currentTimeMillis();
        startJob(jobId, STREAM_CONF_WITH_SLEEP_PATH, false);
        Thread.sleep(5000L);
        PassiveCompletableFuture<Void> savepoint1 = server.getCoordinatorService().savePoint(jobId);
        Thread.sleep(1000L);
        PendingCheckpoint pendingCheckpoint1 =
                server.getCoordinatorService()
                        .getJobMaster(jobId)
                        .getCheckpointManager()
                        .getCheckpointCoordinator(1)
                        .getSavepointPendingCheckpoint();
        PassiveCompletableFuture<Void> savepoint2 = server.getCoordinatorService().savePoint(jobId);
        Thread.sleep(1000L);
        PendingCheckpoint pendingCheckpoint2 =
                server.getCoordinatorService()
                        .getJobMaster(jobId)
                        .getCheckpointManager()
                        .getCheckpointCoordinator(1)
                        .getSavepointPendingCheckpoint();
        savepoint1.join();
        savepoint2.join();
        Assertions.assertSame(pendingCheckpoint1, pendingCheckpoint2);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.SAVEPOINT_DONE));
    }

    @Test
    public void testRestoreWithNoSavepointFile() {
        long jobId = System.currentTimeMillis();
        startJob(jobId, BATCH_CONF_PATH, true);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.FINISHED));
    }

    @Test
    @Disabled()
    public void testSavePointOnServerRestart() throws InterruptedException {
        savePointAndRestore(true);
    }

    public void savePointAndRestore(boolean needRestart) throws InterruptedException {
        String outPath = "/tmp/hive/warehouse/test3";

        long jobId = 823342L;
        FileUtils.createNewDir(outPath);

        // 1 Start a streaming mode job
        startJob(jobId, STREAM_CONF_PATH, false);

        // 2 Wait for the job to running and start outputting data
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                        .getJobStatus(jobId)
                                                        .equals(JobStatus.RUNNING)
                                                && FileUtils.getFileLineNumberFromDir(outPath)
                                                        > 10));

        // 3 start savePoint
        server.getCoordinatorService().savePoint(jobId);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JobStatus status = server.getCoordinatorService().getJobStatus(jobId);
                            Assertions.assertTrue(
                                    status == JobStatus.DOING_SAVEPOINT
                                            || status == JobStatus.SAVEPOINT_DONE);
                        });

        // 4 Wait for savePoint to complete
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.SAVEPOINT_DONE));

        Thread.sleep(1000);

        // restart Server
        if (needRestart) {
            this.restartServer();
        }

        Thread.sleep(1000);

        // 5 Resume from savePoint
        startJob(jobId, STREAM_CONF_PATH, true);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.RUNNING));

        // 6 Run long enough to ensure that the data write is complete
        Thread.sleep(30000);

        server.getCoordinatorService().cancelJob(jobId);

        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.CANCELED));

        // 7 Check the final data count
        Assertions.assertEquals(100, FileUtils.getFileLineNumberFromDir(outPath));

        Thread.sleep(1000);
    }

    @Test
    public void testSavePointAndRestoreCdcUnackedTailSplit() throws Exception {
        String runTag = "savepoint-cdc-" + UUID.randomUUID();
        InMemoryCdcRestoreTestFixture.RunCoordinationState runState =
                InMemoryCdcRestoreTestFixture.registerRun(runTag);
        long jobId = 13340001L + Math.abs(runTag.hashCode() % 100000);

        try {
            Data phase1Job = createCdcSavePointJobData(jobId, runTag, false, 60000L);
            server.getCoordinatorService()
                    .submitJob(jobId, phase1Job, false)
                    .get(60, TimeUnit.SECONDS);

            Assertions.assertTrue(
                    runState.awaitSplit0Acked(30, TimeUnit.SECONDS),
                    "Timed out waiting for enumerator to acknowledge split0");
            Assertions.assertTrue(
                    runState.awaitTailSplitWatermarksRecorded(30, TimeUnit.SECONDS),
                    "Timed out waiting for tail split watermarks in finishedUnackedSplits");
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            new HashSet<>(Arrays.asList(1L, 100L)),
                                            new HashSet<>(runState.getEmittedIds())));

            server.getCoordinatorService().savePoint(jobId).get(60, TimeUnit.SECONDS);
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.SAVEPOINT_DONE,
                                            server.getCoordinatorService().getJobStatus(jobId)));
            runState.releaseTailSplit();

            List<PipelineState> savedStates =
                    server.getCheckpointService()
                            .getCheckpointStorage()
                            .getAllCheckpoints(String.valueOf(jobId));
            Assertions.assertFalse(savedStates.isEmpty());

            Data phase2Job = createCdcSavePointJobData(jobId, runTag, true, 200L);
            server.getCoordinatorService()
                    .submitJob(jobId, phase2Job, true)
                    .get(60, TimeUnit.SECONDS);

            JobResult restoreResult =
                    server.getCoordinatorService()
                            .waitForJobComplete(jobId)
                            .get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    JobStatus.FINISHED,
                    restoreResult.getStatus(),
                    () -> "CDC savepoint restore failed:\n" + restoreResult.getError());

            List<Long> emitted = new ArrayList<>(runState.getEmittedIds());
            Assertions.assertEquals(4, emitted.size(), "Unexpected row count: " + emitted);
            Assertions.assertEquals(
                    4,
                    new HashSet<>(emitted).size(),
                    "Duplicate primary keys emitted across restore: " + emitted);
            Assertions.assertEquals(
                    Arrays.asList(1L, 100L, 200L, 300L),
                    emitted.stream().sorted().collect(Collectors.toList()));
        } finally {
            runState.releaseTailSplit();
            try {
                JobStatus status = server.getCoordinatorService().getJobStatus(jobId);
                if (status != null && !status.isEndState()) {
                    server.getCoordinatorService().cancelJob(jobId).get(30, TimeUnit.SECONDS);
                }
            } catch (Exception ignored) {
            }
            InMemoryCdcRestoreTestFixture.unregisterRun(runTag);
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            0,
                                            server.getSlotService()
                                                    .getWorkerProfile()
                                                    .getAssignedSlots()
                                                    .length));
            FileUtils.deleteFile("/tmp/seatunnel/checkpoint_snapshot/" + jobId);
        }
    }

    private Data createCdcSavePointJobData(
            long jobId, String runTag, boolean isStartWithSavePoint, long checkpointIntervalMs) {
        JobContext jobContext = new JobContext(jobId);
        jobContext.setJobMode(JobMode.STREAMING);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("savepoint-cdc-13340-" + runTag);
        jobConfig.setJobContext(jobContext);
        Map<String, Object> envOptions = jobConfig.getEnvOptions();
        envOptions.put(EnvCommonOptions.JOB_MODE.key(), JobMode.STREAMING.name());
        envOptions.put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(), checkpointIntervalMs);
        envOptions.put(EnvCommonOptions.CHECKPOINT_TIMEOUT.key(), 30000L);
        envOptions.put(EnvCommonOptions.CHECKPOINT_MIN_PAUSE.key(), 10L);
        envOptions.put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 0);
        envOptions.put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), 0);

        IdGenerator idGenerator = new IdGenerator();
        CatalogTable catalogTable = InMemoryCdcRestoreTestFixture.createCatalogTable();

        IncrementalSource<SeaTunnelRow, SourceConfig> cdcSource =
                InMemoryCdcRestoreTestFixture.createSource(
                        runTag, catalogTable, false, StopMode.LATEST, !isStartWithSavePoint);
        cdcSource.setJobContext(jobContext);
        Action sourceAction =
                new SourceAction<>(
                        idGenerator.getNextId(),
                        "cdc-source",
                        cdcSource,
                        Collections.emptySet(),
                        Collections.emptySet());
        sourceAction.setParallelism(2);
        LogicalVertex sourceVertex = new LogicalVertex(sourceAction.getId(), sourceAction, 2);

        AbstractSimpleSink<SeaTunnelRow, Void> sink =
                InMemoryCdcRestoreTestFixture.createCollectingSink(runTag, catalogTable);
        sink.setJobContext(jobContext);
        Action sinkAction =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        "collecting-sink",
                        sink,
                        Collections.emptySet(),
                        Collections.emptySet());
        sinkAction.setParallelism(2);
        LogicalVertex sinkVertex = new LogicalVertex(sinkAction.getId(), sinkAction, 2);

        LogicalDag logicalDag = new LogicalDag(jobConfig, idGenerator);
        logicalDag.addLogicalVertex(sourceVertex);
        logicalDag.addLogicalVertex(sinkVertex);
        logicalDag.addEdge(new LogicalEdge(sourceVertex, sinkVertex));

        JobImmutableInformation jobInfo =
                new JobImmutableInformation(
                        jobId,
                        jobConfig.getName(),
                        isStartWithSavePoint,
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        return nodeEngine.getSerializationService().toData(jobInfo);
    }
}
