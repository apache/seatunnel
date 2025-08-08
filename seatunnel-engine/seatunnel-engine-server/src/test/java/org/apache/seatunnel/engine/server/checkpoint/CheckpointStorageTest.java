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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.CheckpointService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
public class CheckpointStorageTest extends AbstractSeaTunnelServerTest {

    public static String STREAM_CONF_PATH = "stream_fake_to_console_biginterval.conf";
    public static String BATCH_CONF_PATH = "batch_fakesource_to_file.conf";
    public static String BATCH_CONF_WITH_CHECKPOINT_PATH =
            "batch_fakesource_to_file_with_checkpoint.conf";
    public static String BATCH_CONF_WITHOUT_CHECKPOINT_INTERVAL_PATH =
            "batch_fake_to_console_without_checkpoint_interval.conf";

    public static String STREAM_CONF_WITH_CHECKPOINT_PATH =
            "stream_fake_to_console_with_checkpoint.conf";

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        CheckpointConfig checkpointConfig = seaTunnelConfig.getEngineConfig().getCheckpointConfig();
        // set a big interval in here and config file to avoid auto trigger checkpoint affect
        // test result
        checkpointConfig.setCheckpointInterval(Integer.MAX_VALUE);
        seaTunnelConfig.getEngineConfig().setCheckpointConfig(checkpointConfig);
        return seaTunnelConfig;
    }

    @Test
    public void testGenerateFileWhenSavepoint()
            throws CheckpointStorageException, InterruptedException {
        long jobId = System.currentTimeMillis();

        CheckpointStorage checkpointStorage = server.getCheckpointService().getCheckpointStorage();
        startJob(jobId, STREAM_CONF_PATH, false);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                .getJobStatus(jobId)
                                                .equals(JobStatus.RUNNING)));
        Thread.sleep(1000);
        CompletableFuture<Boolean> future1 =
                server.getCoordinatorService().getJobMaster(jobId).savePoint();
        future1.join();
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.SAVEPOINT_DONE));
        List<PipelineState> savepoint1 = checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        Assertions.assertEquals(1, savepoint1.size());
    }

    @Test
    public void testBatchJob() throws CheckpointStorageException {
        long jobId = System.currentTimeMillis();

        CheckpointStorage checkpointStorage = server.getCheckpointService().getCheckpointStorage();
        startJob(jobId, BATCH_CONF_PATH, false);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.FINISHED));
        List<PipelineState> allCheckpoints =
                checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        Assertions.assertEquals(0, allCheckpoints.size());
    }

    @Test
    public void testBatchJobWithCheckpoint() throws CheckpointStorageException {
        long jobId = System.currentTimeMillis();
        CheckpointConfig checkpointConfig =
                server.getSeaTunnelConfig().getEngineConfig().getCheckpointConfig();
        server.getSeaTunnelConfig().getEngineConfig().setCheckpointConfig(checkpointConfig);

        CheckpointStorage checkpointStorage = server.getCheckpointService().getCheckpointStorage();
        startJob(jobId, BATCH_CONF_WITH_CHECKPOINT_PATH, false);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.FINISHED));
        List<PipelineState> allCheckpoints =
                checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        Assertions.assertEquals(0, allCheckpoints.size());
    }

    @Test
    public void testStreamJobWithCancel() throws CheckpointStorageException, InterruptedException {
        long jobId = System.currentTimeMillis();
        CheckpointConfig checkpointConfig =
                server.getSeaTunnelConfig().getEngineConfig().getCheckpointConfig();
        server.getSeaTunnelConfig().getEngineConfig().setCheckpointConfig(checkpointConfig);

        CheckpointStorage checkpointStorage = server.getCheckpointService().getCheckpointStorage();
        startJob(jobId, STREAM_CONF_WITH_CHECKPOINT_PATH, false);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.RUNNING));
        // wait for checkpoint
        Thread.sleep(10 * 1000);
        server.getCoordinatorService().getJobMaster(jobId).cancelJob();
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.CANCELED));
        List<PipelineState> allCheckpoints =
                checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        Assertions.assertEquals(0, allCheckpoints.size());
    }

    @Test
    public void testBatchJobResetCheckpointStorage() throws CheckpointStorageException {
        long jobId = System.currentTimeMillis();
        CheckpointConfig checkpointConfig =
                server.getSeaTunnelConfig().getEngineConfig().getCheckpointConfig();
        server.getSeaTunnelConfig().getEngineConfig().setCheckpointConfig(checkpointConfig);
        final CheckpointStorage originalCheckpointStorage =
                server.getCheckpointService().getCheckpointStorage();

        // access checkpoint storage counter
        AtomicInteger accessCounter = new AtomicInteger(0);
        CheckpointStorage checkpointStorage =
                new CheckpointStorage() {
                    @Override
                    public String storeCheckPoint(PipelineState pipelineState)
                            throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                        return "";
                    }

                    @Override
                    public void asyncStoreCheckPoint(PipelineState pipelineState)
                            throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                    }

                    @Override
                    public List<PipelineState> getAllCheckpoints(String s)
                            throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                        return Collections.emptyList();
                    }

                    @Override
                    public List<PipelineState> getLatestCheckpoint(String s)
                            throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                        return Collections.emptyList();
                    }

                    @Override
                    public PipelineState getLatestCheckpointByJobIdAndPipelineId(
                            String s, String s1) throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                        return null;
                    }

                    @Override
                    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(
                            String s, String s1) throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                        return Collections.emptyList();
                    }

                    @Override
                    public void deleteCheckpoint(String s) {
                        accessCounter.incrementAndGet();
                    }

                    @Override
                    public PipelineState getCheckpoint(String s, String s1, String s2)
                            throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                        return null;
                    }

                    @Override
                    public void deleteCheckpoint(String s, String s1, String s2)
                            throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                    }

                    @Override
                    public void deleteCheckpoint(String s, String s1, List<String> list)
                            throws CheckpointStorageException {
                        accessCounter.incrementAndGet();
                    }
                };

        // replace the checkpoint storage reused by the system
        CheckpointService checkpointService = server.getCheckpointService();
        ReflectionUtils.setField(checkpointService, "checkpointStorage", checkpointStorage);

        startJob(jobId, BATCH_CONF_WITHOUT_CHECKPOINT_INTERVAL_PATH, false);
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        server.getCoordinatorService().getJobStatus(jobId),
                                        JobStatus.FINISHED));

        checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        Assertions.assertEquals(1, accessCounter.get());

        // restore the server's checkpointStorage to avoid affecting other unit cases
        ReflectionUtils.setField(checkpointService, "checkpointStorage", originalCheckpointStorage);
    }
}
