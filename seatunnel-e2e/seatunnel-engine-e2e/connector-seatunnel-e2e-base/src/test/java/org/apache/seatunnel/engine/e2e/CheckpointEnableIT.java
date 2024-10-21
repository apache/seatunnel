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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnJre(value = JRE.JAVA_11, disabledReason = "slf4j jar conflict, we should fix it later")
public class CheckpointEnableIT extends TestSuiteBase {

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "depending on the engine, the logic for determining whether a checkpoint is enabled is different")
    public void testZetaBatchCheckpointEnable(TestContainer container)
            throws IOException, InterruptedException {
        // checkpoint disable, log don't contains 'checkpoint is disabled'
        Container.ExecResult disableExecResult =
                container.executeJob(
                        "/checkpoint-batch-disable-test-resources/batch_fakesource_to_localfile_checkpoint_disable.conf");
        Assertions.assertTrue(container.getServerLogs().contains("checkpoint is disabled"));
        Assertions.assertEquals(0, disableExecResult.getExitCode());
        // check sink file is right
        Container.ExecResult disableSinkFileExecResult =
                container.executeJob(
                        "/checkpoint-batch-disable-test-resources/sink_file_text_to_assert.conf");
        Assertions.assertEquals(0, disableSinkFileExecResult.getExitCode());

        // checkpoint enable, log contains 'checkpoint is enabled'
        Container.ExecResult enableExecResult =
                container.executeJob(
                        "/checkpoint-batch-enable-test-resources/batch_fakesource_to_localfile_checkpoint_enable.conf");
        Assertions.assertTrue(container.getServerLogs().contains("checkpoint is enabled"));
        Assertions.assertEquals(0, enableExecResult.getExitCode());
        // check sink file is right
        Container.ExecResult enableSinkFileExecResult =
                container.executeJob(
                        "/checkpoint-batch-enable-test-resources/sink_file_text_to_assert.conf");
        Assertions.assertEquals(0, enableSinkFileExecResult.getExitCode());

        // checkpoint disable and timeout = 10, but timeout is not supported in disable mode
        Container.ExecResult disableExecResult2 =
                container.executeJob(
                        "/checkpoint-batch-disable-test-resources/batch_fakesource_to_localfile_checkpoint_disable_withtimeout.conf");
        Assertions.assertEquals(0, disableExecResult2.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "depending on the engine, the logic for determining whether a checkpoint is enabled is different")
    public void testZetaStreamingCheckpointInterval(TestContainer container)
            throws IOException, InterruptedException, ExecutionException {
        // start job
        String jobId = JobIdGenerator.newJobId();
        CompletableFuture<Container.ExecResult> startFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/checkpoint-streaming-enable-test-resources/stream_fakesource_to_localfile_interval.conf",
                                        jobId);
                            } catch (Exception e) {
                                log.error("Commit task exception :" + e.getMessage());
                                throw new RuntimeException(e);
                            }
                        });

        // wait obtain job id
        Thread.sleep(15000);
        Assertions.assertTrue(container.getServerLogs().contains("checkpoint is enabled"));
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());
        Assertions.assertEquals(0, startFuture.get().getExitCode());
        // restore job
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.restoreJob(
                                "/checkpoint-streaming-enable-test-resources/stream_fakesource_to_localfile_interval.conf",
                                jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // check sink file is right
        AtomicReference<Boolean> checkSinkFile = new AtomicReference<>(false);
        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Container.ExecResult disableSinkFileExecResult =
                                    container.executeJob(
                                            "/checkpoint-streaming-enable-test-resources/sink_file_text_to_assert.conf");
                            checkSinkFile.set(0 == disableSinkFileExecResult.getExitCode());
                            Assertions.assertEquals(0, disableSinkFileExecResult.getExitCode());
                        });
        Assertions.assertTrue(checkSinkFile.get());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "depending on the engine, the logic for determining whether a checkpoint is enabled is different")
    public void testZetaStreamingCheckpointNoInterval(TestContainer container)
            throws IOException, InterruptedException {
        // start job
        String jobId = JobIdGenerator.newJobId();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/checkpoint-streaming-enable-test-resources/stream_fakesource_to_localfile.conf",
                                jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        Thread.sleep(15000);
        Assertions.assertTrue(container.getServerLogs().contains("checkpoint is enabled"));
        Assertions.assertEquals(0, container.savepointJob(jobId).getExitCode());

        // restore job
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container
                                .restoreJob(
                                        "/checkpoint-streaming-enable-test-resources/stream_fakesource_to_localfile.conf",
                                        jobId)
                                .getExitCode();
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // check sink file is right
        AtomicReference<Boolean> checkSinkFile = new AtomicReference<>(false);
        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Container.ExecResult disableSinkFileExecResult =
                                    container.executeJob(
                                            "/checkpoint-streaming-enable-test-resources/sink_file_text_to_assert.conf");
                            checkSinkFile.set(0 == disableSinkFileExecResult.getExitCode());
                            Assertions.assertEquals(0, disableSinkFileExecResult.getExitCode());
                        });
        Assertions.assertTrue(checkSinkFile.get());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.FLINK_1_17, TestContainerId.FLINK_1_18},
            type = {EngineType.SEATUNNEL, EngineType.SPARK},
            disabledReason =
                    "depending on the engine, the logic for determining whether a checkpoint is enabled is different")
    public void testFlinkCheckpointEnable(AbstractTestFlinkContainer container)
            throws IOException, InterruptedException {
        /**
         * In flink execution environment, checkpoint is not supported and not needed when executing
         * jobs in BATCH mode. So it is only necessary to determine whether flink has enabled
         * checkpoint by configuring tasks with 'checkpoint.interval'.
         */
        Container.ExecResult enableExecResult =
                container.executeJob(
                        "/checkpoint-batch-enable-test-resources/batch_fakesource_to_localfile_checkpoint_enable.conf");
        // obtain flink job configuration
        Matcher matcher =
                Pattern.compile("JobID\\s([a-fA-F0-9]+)").matcher(enableExecResult.getStdout());
        Assertions.assertTrue(matcher.find());
        String jobId = matcher.group(1);
        Map<String, Object> jobConfig =
                JsonUtils.toMap(
                        container.executeJobManagerInnerCommand(
                                String.format(
                                        "curl http://localhost:8081/jobs/%s/checkpoints/config",
                                        jobId)),
                        String.class,
                        Object.class);
        /**
         * when the checkpoint interval is 0x7fffffffffffffff, indicates that checkpoint is
         * disabled. reference {@link
         * org.apache.flink.runtime.jobgraph.JobGraph#isCheckpointingEnabled()}
         */
        Assertions.assertEquals(Long.MAX_VALUE, jobConfig.getOrDefault("interval", 0L));
        Assertions.assertEquals(0, enableExecResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SEATUNNEL, EngineType.FLINK},
            disabledReason =
                    "depending on the engine, the logic for determining whether a checkpoint is enabled is different")
    public void testSparkCheckpointEnable(TestContainer container)
            throws IOException, InterruptedException {
        /**
         * In spark execution environment, checkpoint is not supported and not needed when executing
         * jobs in BATCH mode. So it is only necessary to determine whether spark has enabled
         * checkpoint by configuring tasks with 'checkpoint.interval'.
         */
        Container.ExecResult enableExecResult =
                container.executeJob(
                        "/checkpoint-batch-enable-test-resources/batch_fakesource_to_localfile_checkpoint_enable.conf");
        // according to logs, if checkpoint.interval is configured, spark also ignores this
        // configuration
        Assertions.assertTrue(
                enableExecResult
                        .getStderr()
                        .contains("Ignoring non-Spark config property: checkpoint.interval"));
        Assertions.assertEquals(0, enableExecResult.getExitCode());
    }
}
