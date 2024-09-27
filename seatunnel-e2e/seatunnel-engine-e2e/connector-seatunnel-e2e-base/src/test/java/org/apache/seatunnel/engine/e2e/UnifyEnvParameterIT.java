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
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.collections4.CollectionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SEATUNNEL, EngineType.SPARK},
        disabledReason = "only flink adjusts the parameter configuration rules")
public class UnifyEnvParameterIT extends TestSuiteBase {

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel && chown -R flink /tmp/seatunnel");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @TestTemplate
    public void testUnifiedParam(AbstractTestFlinkContainer container)
            throws IOException, InterruptedException {
        genericTest(
                "/unify-env-param-test-resource/unify_env_param_fakesource_to_localfile.conf",
                container);
    }

    @TestTemplate
    public void testOutdatedParam(AbstractTestFlinkContainer container)
            throws IOException, InterruptedException {
        genericTest(
                "/unify-env-param-test-resource/outdated_env_param_fakesource_to_localfile.conf",
                container);
    }

    @TestTemplate
    public void testUnifiedFlinkTableEnvParam(AbstractTestFlinkContainer container) {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/unify-env-param-test-resource/unify_flink_table_env_param_fakesource_to_console.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        // wait obtain job id
        AtomicReference<String> jobId = new AtomicReference<>();
        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> jobInfo =
                                    JsonUtils.toMap(
                                            container.executeJobManagerInnerCommand(
                                                    "curl http://localhost:8081/jobs/overview"),
                                            String.class,
                                            Object.class);
                            List<Map<String, Object>> jobs =
                                    (List<Map<String, Object>>) jobInfo.get("jobs");
                            if (!CollectionUtils.isEmpty(jobs)) {
                                jobId.set(jobs.get(0).get("jid").toString());
                            }
                            Assertions.assertNotNull(jobId.get());
                        });

        // obtain job info
        AtomicReference<Map<String, Object>> jobInfoReference = new AtomicReference<>();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> jobInfo =
                                    JsonUtils.toMap(
                                            container.executeJobManagerInnerCommand(
                                                    String.format(
                                                            "curl http://localhost:8081/jobs/%s",
                                                            jobId.get())),
                                            String.class,
                                            Object.class);
                            // wait the job initialization is complete and enters the Running state
                            if (null != jobInfo && "RUNNING".equals(jobInfo.get("state"))) {
                                jobInfoReference.set(jobInfo);
                            }
                            Assertions.assertNotNull(jobInfoReference.get());
                        });
    }

    public void genericTest(String configPath, AbstractTestFlinkContainer container)
            throws IOException, InterruptedException {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(configPath);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
        // wait obtain job id
        AtomicReference<String> jobId = new AtomicReference<>();
        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> jobInfo =
                                    JsonUtils.toMap(
                                            container.executeJobManagerInnerCommand(
                                                    "curl http://localhost:8081/jobs/overview"),
                                            String.class,
                                            Object.class);
                            List<Map<String, Object>> jobs =
                                    (List<Map<String, Object>>) jobInfo.get("jobs");
                            if (!CollectionUtils.isEmpty(jobs)) {
                                jobId.set(jobs.get(0).get("jid").toString());
                            }
                            Assertions.assertNotNull(jobId.get());
                        });

        // obtain job info
        AtomicReference<Map<String, Object>> jobInfoReference = new AtomicReference<>();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> jobInfo =
                                    JsonUtils.toMap(
                                            container.executeJobManagerInnerCommand(
                                                    String.format(
                                                            "curl http://localhost:8081/jobs/%s",
                                                            jobId.get())),
                                            String.class,
                                            Object.class);
                            // wait the job initialization is complete and enters the Running state
                            if (null != jobInfo && "RUNNING".equals(jobInfo.get("state"))) {
                                jobInfoReference.set(jobInfo);
                            }
                            Assertions.assertNotNull(jobInfoReference.get());
                        });
        Map<String, Object> jobInfo = jobInfoReference.get();

        // obtain execution configuration
        Map<String, Object> jobConfig =
                JsonUtils.toMap(
                        container.executeJobManagerInnerCommand(
                                String.format(
                                        "curl http://localhost:8081/jobs/%s/config", jobId.get())),
                        String.class,
                        Object.class);
        Map<String, Object> executionConfig =
                (Map<String, Object>) jobConfig.get("execution-config");

        // obtain checkpoint configuration
        Map<String, Object> checkpointConfig =
                JsonUtils.toMap(
                        container.executeJobManagerInnerCommand(
                                String.format(
                                        "curl http://localhost:8081/jobs/%s/checkpoints/config",
                                        jobId.get())),
                        String.class,
                        Object.class);

        // obtain checkpoint storage
        AtomicReference<Map<String, Object>> completedCheckpointReference = new AtomicReference<>();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Map<String, Object> checkpointsInfo =
                                    JsonUtils.toMap(
                                            container.executeJobManagerInnerCommand(
                                                    String.format(
                                                            "curl http://localhost:8081/jobs/%s/checkpoints",
                                                            jobId.get())),
                                            String.class,
                                            Object.class);
                            Map<String, Object> latestCheckpoint =
                                    (Map<String, Object>) checkpointsInfo.get("latest");
                            // waiting for at least one checkpoint trigger
                            if (null != latestCheckpoint) {
                                completedCheckpointReference.set(
                                        (Map<String, Object>) latestCheckpoint.get("completed"));
                                Assertions.assertNotNull(completedCheckpointReference.get());
                            }
                        });
        /**
         * adjust the configuration of this {@link
         * org.apache.seatunnel.core.starter.flink.utils.ConfigKeyName} to use the 'flink.' and the
         * flink parameter name, and check whether the configuration takes effect
         */
        // PARALLELISM
        int parallelism = (int) executionConfig.get("job-parallelism");
        Assertions.assertEquals(1, parallelism);

        // MAX_PARALLELISM
        int maxParallelism = (int) jobInfo.get("maxParallelism");
        Assertions.assertEquals(5, maxParallelism);

        // CHECKPOINT_INTERVAL
        int interval = (int) checkpointConfig.get("interval");
        Assertions.assertEquals(10000, interval);

        // CHECKPOINT_MODE
        String mode = checkpointConfig.get("mode").toString();
        Assertions.assertEquals("exactly_once", mode);

        // CHECKPOINT_TIMEOUT
        int checkpointTimeout = (int) checkpointConfig.get("timeout");
        Assertions.assertEquals(600000, checkpointTimeout);

        // CHECKPOINT_DATA_URI
        String externalPath = completedCheckpointReference.get().get("external_path").toString();
        Assertions.assertTrue(externalPath.startsWith("file:/tmp/seatunnel/flink/checkpoints"));

        // MAX_CONCURRENT_CHECKPOINTS
        int maxConcurrent = (int) checkpointConfig.get("max_concurrent");
        Assertions.assertEquals(2, maxConcurrent);

        // CHECKPOINT_CLEANUP_MODE
        Map<String, Object> externalizationMap =
                (Map<String, Object>) checkpointConfig.get("externalization");
        boolean externalization = (boolean) externalizationMap.get("delete_on_cancellation");
        Assertions.assertTrue(externalization);

        // MIN_PAUSE_BETWEEN_CHECKPOINTS
        int minPause = (int) checkpointConfig.get("min_pause");
        Assertions.assertEquals(100, minPause);

        // FAIL_ON_CHECKPOINTING_ERRORS
        int tolerableFailedCheckpoints = (int) checkpointConfig.get("tolerable_failed_checkpoints");
        Assertions.assertEquals(5, tolerableFailedCheckpoints);

        // RESTART_STRATEGY / because the restart strategy is fixed-delay in config file, so don't
        // check failure-rate
        String restartStrategy = executionConfig.get("restart-strategy").toString();
        Assertions.assertTrue(restartStrategy.contains("fixed delay"));

        // RESTART_ATTEMPTS
        Assertions.assertTrue(restartStrategy.contains("2 restart attempts"));

        // RESTART_DELAY_BETWEEN_ATTEMPTS
        Assertions.assertTrue(restartStrategy.contains("fixed delay (1000 ms)"));

        // STATE_BACKEND
        String stateBackend = checkpointConfig.get("state_backend").toString();
        Assertions.assertTrue(stateBackend.contains("RocksDBStateBackend"));
    }
}
