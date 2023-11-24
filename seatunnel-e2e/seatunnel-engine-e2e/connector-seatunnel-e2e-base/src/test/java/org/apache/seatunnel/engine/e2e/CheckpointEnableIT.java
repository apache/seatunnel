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
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class CheckpointEnableIT extends TestSuiteBase {

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "depending on the engine, the logic for determining whether a checkpoint is enabled is different")
    public void testZetaCheckpointEnable(TestContainer container)
            throws IOException, InterruptedException {
        // checkpoint disable, log don't contains 'CHECKPOINT_TYPE'
        Container.ExecResult disableExecResult =
                container.executeJob("/batch_fakesource_to_console_checkpoint_disable.conf");
        Assertions.assertFalse(container.getServerLogs().contains("CHECKPOINT_TYPE"));
        Assertions.assertEquals(0, disableExecResult.getExitCode());

        // checkpoint enable, log contains 'CHECKPOINT_TYPE'
        Container.ExecResult enableExecResult =
                container.executeJob("/batch_fakesource_to_console_checkpoint_enable.conf");
        Assertions.assertTrue(container.getServerLogs().contains("CHECKPOINT_TYPE"));
        Assertions.assertEquals(0, enableExecResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
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
                container.executeJob("/batch_fakesource_to_console_checkpoint_enable.conf");
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
                container.executeJob("/batch_fakesource_to_console_checkpoint_enable.conf");
        // according to logs, if checkpoint.interval is configured, spark also ignores this
        // configuration
        Assertions.assertTrue(
                enableExecResult
                        .getStderr()
                        .contains("Ignoring non-Spark config property: checkpoint.interval"));
        Assertions.assertEquals(0, enableExecResult.getExitCode());
    }
}
