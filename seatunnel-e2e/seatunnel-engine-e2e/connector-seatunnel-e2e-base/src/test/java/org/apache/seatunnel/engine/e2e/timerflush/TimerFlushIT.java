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

package org.apache.seatunnel.engine.e2e.timerflush;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class TimerFlushIT {

    private static final String CLUSTER_NAME = "TimerFlushIT";

    private HazelcastInstanceImpl hazelcastInstance;
    private SeaTunnelConfig seaTunnelConfig;

    @BeforeEach
    void setUp() {
        TimerFlushTestSinkWriter.reset();
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    @AfterEach
    void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void testFlushTriggeredByEngineTimer() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("timer_flush_enabled.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("timer-flush-enabled");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));

        try (SeaTunnelClient client = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment env =
                    client.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = env.execute();
            CompletableFuture.supplyAsync(jobProxy::waitForJobComplete);

            // sink.flush.interval=2000ms; allow up to 15s for the first timer tick
            await().atMost(15, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            TimerFlushTestSinkWriter.FLUSH_COUNT.get() >= 1,
                                            "Expected at least one engine-timer-driven flush, but flushCount="
                                                    + TimerFlushTestSinkWriter.FLUSH_COUNT.get()));

            Assertions.assertFalse(
                    TimerFlushTestSinkWriter.FLUSHED_ROWS.isEmpty(),
                    "Flushed rows must not be empty after a timer flush");

            jobProxy.cancelJob();
        }
    }

    @Test
    void testNoFlushWhenTimerDisabled() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("timer_flush_disabled.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("timer-flush-disabled");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));

        try (SeaTunnelClient client = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment env =
                    client.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = env.execute();
            CompletableFuture.supplyAsync(jobProxy::waitForJobComplete);

            // Assert that flushCount stays at 0 for 8 continuous seconds.
            // No timer is registered when sink.flush.interval is unset, so nothing should leak.
            Awaitility.await()
                    .during(8, TimeUnit.SECONDS)
                    .atMost(9, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .until(() -> TimerFlushTestSinkWriter.FLUSH_COUNT.get() == 0);

            Assertions.assertTrue(
                    TimerFlushTestSinkWriter.FLUSHED_ROWS.isEmpty(),
                    "Flushed rows must remain empty when timer flush is disabled");

            jobProxy.cancelJob();
        }
    }
}
