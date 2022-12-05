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

package org.apache.seatunnel.engine.client;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;
import static org.awaitility.Awaitility.await;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("checkstyle:MagicNumber")
@DisabledOnOs(OS.WINDOWS)
public class SeaTunnelClientTest {

    private static SeaTunnelConfig SEATUNNEL_CONFIG = ConfigProvider.locateAndGetSeaTunnelConfig();
    private static HazelcastInstance INSTANCE;
    private static SeaTunnelClient CLIENT;

    @BeforeAll
    public static void beforeClass() throws Exception {
        SEATUNNEL_CONFIG.getHazelcastConfig().setClusterName(TestUtils.getClusterName("SeaTunnelClientTest"));
        INSTANCE = HazelcastInstanceFactory.newHazelcastInstance(SEATUNNEL_CONFIG.getHazelcastConfig(),
            Thread.currentThread().getName(),
            new SeaTunnelNodeContext(ConfigProvider.locateAndGetSeaTunnelConfig()));
    }

    @BeforeEach
    void setUp() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("SeaTunnelClientTest"));
        CLIENT = new SeaTunnelClient(clientConfig);
    }

    @Test
    public void testSayHello() {
        String msg = "Hello world";
        String s = CLIENT.printMessageToMaster(msg);
        Assertions.assertEquals(msg, s);
    }

    @Test
    public void testExecuteJob() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        JobExecutionEnvironment jobExecutionEnv = CLIENT.createExecutionContext(filePath, jobConfig);

        try {
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
                return clientJobProxy.waitForJobComplete();
            });

            await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(
                    objectCompletableFuture.isDone() && JobStatus.FINISHED.equals(objectCompletableFuture.get())));

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetJobState() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console");

        JobExecutionEnvironment jobExecutionEnv = CLIENT.createExecutionContext(filePath, jobConfig);

        try {
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
                return clientJobProxy.waitForJobComplete();
            });
            long jobId = clientJobProxy.getJobId();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(
                    CLIENT.getJobState(jobId).contains("RUNNING") && CLIENT.listJobStatus().contains("RUNNING")));

            await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(
                    CLIENT.getJobState(jobId).contains("FINISHED") && CLIENT.listJobStatus().contains("FINISHED")));

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetJobMetrics() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console");

        JobExecutionEnvironment jobExecutionEnv = CLIENT.createExecutionContext(filePath, jobConfig);

        try {
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
                return clientJobProxy.waitForJobComplete();
            });
            long jobId = clientJobProxy.getJobId();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(
                    CLIENT.getJobState(jobId).contains("FINISHED") && CLIENT.listJobStatus().contains("FINISHED")));

            String jobMetrics = CLIENT.getJobMetrics(jobId);

            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_COUNT));
            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_QPS));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_COUNT));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_QPS));

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() {
        CLIENT.close();
    }

    @AfterAll
    public static void after() {
        INSTANCE.shutdown();
    }
}
