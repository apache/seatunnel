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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("checkstyle:MagicNumber")
@DisabledOnOs(OS.WINDOWS)
public class SeaTunnelClientTest {

    private static SeaTunnelConfig SEATUNNEL_CONFIG = ConfigProvider.locateAndGetSeaTunnelConfig();
    private static HazelcastInstance INSTANCE;

    @BeforeAll
    public static void beforeClass() throws Exception {
        SEATUNNEL_CONFIG
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName("SeaTunnelClientTest"));
        INSTANCE =
                HazelcastInstanceFactory.newHazelcastInstance(
                        SEATUNNEL_CONFIG.getHazelcastConfig(),
                        Thread.currentThread().getName(),
                        new SeaTunnelNodeContext(ConfigProvider.locateAndGetSeaTunnelConfig()));
    }

    private SeaTunnelClient createSeaTunnelClient() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("SeaTunnelClientTest"));
        return new SeaTunnelClient(clientConfig);
    }

    @Test
    public void testSayHello() {
        String msg = "Hello world";
        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        String s = seaTunnelClient.printMessageToMaster(msg);
        Assertions.assertEquals(msg, s);
    }

    @Test
    public void testExecuteJob() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testExecuteJob");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();

        try {
            JobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                return clientJobProxy.waitForJobComplete();
                            });

            await().atMost(180000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.FINISHED.equals(
                                                            objectCompletableFuture.get())));

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @Test
    public void testGetJobState() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testGetJobState");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();

        try {
            JobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                return clientJobProxy.waitForJobComplete();
                            });
            long jobId = clientJobProxy.getJobId();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            jobClient.getJobDetailStatus(jobId).contains("RUNNING")
                                                    && jobClient
                                                            .listJobStatus(true)
                                                            .contains("RUNNING")));

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            jobClient.getJobDetailStatus(jobId).contains("FINISHED")
                                                    && jobClient
                                                            .listJobStatus(true)
                                                            .contains("FINISHED")));

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @Test
    public void testGetJobMetrics() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testGetJobMetrics");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();

        try {
            JobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                return clientJobProxy.waitForJobComplete();
                            });
            long jobId = clientJobProxy.getJobId();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            jobClient.getJobDetailStatus(jobId).contains("FINISHED")
                                                    && jobClient
                                                            .listJobStatus(true)
                                                            .contains("FINISHED")));

            String jobMetrics = jobClient.getJobMetrics(jobId);

            System.out.println(jobMetrics);

            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_COUNT));
            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_QPS));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_COUNT));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_QPS));

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @Test
    public void testGetRunningJobMetrics() throws ExecutionException, InterruptedException {
        Common.setDeployMode(DeployMode.CLUSTER);
        String filePath = TestUtils.getResource("/batch_fake_to_console.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console1");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();

        ClientJobProxy execute1 =
                seaTunnelClient.createExecutionContext(filePath, jobConfig).execute();
        long jobId1 = execute1.getJobId();

        execute1.waitForJobComplete();

        filePath = TestUtils.getResource("streaming_fake_to_console.conf");
        jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console2");
        ClientJobProxy execute2 =
                seaTunnelClient.createExecutionContext(filePath, jobConfig).execute();
        ClientJobProxy execute3 =
                seaTunnelClient.createExecutionContext(filePath, jobConfig).execute();

        long jobId2 = execute2.getJobId();
        long jobId3 = execute3.getJobId();

        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        jobClient.getJobStatus(jobId1).equals("FINISHED")
                                                && jobClient.getJobStatus(jobId2).equals("RUNNING")
                                                && jobClient
                                                        .getJobStatus(jobId3)
                                                        .equals("RUNNING")));

        System.out.println(jobClient.getRunningJobMetrics());

        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            String runningJobMetrics = jobClient.getRunningJobMetrics();
                            Assertions.assertTrue(
                                    runningJobMetrics.contains(jobId2 + "")
                                            && runningJobMetrics.contains(jobId3 + ""));
                        });

        jobClient.cancelJob(jobId2);
        jobClient.cancelJob(jobId3);
    }

    @Test
    public void testCancelJob() throws ExecutionException, InterruptedException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/streaming_fake_to_console.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testCancelJob");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();
        try {
            JobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            long jobId = clientJobProxy.getJobId();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING", jobClient.getJobStatus(jobId)));

            jobClient.cancelJob(jobId);

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "CANCELED", jobClient.getJobStatus(jobId)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @Test
    public void testGetJobInfo() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();

        try {
            JobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            long jobId = clientJobProxy.getJobId();

            // Running
            Assertions.assertNotNull(jobClient.getJobInfo(jobId));

            await().atMost(180000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(1000);
                                System.out.println(
                                        "======================job status:"
                                                + jobClient.getJobDetailStatus(jobId));
                                System.out.println(
                                        "======================list job status:"
                                                + jobClient.listJobStatus(true));
                                Assertions.assertTrue(
                                        jobClient.getJobDetailStatus(jobId).contains("FINISHED")
                                                && jobClient
                                                        .listJobStatus(true)
                                                        .contains("FINISHED"));
                            });
            // Finished
            JobDAGInfo jobInfo = jobClient.getJobInfo(jobId);
            Assertions.assertTrue(
                    StringUtils.isNotEmpty(new ObjectMapper().writeValueAsString(jobInfo)));

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @Test
    public void testSavePointAndRestoreWithSavePoint() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/streaming_fake_to_console.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("streaming_fake_to_console.conf");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();

        try {
            JobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            long jobId = clientJobProxy.getJobId();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING", jobClient.getJobStatus(jobId)));

            RetryUtils.retryWithException(
                    () -> {
                        jobClient.savePointJob(jobId);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> {
                                // If we do savepoint for a Job which initialization has not been
                                // completed yet, we will get an error.
                                // In this test case, we need retry savepoint.
                                return exception
                                        .getCause()
                                        .getMessage()
                                        .contains("Task not all ready, savepoint error");
                            },
                            Constant.OPERATION_RETRY_SLEEP));

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "FINISHED", jobClient.getJobStatus(jobId)));

            Thread.sleep(1000);
            seaTunnelClient.restoreExecutionContext(filePath, jobConfig, jobId).execute();

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING", jobClient.getJobStatus(jobId)));

            jobClient.cancelJob(jobId);

            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "CANCELED", jobClient.getJobStatus(jobId)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @AfterAll
    public static void after() {
        INSTANCE.shutdown();
    }
}
