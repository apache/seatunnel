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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Slf4j
public class JobExecutionIT {

    private static HazelcastInstanceImpl hazelcastInstance;

    private static SeaTunnelConfig SEATUNNEL_CONFIG;

    @BeforeEach
    public void beforeClass() {
        SEATUNNEL_CONFIG = ConfigProvider.locateAndGetSeaTunnelConfig();
        SEATUNNEL_CONFIG
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(SEATUNNEL_CONFIG);
    }

    @Test
    public void testSayHello() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            String msg = "Hello world";
            String s = engineClient.printMessageToMaster(msg);
            Assertions.assertEquals(msg, s);
        }
    }

    @Test
    public void testExecuteJob() throws Exception {
        runJobFileWithAssertEndStatus(
                "batch_fakesource_to_file.conf", "fake_to_file", JobStatus.FINISHED);
    }

    private static void runJobFileWithAssertEndStatus(
            String confFile, String name, JobStatus finished)
            throws ExecutionException, InterruptedException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource(confFile);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            await().atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && finished.equals(
                                                            objectCompletableFuture.get())));
        }
    }

    @Test
    public void testExecuteJobWithLockMetrics() throws Exception {
        // lock metrics map
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap =
                hazelcastInstance.getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        metricsImap.lock(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
        try {
            runJobFileWithAssertEndStatus(
                    "batch_fakesource_to_file.conf", "fake_to_file", JobStatus.FINISHED);
        } finally {
            metricsImap.unlock(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
        }
    }

    @Test
    public void cancelJobTest() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("streaming_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            JobStatus jobStatus1 = clientJobProxy.getJobStatus();
            Assertions.assertFalse(jobStatus1.isEndState());
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Thread.sleep(1000);
            clientJobProxy.cancelJob();

            await().atMost(20000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.CANCELED.equals(
                                                            objectCompletableFuture.get())));
        }
    }

    @Test
    public void testGetErrorInfo() throws ExecutionException, InterruptedException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("batch_fakesource_to_console_error.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console_error");
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> completableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            await().atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(completableFuture.isDone()));

            JobResult result = clientJobProxy.getJobResultCache();
            Assertions.assertEquals(result.getStatus(), JobStatus.FAILED);
            Assertions.assertTrue(result.getError().startsWith("java.lang.NumberFormatException"));
        }
    }

    @Test
    public void testValidJobNameInJobConfig() throws ExecutionException, InterruptedException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("valid_job_name.conf");
        JobConfig jobConfig = new JobConfig();
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> completableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            await().atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(completableFuture.isDone()));
            String value = engineClient.getJobClient().listJobStatus(false);
            Assertions.assertTrue(value.contains("\"jobName\":\"valid_job_name\""));
        }
    }

    @Test
    public void testGetUnKnownJobID() {

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobProxy newClientJobProxy =
                    engineClient.createJobClient().getJobProxy(System.currentTimeMillis());
            CompletableFuture<JobStatus> waitForJobCompleteFuture =
                    CompletableFuture.supplyAsync(newClientJobProxy::waitForJobComplete);

            await().atMost(20000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.UNKNOWABLE, waitForJobCompleteFuture.get()));

            Assertions.assertEquals(
                    "UNKNOWABLE",
                    engineClient.getJobClient().getJobStatus(System.currentTimeMillis()));
        }
    }

    @Test
    public void testExpiredJobWasDeleted() throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("job_expire");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            Assertions.assertEquals(clientJobProxy.waitForJobComplete(), JobStatus.FINISHED);
            await().atMost(65, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.UNKNOWABLE, clientJobProxy.getJobStatus()));
        }
    }

    @AfterEach
    void afterClass() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    public void testLastCheckpointErrorJob() throws Exception {
        runJobFileWithAssertEndStatus(
                "batch_last_checkpoint_error.conf",
                "batch_last_checkpoint_error",
                JobStatus.FAILED);
    }
}
