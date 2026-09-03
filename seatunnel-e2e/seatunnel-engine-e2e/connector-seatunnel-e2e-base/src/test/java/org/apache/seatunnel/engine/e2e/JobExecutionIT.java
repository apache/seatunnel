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
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
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

            await().atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && finished.equals(
                                                            objectCompletableFuture.get())));
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
            JobStatus jobStatus = clientJobProxy.getJobStatus();
            Assertions.assertFalse(
                    jobStatus.isEndState(), "Job should not be in end state: " + jobStatus);

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Thread.sleep(1000);
            clientJobProxy.cancelJob();

            await().atMost(20000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, objectCompletableFuture.get());
                            });
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
            await().atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(completableFuture.isDone()));

            JobResult result = clientJobProxy.getJobResultCache();
            Assertions.assertEquals(result.getStatus(), JobStatus.FAILED);
            Assertions.assertTrue(result.getError().contains("java.lang.NumberFormatException"));
        }
    }

    /**
     * Regression test for the FAILED-pipeline metrics leak fixed by <a
     * href="https://github.com/apache/seatunnel/pull/10757">#10757</a> ("[Fix][Zeta] Clean failed
     * pipeline metrics without full-map cleanup scan"). Before that fix, {@code
     * CoordinatorService#shouldCleanup} and {@code JobMaster#removeMetricsContext} only matched
     * {@code PipelineStatus.CANCELED}/{@code FINISHED}, so a pipeline that ended in {@code FAILED}
     * never had its entry removed from the coordinator's metrics {@code IMap}: it leaked there for
     * the remaining life of the cluster, once per failed pipeline.
     *
     * <p>This reuses the same deterministic {@code NumberFormatException} failure as {@link
     * #testGetErrorInfo()} (a SQL transform casting a random string to {@code int}), which fails
     * the pipeline almost immediately after task deployment. That speed makes it unsafe to rely on
     * the worker's real metrics-report cycle ({@code job-metrics-backup-interval}, 10 seconds by
     * default) to populate the coordinator's metrics {@code IMap} before the pipeline ends: the
     * pipeline can fail and have its task group context torn down before the next scheduled report
     * ever fires, which would make "no metrics after FAILED" trivially true and prove nothing about
     * the fix. Instead, this test seeds one metrics entry for the pipeline directly through {@link
     * SeaTunnelServer#updateMetrics}, the exact entry point the worker's periodic reporter itself
     * calls (via {@code ReportMetricsOperation}). From the coordinator cleanup logic's point of
     * view, a seeded entry is indistinguishable from a worker-reported one: {@code
     * CoordinatorService#shouldCleanup} and {@code #processPendingPipelineCleanup} match purely on
     * {@link PipelineLocation} and the pipeline's terminal status recorded by the real job, not on
     * how the metrics entry was populated.
     *
     * <p>Cleanup can complete either through the immediate best-effort call in {@code
     * SubPlan#subPlanDone}, or, if that racing attempt lands before this test seeds the entry,
     * through the 60-second scheduled {@code CoordinatorService#cleanupPendingPipelines} safety net
     * introduced by <a href="https://github.com/apache/seatunnel/pull/10418">#10418</a>. This test
     * does not need to know which one fires; it only asserts the end-to-end guarantee that the
     * entry does not survive forever.
     */
    @Test
    public void testFailedPipelineMetricsAreCleanedUp()
            throws ExecutionException, InterruptedException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("batch_fakesource_to_console_error.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console_error_cleanup");
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName("JobExecutionIT"));
        try (SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig)) {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            // A config with no shuffle/split boundary always compiles down to exactly one
            // pipeline, whose id is always 1 (see ExecutionPlanGenerator#normalizePipeline), so
            // the location can be constructed directly instead of racing the failing pipeline to
            // read it off the JobMaster before its bookkeeping is torn down.
            PipelineLocation pipelineLocation = new PipelineLocation(clientJobProxy.getJobId(), 1);

            SeaTunnelServer seaTunnelServer =
                    hazelcastInstance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

            // Sentinel task group id 999: this entry stands in for whatever a worker would have
            // reported for this pipeline. It is deliberately not tied to any of the job's real
            // task groups so seeding it cannot race, or depend on, the job's own execution.
            TaskGroupLocation seededTaskGroupLocation =
                    new TaskGroupLocation(
                            pipelineLocation.getJobId(), pipelineLocation.getPipelineId(), 999L);
            TaskLocation seededTaskLocation = new TaskLocation(seededTaskGroupLocation, 0, 0);
            Map<TaskLocation, SeaTunnelMetricsContext> seededMetrics = new HashMap<>();
            seededMetrics.put(seededTaskLocation, new SeaTunnelMetricsContext());
            seaTunnelServer.updateMetrics(seededMetrics);
            Assertions.assertTrue(
                    seaTunnelServer
                            .getEngineContext()
                            .getStateStores()
                            .metricsSnapshotStore()
                            .containsPipeline(pipelineLocation),
                    "Sanity check: seeded metrics entry must be visible before asserting cleanup");

            CompletableFuture<JobStatus> completableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            await().atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(completableFuture.isDone()));

            JobResult result = clientJobProxy.getJobResultCache();
            Assertions.assertEquals(JobStatus.FAILED, result.getStatus());

            // Bound generously above CoordinatorService#PIPELINE_CLEANUP_INTERVAL_SECONDS (60s):
            // the scheduled safety net's first run can land anywhere in that window relative to
            // when this pipeline actually failed.
            await().atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            seaTunnelServer
                                                    .getEngineContext()
                                                    .getStateStores()
                                                    .metricsSnapshotStore()
                                                    .containsPipeline(pipelineLocation),
                                            "Metrics for a FAILED pipeline must eventually be "
                                                    + "removed from the coordinator's metrics "
                                                    + "IMap, not retained forever"));
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
            await().atMost(300000, TimeUnit.MILLISECONDS)
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
