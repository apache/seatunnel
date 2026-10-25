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
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.restassured.response.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;

@Slf4j
public class PendingJobsRestIT {

    private static final String HOST = "http://localhost:";
    private static final String JOB_FILE = "pending_jobs_streaming.conf";

    private HazelcastInstanceImpl node;
    private SeaTunnelClient engineClient;
    private SeaTunnelConfig seaTunnelConfig;
    private final List<ClientJobProxy> submittedJobs = new ArrayList<>();
    private int httpPort;

    @BeforeEach
    void setUp() throws Exception {
        String testClusterName = TestUtils.getClusterName("PendingJobsRestIT");
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(2);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnableDynamicPort(false);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(18082);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setContextPath("/seatunnel");
        httpPort = seaTunnelConfig.getEngineConfig().getHttpConfig().getPort();

        node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

        Common.setDeployMode(DeployMode.CLIENT);
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
    }

    @AfterEach
    void tearDown() {
        submittedJobs.forEach(
                job -> {
                    try {
                        job.cancelJob();
                    } catch (Exception e) {
                        log.warn("Failed to cancel job {}: {}", job.getJobId(), e.getMessage());
                    }
                });
        submittedJobs.clear();
        if (engineClient != null) {
            engineClient.close();
        }
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void testPendingJobsEndpoint() {
        String jobName = "pending_waiting_job";
        ClientJobProxy pendingJob = submitStreamingJob(jobName);
        waitForStatus(pendingJob, JobStatus.PENDING);

        assertPendingJobVisible(pendingJob.getJobId(), jobName, JobStatus.PENDING);
    }

    private ClientJobProxy submitStreamingJob(String jobName) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        String filePath = TestUtils.getResource(JOB_FILE);
        ClientJobExecutionEnvironment env =
                engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
        ClientJobProxy jobProxy;
        try {
            jobProxy = env.execute();
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to submit job " + jobName, e);
        }
        submittedJobs.add(jobProxy);
        return jobProxy;
    }

    private void waitForStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .until(() -> jobProxy.getJobStatus() == expectedStatus);
    }

    private void assertPendingJobVisible(
            long pendingJobId, String expectedJobName, JobStatus expectedJobStatus) {
        String baseUrl =
                HOST
                        + httpPort
                        + seaTunnelConfig.getEngineConfig().getHttpConfig().getContextPath()
                        + RestConstant.REST_URL_PENDING_JOBS;
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Response response =
                                    given().get(baseUrl)
                                            .then()
                                            .statusCode(200)
                                            .extract()
                                            .response();
                            List<Map<String, Object>> pendingJobs =
                                    response.jsonPath().getList("pendingJobs");
                            Assertions.assertNotNull(pendingJobs);
                            Map<String, Object> job =
                                    pendingJobs.stream()
                                            .filter(
                                                    pendingJob ->
                                                            ((Number)
                                                                                    pendingJob.get(
                                                                                            RestConstant
                                                                                                    .JOB_ID))
                                                                            .longValue()
                                                                    == pendingJobId)
                                            .findFirst()
                                            .orElseThrow(
                                                    () ->
                                                            new AssertionError(
                                                                    "Pending job "
                                                                            + pendingJobId
                                                                            + " not found"));
                            Assertions.assertEquals(
                                    expectedJobName, job.get(RestConstant.JOB_NAME));
                            Assertions.assertEquals(
                                    expectedJobStatus.name(), job.get(RestConstant.JOB_STATUS));
                        });
    }
}
