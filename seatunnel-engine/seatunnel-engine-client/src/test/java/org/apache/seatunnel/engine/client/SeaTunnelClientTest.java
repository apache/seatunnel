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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
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
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SINK_WRITE_QPS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_BYTES_PER_SECONDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_COUNT;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_RECEIVED_QPS;
import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
@Slf4j
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
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
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
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
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
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);

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

            log.info(jobMetrics);

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
                seaTunnelClient
                        .createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG)
                        .execute();
        long jobId1 = execute1.getJobId();

        execute1.waitForJobComplete();

        filePath = TestUtils.getResource("streaming_fake_to_console.conf");
        jobConfig = new JobConfig();
        jobConfig.setName("fake_to_console2");
        ClientJobProxy execute2 =
                seaTunnelClient
                        .createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG)
                        .execute();
        ClientJobProxy execute3 =
                seaTunnelClient
                        .createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG)
                        .execute();

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

        log.info(jobClient.getRunningJobMetrics());

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
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);

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
    public void testSetJobId() throws ExecutionException, InterruptedException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/streaming_fake_to_console.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testSetJobId");
        long jobId = 12345;
        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();
        try {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(
                            filePath, new ArrayList<>(), jobConfig, SEATUNNEL_CONFIG, jobId);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            Assertions.assertEquals(jobId, clientJobProxy.getJobId());

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
    public void testSetJobIdDuplicate() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/streaming_fake_to_console.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testSetJobId");
        long jobId = System.currentTimeMillis();
        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();
        try {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(
                            filePath, new ArrayList<>(), jobConfig, SEATUNNEL_CONFIG, jobId);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            Assertions.assertEquals(jobId, clientJobProxy.getJobId());

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

            ClientJobExecutionEnvironment jobExecutionEnvWithSameJobId =
                    seaTunnelClient.createExecutionContext(
                            filePath, new ArrayList<>(), jobConfig, SEATUNNEL_CONFIG, jobId);
            Exception exception =
                    Assertions.assertThrows(
                            Exception.class,
                            () -> jobExecutionEnvWithSameJobId.execute().waitForJobCompleteV2());
            Assertions.assertTrue(
                    exception
                            .getCause()
                            .getMessage()
                            .contains(
                                    String.format(
                                            "The job id %s has already been submitted and is not starting with a savepoint.",
                                            jobId)));
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
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            long jobId = clientJobProxy.getJobId();

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertNotNull(jobClient.getJobInfo(jobId));
                            });

            await().atMost(180000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(1000);
                                log.info(
                                        "======================job status:"
                                                + jobClient.getJobDetailStatus(jobId));
                                log.info(
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
    public void testJarsInEnvAddedToCommonJars() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test_with_jars.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("client_test_with_jars");
        try (SeaTunnelClient seaTunnelClient = createSeaTunnelClient()) {
            LogicalDag logicalDag =
                    seaTunnelClient
                            .createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG)
                            .getLogicalDag();
            Assertions.assertIterableEquals(
                    Arrays.asList("file:/tmp/test.jar", "file:/tmp/test2.jar"),
                    logicalDag.getLogicalVertexMap().values().iterator().next().getAction()
                            .getJarUrls().stream()
                            .map(URL::toString)
                            .collect(Collectors.toList()));
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
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);
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
                                            "SAVEPOINT_DONE", jobClient.getJobStatus(jobId)));

            Thread.sleep(1000);
            seaTunnelClient
                    .restoreExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG, jobId)
                    .execute();

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
    public void testGetMultiTableJobMetrics() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/batch_fake_multi_table_to_console.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("testGetMultiTableJobMetrics");

        SeaTunnelClient seaTunnelClient = createSeaTunnelClient();
        JobClient jobClient = seaTunnelClient.getJobClient();

        try {
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, SEATUNNEL_CONFIG);

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

            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_COUNT + "#fake.table1"));
            Assertions.assertTrue(
                    jobMetrics.contains(SOURCE_RECEIVED_COUNT + "#fake.public.table2"));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_COUNT + "#fake.table1"));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_COUNT + "#fake.public.table2"));
            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_BYTES + "#fake.table1"));
            Assertions.assertTrue(
                    jobMetrics.contains(SOURCE_RECEIVED_BYTES + "#fake.public.table2"));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_BYTES + "#fake.table1"));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_BYTES + "#fake.public.table2"));
            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_QPS + "#fake.table1"));
            Assertions.assertTrue(jobMetrics.contains(SOURCE_RECEIVED_QPS + "#fake.public.table2"));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_QPS + "#fake.table1"));
            Assertions.assertTrue(jobMetrics.contains(SINK_WRITE_QPS + "#fake.public.table2"));
            Assertions.assertTrue(
                    jobMetrics.contains(SOURCE_RECEIVED_BYTES_PER_SECONDS + "#fake.table1"));
            Assertions.assertTrue(
                    jobMetrics.contains(SOURCE_RECEIVED_BYTES_PER_SECONDS + "#fake.public.table2"));
            Assertions.assertTrue(
                    jobMetrics.contains(SINK_WRITE_BYTES_PER_SECONDS + "#fake.table1"));
            Assertions.assertTrue(
                    jobMetrics.contains(SINK_WRITE_BYTES_PER_SECONDS + "#fake.public.table2"));

            log.info("jobMetrics : {}", jobMetrics);
            JsonNode jobMetricsStr = new ObjectMapper().readTree(jobMetrics);
            List<String> metricNameList =
                    StreamSupport.stream(
                                    Spliterators.spliteratorUnknownSize(
                                            jobMetricsStr.fieldNames(), 0),
                                    false)
                            .collect(Collectors.toList());

            Map<String, Long> totalCount =
                    metricNameList.stream()
                            .filter(metrics -> !metrics.contains("#"))
                            .collect(
                                    Collectors.toMap(
                                            metrics -> metrics,
                                            metrics ->
                                                    StreamSupport.stream(
                                                                    jobMetricsStr
                                                                            .get(metrics)
                                                                            .spliterator(),
                                                                    false)
                                                            .mapToLong(
                                                                    value ->
                                                                            value.get("value")
                                                                                    .asLong())
                                                            .sum()));

            Map<String, Long> tableCount =
                    metricNameList.stream()
                            .filter(metrics -> metrics.contains("#"))
                            .collect(
                                    Collectors.toMap(
                                            metrics -> metrics,
                                            metrics ->
                                                    StreamSupport.stream(
                                                                    jobMetricsStr
                                                                            .get(metrics)
                                                                            .spliterator(),
                                                                    false)
                                                            .mapToLong(
                                                                    value ->
                                                                            value.get("value")
                                                                                    .asLong())
                                                            .sum()));

            Assertions.assertEquals(
                    totalCount.get(SOURCE_RECEIVED_COUNT),
                    tableCount.entrySet().stream()
                            .filter(e -> e.getKey().startsWith(SOURCE_RECEIVED_COUNT + "#"))
                            .mapToLong(Map.Entry::getValue)
                            .sum());
            Assertions.assertEquals(
                    totalCount.get(SINK_WRITE_COUNT),
                    tableCount.entrySet().stream()
                            .filter(e -> e.getKey().startsWith(SINK_WRITE_COUNT + "#"))
                            .mapToLong(Map.Entry::getValue)
                            .sum());
            Assertions.assertEquals(
                    totalCount.get(SOURCE_RECEIVED_BYTES),
                    tableCount.entrySet().stream()
                            .filter(e -> e.getKey().startsWith(SOURCE_RECEIVED_BYTES + "#"))
                            .mapToLong(Map.Entry::getValue)
                            .sum());
            Assertions.assertEquals(
                    totalCount.get(SINK_WRITE_BYTES),
                    tableCount.entrySet().stream()
                            .filter(e -> e.getKey().startsWith(SINK_WRITE_BYTES + "#"))
                            .mapToLong(Map.Entry::getValue)
                            .sum());
            // Instantaneous rates in the same direction are directly added
            // The size does not fluctuate more than %2 of the total value
            Assertions.assertTrue(
                    Math.abs(
                                    totalCount.get(SOURCE_RECEIVED_QPS)
                                            - tableCount.entrySet().stream()
                                                    .filter(
                                                            e ->
                                                                    e.getKey()
                                                                            .startsWith(
                                                                                    SOURCE_RECEIVED_QPS
                                                                                            + "#"))
                                                    .mapToLong(Map.Entry::getValue)
                                                    .sum())
                            < totalCount.get(SOURCE_RECEIVED_QPS) * 0.02);
            Assertions.assertTrue(
                    Math.abs(
                                    totalCount.get(SINK_WRITE_QPS)
                                            - tableCount.entrySet().stream()
                                                    .filter(
                                                            e ->
                                                                    e.getKey()
                                                                            .startsWith(
                                                                                    SINK_WRITE_QPS
                                                                                            + "#"))
                                                    .mapToLong(Map.Entry::getValue)
                                                    .sum())
                            < totalCount.get(SINK_WRITE_QPS) * 0.02);
            Assertions.assertTrue(
                    Math.abs(
                                    totalCount.get(SOURCE_RECEIVED_BYTES_PER_SECONDS)
                                            - tableCount.entrySet().stream()
                                                    .filter(
                                                            e ->
                                                                    e.getKey()
                                                                            .startsWith(
                                                                                    SOURCE_RECEIVED_BYTES_PER_SECONDS
                                                                                            + "#"))
                                                    .mapToLong(Map.Entry::getValue)
                                                    .sum())
                            < totalCount.get(SOURCE_RECEIVED_BYTES_PER_SECONDS) * 0.02);
            Assertions.assertTrue(
                    Math.abs(
                                    totalCount.get(SINK_WRITE_BYTES_PER_SECONDS)
                                            - tableCount.entrySet().stream()
                                                    .filter(
                                                            e ->
                                                                    e.getKey()
                                                                            .startsWith(
                                                                                    SINK_WRITE_BYTES_PER_SECONDS
                                                                                            + "#"))
                                                    .mapToLong(Map.Entry::getValue)
                                                    .sum())
                            < totalCount.get(SINK_WRITE_BYTES_PER_SECONDS) * 0.02);

        } catch (ExecutionException | InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            seaTunnelClient.close();
        }
    }

    @Test
    @SetEnvironmentVariable(
            key = "ST_DOCKER_MEMBER_LIST",
            value = "127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4")
    public void testDockerEnvOverwrite() {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        Assertions.assertEquals(4, clientConfig.getNetworkConfig().getAddresses().size());
    }

    @AfterAll
    public static void after() {
        INSTANCE.shutdown();
    }
}
