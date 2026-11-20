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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;

/**
 * E2E test for realtime metrics REST endpoints:
 *
 * <ul>
 *   <li>GET /metrics/realtime/jobs
 *   <li>GET /metrics/realtime/jobs/{jobId}/vertices?windowMs=...
 *   <li>GET /metrics/realtime/jobs/{jobId}/edges?windowMs=...
 * </ul>
 */
public class RealtimeMetricsRestIT {

    private static final String HOST = "http://localhost:";

    private HazelcastInstanceImpl node;
    private SeaTunnelClient engineClient;
    private SeaTunnelConfig config;
    private ClientJobProxy jobProxy;

    @BeforeEach
    void beforeEach() throws Exception {
        String testClusterName = TestUtils.getClusterName("RealtimeMetricsRestIT");
        config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getEngineConfig().getHttpConfig().setPort(getAvailablePort());
        config.getEngineConfig().getHttpConfig().setEnabled(true);
        config.getHazelcastConfig().setClusterName(testClusterName);
        config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        MemberAttributeConfig tags = new MemberAttributeConfig();
        tags.setAttribute("node", "node1");
        config.getHazelcastConfig().setMemberAttributeConfig(tags);
        node = SeaTunnelServerStarter.createHazelcastInstance(config);

        String filePath =
                TestUtils.getResource("multi_pipeline_fake_to_console_observability.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("multi_pipeline_fake_to_console_observability_realtime_metrics");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, config);

        jobProxy = jobExecutionEnv.execute();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.RUNNING, jobProxy.getJobStatus()));
    }

    @AfterEach
    void afterEach() {
        if (engineClient != null) {
            engineClient.close();
        }
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    public void testRealtimeMetricsEndpointsReturnData() {
        String baseUrl = HOST + config.getEngineConfig().getHttpConfig().getPort();
        long jobId = jobProxy.getJobId();

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            List<Map<String, Object>> jobs =
                                    given().get(baseUrl + "/metrics/realtime/jobs")
                                            .then()
                                            .statusCode(200)
                                            .extract()
                                            .path("jobs");
                            Assertions.assertNotNull(jobs);
                            boolean found =
                                    jobs.stream()
                                            .anyMatch(
                                                    m ->
                                                            m != null
                                                                    && String.valueOf(jobId)
                                                                            .equals(
                                                                                    String.valueOf(
                                                                                            m.get(
                                                                                                    "jobId"))));
                            Assertions.assertTrue(found, "job not found in /metrics/realtime/jobs");
                        });

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Map<String, Object> resp =
                                    given().get(
                                                    baseUrl
                                                            + "/metrics/realtime/jobs/"
                                                            + jobId
                                                            + "/vertices?windowMs=600000")
                                            .then()
                                            .statusCode(200)
                                            .extract()
                                            .as(Map.class);
                            Assertions.assertNotNull(resp);
                            Assertions.assertNotNull(resp.get("bucketMs"));
                            List<Map<String, Object>> vertices =
                                    (List<Map<String, Object>>) resp.get("vertices");
                            Assertions.assertNotNull(vertices);
                            Assertions.assertFalse(vertices.isEmpty(), "vertices is empty");

                            Map<String, Object> v0 = vertices.get(0);
                            Assertions.assertNotNull(v0.get("vertexId"));
                            List<Map<String, Object>> points =
                                    (List<Map<String, Object>>) v0.get("points");
                            Assertions.assertNotNull(points);
                            Assertions.assertFalse(points.isEmpty(), "vertex points is empty");

                            Map<String, Object> p0 = points.get(0);
                            assertRatio01(p0.get("sourceReadRatio"));
                            assertRatio01(p0.get("sourceIdleRatio"));
                            assertRatio01(p0.get("transformBusyRatio"));
                            assertRatio01(p0.get("sinkBusyRatio"));
                        });

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Map<String, Object> verticesResp =
                                    given().get(
                                                    baseUrl
                                                            + "/metrics/realtime/jobs/"
                                                            + jobId
                                                            + "/vertices?windowMs=600000")
                                            .then()
                                            .statusCode(200)
                                            .extract()
                                            .as(Map.class);
                            List<Map<String, Object>> vertices =
                                    (List<Map<String, Object>>) verticesResp.get("vertices");
                            Set<Long> vertexIds =
                                    vertices == null
                                            ? Collections.emptySet()
                                            : vertices.stream()
                                                    .map(v -> toLong(v.get("vertexId")))
                                                    .collect(Collectors.toSet());

                            Map<String, Object> resp =
                                    given().get(
                                                    baseUrl
                                                            + "/metrics/realtime/jobs/"
                                                            + jobId
                                                            + "/edges?windowMs=600000")
                                            .then()
                                            .statusCode(200)
                                            .extract()
                                            .as(Map.class);
                            Assertions.assertNotNull(resp);
                            Assertions.assertNotNull(resp.get("bucketMs"));
                            List<Map<String, Object>> edges =
                                    (List<Map<String, Object>>) resp.get("edges");
                            Assertions.assertNotNull(edges);
                            Assertions.assertFalse(edges.isEmpty(), "edges is empty");

                            Map<String, Object> e0 = edges.get(0);
                            Assertions.assertNotNull(e0.get("queueId"));
                            Assertions.assertNotNull(e0.get("targetVertexId"));
                            long targetVertexId = toLong(e0.get("targetVertexId"));
                            Assertions.assertTrue(
                                    vertexIds.isEmpty() || vertexIds.contains(targetVertexId),
                                    "edge targetVertexId should exist in vertices list");
                            List<Map<String, Object>> points =
                                    (List<Map<String, Object>>) e0.get("points");
                            Assertions.assertNotNull(points);
                            Assertions.assertFalse(points.isEmpty(), "edge points is empty");

                            Map<String, Object> p0 = points.get(0);
                            assertRatio01(p0.get("queueFillRatio"));
                            long capacity = toLong(p0.get("queueCapacity"));
                            long size = toLong(p0.get("queueSize"));
                            Assertions.assertTrue(capacity >= 0);
                            Assertions.assertTrue(size >= 0);
                            if (capacity > 0) {
                                Assertions.assertTrue(
                                        size <= capacity,
                                        "queueSize should not exceed queueCapacity");
                            }
                        });
    }

    @Test
    public void testRestoreUsesCurrentRealtimeObservabilityConfiguration() throws Exception {
        String baseUrl = HOST + config.getEngineConfig().getHttpConfig().getPort();
        jobProxy.cancelJob();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.CANCELED, jobProxy.getJobStatus()));

        Path disabledConfig = Files.createTempFile("realtime-observability-disabled-", ".conf");
        Path enabledConfig = Files.createTempFile("realtime-observability-enabled-", ".conf");
        Files.writeString(
                disabledConfig, restoreObservabilityConfig(false, 1000), StandardCharsets.UTF_8);
        Files.writeString(
                enabledConfig, restoreObservabilityConfig(true, 2000), StandardCharsets.UTF_8);

        try {
            long jobId;
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("realtime_observability_restore");
            ClientJobExecutionEnvironment executionEnvironment =
                    engineClient.createExecutionContext(
                            disabledConfig.toString(), jobConfig, config);
            ClientJobProxy initialJob = executionEnvironment.execute();
            jobId = initialJob.getJobId();
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, initialJob.getJobStatus()));

            awaitAllTasksReady(jobId);
            engineClient.savePointJob(jobId);
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.SAVEPOINT_DONE, initialJob.getJobStatus()));

            ClientJobProxy restoredJob =
                    engineClient
                            .restoreExecutionContext(
                                    enabledConfig.toString(), jobConfig, config, jobId)
                            .execute();
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, restoredJob.getJobStatus()));

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                List<Map<String, Object>> jobs =
                                        given().get(baseUrl + "/metrics/realtime/jobs")
                                                .then()
                                                .statusCode(200)
                                                .extract()
                                                .path("jobs");
                                Map<String, Object> job =
                                        jobs.stream()
                                                .filter(
                                                        value ->
                                                                String.valueOf(jobId)
                                                                        .equals(
                                                                                String.valueOf(
                                                                                        value.get(
                                                                                                "jobId"))))
                                                .findFirst()
                                                .orElse(null);
                                Assertions.assertNotNull(job, "restored job not found");
                                Assertions.assertEquals(true, job.get("enabled"));
                                Assertions.assertEquals(2000, toLong(job.get("bucketMs")));
                            });

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Map<String, Object> vertices =
                                        given().get(
                                                        baseUrl
                                                                + "/metrics/realtime/jobs/"
                                                                + jobId
                                                                + "/vertices?windowMs=180000")
                                                .then()
                                                .statusCode(200)
                                                .extract()
                                                .as(Map.class);
                                Assertions.assertEquals(2000, toLong(vertices.get("bucketMs")));
                                List<Map<String, Object>> vertexValues =
                                        (List<Map<String, Object>>) vertices.get("vertices");
                                Assertions.assertFalse(vertexValues.isEmpty());
                                Assertions.assertFalse(
                                        ((List<?>) vertexValues.get(0).get("points")).isEmpty());
                            });

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Map<String, Object> edges =
                                        given().get(
                                                        baseUrl
                                                                + "/metrics/realtime/jobs/"
                                                                + jobId
                                                                + "/edges?windowMs=180000")
                                                .then()
                                                .statusCode(200)
                                                .extract()
                                                .as(Map.class);
                                Assertions.assertEquals(2000, toLong(edges.get("bucketMs")));
                                List<Map<String, Object>> edgeValues =
                                        (List<Map<String, Object>>) edges.get("edges");
                                Assertions.assertFalse(edgeValues.isEmpty());
                                Assertions.assertFalse(
                                        ((List<?>) edgeValues.get(0).get("points")).isEmpty());
                            });
        } finally {
            Files.deleteIfExists(disabledConfig);
            Files.deleteIfExists(enabledConfig);
        }
    }

    private static void assertRatio01(Object value) {
        double v = toDouble(value);
        Assertions.assertTrue(v >= 0D, "ratio must be >= 0");
        Assertions.assertTrue(v <= 1D, "ratio must be <= 1");
    }

    private static String restoreObservabilityConfig(boolean enabled, long bucketMs) {
        // Keep the same job ID across restore while changing observability settings; the long-lived
        // source and delayed sink keep the job running until the savepoint and restore complete.
        return "env {\n"
                + "  job.mode = \"STREAMING\"\n"
                + "  checkpoint.interval = 15000\n"
                + "  parallelism = 1\n"
                + "  engine {\n"
                + "    observability {\n"
                + "      enabled = "
                + enabled
                + "\n"
                + "      bucket_ms = "
                + bucketMs
                + "\n"
                + "      retention_minutes = 3\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "\nsource {\n"
                + "  FakeSource {\n"
                + "    row.num = 10000000\n"
                + "    split.num = 1\n"
                + "    split.read-interval = 0\n"
                + "    schema = {\n"
                + "      fields {\n"
                + "        c_int = int\n"
                + "        c_string = string\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
                + "\n"
                + "sink {\n"
                + "  InMemory {\n"
                + "    write_delay_ms = 2\n"
                + "  }\n"
                + "}\n";
    }

    private static double toDouble(Object value) {
        if (value == null) {
            return 0D;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception ignored) {
            return 0D;
        }
    }

    private void awaitAllTasksReady(long jobId) {
        // Ensure all tasks are running before taking the savepoint.
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            SeaTunnelServer server =
                                    node.node
                                            .getNodeEngine()
                                            .getService(SeaTunnelServer.SERVICE_NAME);
                            JobMaster jobMaster =
                                    server.getCoordinatorService().getJobMaster(jobId);
                            Assertions.assertNotNull(jobMaster);
                            CheckpointManager checkpointManager = jobMaster.getCheckpointManager();
                            Assertions.assertNotNull(checkpointManager);
                            Map<Integer, CheckpointCoordinator> coordinators =
                                    ReflectionUtils.getField(checkpointManager, "coordinatorMap")
                                            .filter(Map.class::isInstance)
                                            .map(
                                                    value ->
                                                            (Map<Integer, CheckpointCoordinator>)
                                                                    value)
                                            .orElseThrow();
                            Assertions.assertFalse(coordinators.isEmpty());
                            coordinators
                                    .values()
                                    .forEach(
                                            coordinator -> {
                                                AtomicBoolean ready =
                                                        (AtomicBoolean)
                                                                ReflectionUtils.getField(
                                                                                coordinator,
                                                                                "isAllTaskReady")
                                                                        .orElseThrow();
                                                Assertions.assertTrue(ready.get());
                                            });
                        });
    }

    private static long toLong(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static int getAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No available port for realtime metrics REST IT", e);
        }
    }
}
