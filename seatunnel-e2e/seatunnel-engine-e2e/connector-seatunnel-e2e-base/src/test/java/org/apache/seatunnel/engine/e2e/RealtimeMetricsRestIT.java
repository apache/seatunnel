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

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.observability.ObservabilityConfig;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
        config.getEngineConfig().setHistoryJobExpireMinutes(10);
        MemberAttributeConfig tags = new MemberAttributeConfig();
        tags.setAttribute("node", "node1");
        config.getHazelcastConfig().setMemberAttributeConfig(tags);
        node = SeaTunnelServerStarter.createHazelcastInstance(config);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
    }

    private void startJob(String filePath) throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("multi_pipeline_fake_to_console_observability_realtime_metrics");
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, config);

        jobProxy = jobExecutionEnv.execute();
        awaitJobRunning();
    }

    private void awaitJobRunning() {
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
    public void testRealtimeMetricsEndpointsReturnData() throws Exception {
        startJob(TestUtils.getResource("multi_pipeline_fake_to_console_observability.conf"));
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
    public void testRealtimeMetricsUseUpdatedConfigAfterSavepointRestore(@TempDir Path directory)
            throws Exception {
        Path jobFile = directory.resolve("observability-restore.conf");
        String jobConfig =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        TestUtils.getResource(
                                                "stream_fake_to_console_terminal_zombie_master_switch.conf"))),
                        StandardCharsets.UTF_8);
        String observabilityConfig =
                "env {\n"
                        + "  engine {\n"
                        + "    observability {\n"
                        + "      enabled = %s\n"
                        + "      bucket_ms = %d\n"
                        + "      retention_minutes = 3\n"
                        + "    }\n"
                        + "  }\n";
        Files.write(
                jobFile,
                jobConfig
                        .replace("env {", String.format(observabilityConfig, false, 1000))
                        .getBytes(StandardCharsets.UTF_8));
        startJob(jobFile.toString());
        String baseUrl = HOST + config.getEngineConfig().getHttpConfig().getPort();
        long jobId = jobProxy.getJobId();
        awaitRealtimeConfig(baseUrl, jobId, false, 1000);

        given().contentType("application/json")
                .body("{\"jobId\":\"" + jobId + "\",\"isStopWithSavePoint\":true}")
                .post(baseUrl + "/stop-job")
                .then()
                .statusCode(200);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.SAVEPOINT_DONE, jobProxy.getJobStatus()));
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNotNull(
                                        server.getCoordinatorService()
                                                .getJobHistoryService()
                                                .getJobDAGInfo(jobId)));

        Files.write(
                jobFile,
                jobConfig
                        .replace("env {", String.format(observabilityConfig, true, 2000))
                        .getBytes(StandardCharsets.UTF_8));
        JobConfig restoredJobConfig = new JobConfig();
        restoredJobConfig.setName("restored_observability_realtime_metrics");
        jobProxy =
                engineClient
                        .restoreExecutionContext(
                                jobFile.toString(), restoredJobConfig, config, jobId)
                        .execute();
        Assertions.assertEquals(jobId, jobProxy.getJobId());
        awaitJobRunning();

        awaitRealtimeConfig(baseUrl, jobId, true, 2000);
        Assertions.assertFalse(
                ObservabilityConfig.fromEnvOptions(
                                server.getCoordinatorService()
                                        .getJobHistoryService()
                                        .getJobDAGInfo(jobId)
                                        .getEnvOptions())
                        .isEnabled(),
                "The previous attempt must remain in history while the restored job is running");
    }

    private void awaitRealtimeConfig(String baseUrl, long jobId, boolean enabled, long bucketMs) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
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
                                            .filter(item -> toLong(item.get("jobId")) == jobId)
                                            .findFirst()
                                            .orElseThrow(() -> new AssertionError("job not found"));
                            Assertions.assertEquals(enabled, job.get("enabled"));
                            Assertions.assertEquals(bucketMs, toLong(job.get("bucketMs")));
                            for (String endpoint : new String[] {"edges", "vertices"}) {
                                Map<String, Object> response =
                                        given().get(
                                                        baseUrl
                                                                + "/metrics/realtime/jobs/"
                                                                + jobId
                                                                + "/"
                                                                + endpoint)
                                                .then()
                                                .statusCode(200)
                                                .extract()
                                                .as(Map.class);
                                if (enabled) {
                                    Assertions.assertNotEquals(
                                            Boolean.FALSE, response.get("enabled"));
                                    if ("vertices".equals(endpoint)) {
                                        List<Map<String, Object>> vertices =
                                                (List<Map<String, Object>>)
                                                        response.get("vertices");
                                        Assertions.assertNotNull(vertices);
                                        Assertions.assertFalse(vertices.isEmpty());
                                        List<?> points = (List<?>) vertices.get(0).get("points");
                                        Assertions.assertNotNull(points);
                                        Assertions.assertFalse(points.isEmpty());
                                    }
                                } else {
                                    Assertions.assertEquals(Boolean.FALSE, response.get("enabled"));
                                }
                                Assertions.assertEquals(bucketMs, toLong(response.get("bucketMs")));
                            }
                        });
    }

    private static void assertRatio01(Object value) {
        double v = toDouble(value);
        Assertions.assertTrue(v >= 0D, "ratio must be >= 0");
        Assertions.assertTrue(v <= 1D, "ratio must be <= 1");
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
