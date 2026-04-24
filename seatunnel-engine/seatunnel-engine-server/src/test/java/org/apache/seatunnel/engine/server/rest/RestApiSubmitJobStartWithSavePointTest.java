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

package org.apache.seatunnel.engine.server.rest;

import org.apache.seatunnel.shade.org.eclipse.jetty.server.Connector;
import org.apache.seatunnel.shade.org.eclipse.jetty.server.ServerConnector;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.parse.JobConfigParser;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.checkpoint.ActionState;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.utils.RestUtil;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiSubmitJobStartWithSavePointTest {

    private static final String SOURCE_FACTORY_ID = "FakeSource";
    private static final String TEST_JOB_NAME = "test";

    private HazelcastInstanceImpl masterInstance;
    private HazelcastInstanceImpl workerInstance;
    private SeaTunnelServer masterServer;
    private SeaTunnelServer workerServer;
    private Path checkpointDir;
    private int workerRestPort;

    @BeforeAll
    public void setUp() throws Exception {
        String clusterName =
                TestUtils.getClusterName(
                        "RestApiSubmitJobStartWithSavePointTest_" + System.nanoTime());
        checkpointDir = Files.createTempDirectory(clusterName + "_checkpoint_");

        SeaTunnelConfig masterConfig = createSeaTunnelConfig(clusterName, 20000, false);
        SeaTunnelConfig workerConfig = createSeaTunnelConfig(clusterName, 23000, true);

        masterInstance = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
        workerInstance = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);

        masterServer = masterInstance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        workerServer = workerInstance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    2, masterInstance.getCluster().getMembers().size());
                            Assertions.assertEquals(
                                    2, workerInstance.getCluster().getMembers().size());
                        });

        workerRestPort = getHttpPort(workerServer);
        awaitRestReady(workerRestPort);
    }

    @AfterAll
    public void tearDown() {
        try {
            if (workerServer != null) {
                workerServer.shutdown(true);
            }
            if (masterServer != null) {
                masterServer.shutdown(true);
            }
            if (workerInstance != null) {
                workerInstance.shutdown();
            }
            if (masterInstance != null) {
                masterInstance.shutdown();
            }

            if (checkpointDir != null) {
                FileUtils.deleteFile(checkpointDir.toString());
            }

            Path logPath = Paths.get("logs");
            FileUtils.deleteFile(logPath.toString());
        } catch (Exception e) {
            // Best-effort cleanup; avoid masking test assertion failures.
            System.err.println(ExceptionUtils.getMessage(e));
        }
    }

    @Test
    public void testSubmitJobStartWithSavePointNoCheckpointOnWorkerReturns400() throws Exception {
        long jobId = System.currentTimeMillis();
        String requestUrl =
                "http://localhost:"
                        + workerRestPort
                        + "/submit-job?format=json&jobId="
                        + jobId
                        + "&jobName="
                        + TEST_JOB_NAME
                        + "&isStartWithSavePoint=true";

        HttpResponse response = postJson(requestUrl, getRequestBody());
        Assertions.assertEquals(400, response.code, () -> "responseBody=" + response.body);
        Assertions.assertTrue(response.body.contains("\"status\":\"fail\""));
        Assertions.assertTrue(response.body.contains("No checkpoint found for jobId=" + jobId));
    }

    @Test
    public void testBuildJobStartWithSavePointOnWorkerWhenCheckpointExists() throws Exception {
        Assertions.assertNotNull(masterServer);
        Assertions.assertNotNull(masterServer.getCheckpointService());
        Assertions.assertNotNull(workerServer);
        Assertions.assertNull(workerServer.getCheckpointService());

        long jobId = System.currentTimeMillis();
        storeFakeSourceCheckpoint(jobId);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(TEST_JOB_NAME);
        org.apache.seatunnel.shade.com.typesafe.config.Config seaTunnelJobConfig =
                buildSeaTunnelJobConfigFromJsonRequest();

        RestJobExecutionEnvironment restJobExecutionEnvironment =
                new RestJobExecutionEnvironment(
                        workerServer,
                        jobConfig,
                        seaTunnelJobConfig,
                        workerInstance.node,
                        true,
                        jobId);
        JobImmutableInformation jobImmutableInformation = restJobExecutionEnvironment.build();
        Assertions.assertEquals(jobId, jobImmutableInformation.getJobId());
        Assertions.assertTrue(jobImmutableInformation.isStartWithSavePoint());
    }

    @Test
    public void testBuildJobStartWithSavePointOnMasterWhenCheckpointExists() throws Exception {
        Assertions.assertNotNull(masterServer);
        Assertions.assertNotNull(masterServer.getCheckpointService());

        long jobId = System.currentTimeMillis();
        storeFakeSourceCheckpoint(jobId);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(TEST_JOB_NAME);
        org.apache.seatunnel.shade.com.typesafe.config.Config seaTunnelJobConfig =
                buildSeaTunnelJobConfigFromJsonRequest();

        RestJobExecutionEnvironment restJobExecutionEnvironment =
                new RestJobExecutionEnvironment(
                        masterServer,
                        jobConfig,
                        seaTunnelJobConfig,
                        masterInstance.node,
                        true,
                        jobId);
        JobImmutableInformation jobImmutableInformation = restJobExecutionEnvironment.build();
        Assertions.assertEquals(jobId, jobImmutableInformation.getJobId());
        Assertions.assertTrue(jobImmutableInformation.isStartWithSavePoint());
    }

    private int getHttpPort(SeaTunnelServer seaTunnelServer) throws Exception {
        Field jettyServiceField = SeaTunnelServer.class.getDeclaredField("jettyService");
        jettyServiceField.setAccessible(true);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> jettyServiceField.get(seaTunnelServer) != null);
        Object jettyService = jettyServiceField.get(seaTunnelServer);

        Field serverField = jettyService.getClass().getDeclaredField("server");
        serverField.setAccessible(true);
        org.apache.seatunnel.shade.org.eclipse.jetty.server.Server server =
                (org.apache.seatunnel.shade.org.eclipse.jetty.server.Server)
                        serverField.get(jettyService);

        return Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(
                        () -> {
                            for (Connector connector : server.getConnectors()) {
                                if (connector instanceof ServerConnector) {
                                    int port = ((ServerConnector) connector).getLocalPort();
                                    if (port > 0) {
                                        return port;
                                    }
                                }
                            }
                            return -1;
                        },
                        port -> port > 0);
    }

    private void storeFakeSourceCheckpoint(long jobId) throws Exception {
        Assertions.assertNotNull(masterServer);
        Assertions.assertNotNull(masterServer.getCheckpointService());

        String sourceActionName = JobConfigParser.createSourceActionName(0, SOURCE_FACTORY_ID);
        ActionStateKey actionStateKey = new ActionStateKey("ActionStateKey - " + sourceActionName);

        ActionState actionState = new ActionState(actionStateKey, 1);
        actionState.reportState(
                -1,
                new ActionSubtaskState(
                        actionStateKey,
                        -1,
                        Collections.singletonList("coordinator".getBytes(StandardCharsets.UTF_8))));
        actionState.reportState(
                0, new ActionSubtaskState(actionStateKey, 0, Collections.emptyList()));

        Map<ActionStateKey, ActionState> taskStates = new HashMap<>();
        taskStates.put(actionStateKey, actionState);

        long checkpointId = 1L;
        int pipelineId = 1;
        long now = System.currentTimeMillis();
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        jobId,
                        pipelineId,
                        checkpointId,
                        now,
                        CheckpointType.SAVEPOINT_TYPE,
                        now,
                        taskStates,
                        Collections.emptyMap());

        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        byte[] checkpointBytes = serializer.serialize(completedCheckpoint);

        PipelineState pipelineState =
                PipelineState.builder()
                        .jobId(String.valueOf(jobId))
                        .pipelineId(pipelineId)
                        .checkpointId(checkpointId)
                        .states(checkpointBytes)
                        .build();

        masterServer.getCheckpointService().getCheckpointStorage().storeCheckPoint(pipelineState);
    }

    private org.apache.seatunnel.shade.com.typesafe.config.Config
            buildSeaTunnelJobConfigFromJsonRequest() throws IOException {
        return RestUtil.buildConfig(
                RestUtil.convertByteToJsonNode(getRequestBody().getBytes(StandardCharsets.UTF_8)));
    }

    private String getRequestBody() {
        return "{\n"
                + "  \"env\": {\n"
                + "    \"job.mode\": \"BATCH\",\n"
                + "    \"job.name\": \"rest_api_test\"\n"
                + "  },\n"
                + "  \"source\": [\n"
                + "    {\n"
                + "      \"plugin_name\": \"FakeSource\",\n"
                + "      \"plugin_output\": \"fake\",\n"
                + "      \"row.num\": 1,\n"
                + "      \"schema\": {\n"
                + "        \"fields\": {\n"
                + "          \"name\": \"string\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ],\n"
                + "  \"transform\": [],\n"
                + "  \"sink\": [\n"
                + "    {\n"
                + "      \"plugin_name\": \"Console\",\n"
                + "      \"plugin_input\": [\"fake\"]\n"
                + "    }\n"
                + "  ]\n"
                + "}\n";
    }

    private SeaTunnelConfig createSeaTunnelConfig(
            String clusterName, int httpPort, boolean enableRest) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(clusterName);

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(enableRest);
        httpConfig.setEnableHttps(false);
        if (enableRest) {
            httpConfig.setPort(httpPort);
            httpConfig.setEnableDynamicPort(true);
            httpConfig.setPortRange(2000);
        }

        if (checkpointDir != null) {
            seaTunnelConfig
                    .getEngineConfig()
                    .getCheckpointConfig()
                    .getStorage()
                    .setStorage("localfile");
            seaTunnelConfig
                    .getEngineConfig()
                    .getCheckpointConfig()
                    .getStorage()
                    .getStoragePluginConfig()
                    .put(StorageConstants.STORAGE_NAME_SPACE, checkpointDir.toString());
        }
        return seaTunnelConfig;
    }

    private void awaitRestReady(int port) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(
                        () -> {
                            try {
                                HttpURLConnection conn =
                                        (HttpURLConnection)
                                                new URL("http://localhost:" + port + "/overview")
                                                        .openConnection();
                                conn.setRequestMethod("GET");
                                conn.setConnectTimeout(2000);
                                conn.setReadTimeout(2000);
                                int code = conn.getResponseCode();
                                conn.disconnect();
                                return code == 200;
                            } catch (Exception e) {
                                return false;
                            }
                        });
    }

    private HttpResponse postJson(String requestUrl, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(requestUrl).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(30000);
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int code = conn.getResponseCode();
        try (BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(
                                code >= 200 && code < 300
                                        ? conn.getInputStream()
                                        : conn.getErrorStream(),
                                StandardCharsets.UTF_8))) {
            String responseBody = in.lines().collect(Collectors.joining());
            return new HttpResponse(code, responseBody);
        } finally {
            conn.disconnect();
        }
    }

    private static String getHazelcastConfig() {
        return "hazelcast:\n"
                + "  cluster-name: seatunnel\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        CLUSTER_WRITE:\n"
                + "          enabled: true\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - localhost\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: 5801\n"
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n";
    }

    private static class HttpResponse {
        private final int code;
        private final String body;

        private HttpResponse(int code, String body) {
            this.code = code;
            this.body = body;
        }
    }
}
