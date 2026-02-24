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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RestApiSubmitJobStartWithSavePointTest {

    private HazelcastInstanceImpl masterInstance;
    private HazelcastInstanceImpl workerInstance;
    private SeaTunnelServer masterServer;
    private SeaTunnelServer workerServer;

    @AfterEach
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

            Path logPath = Paths.get("logs");
            FileUtils.deleteFile(logPath.toString());
        } catch (Exception e) {
            // Best-effort cleanup; avoid masking test assertion failures.
            System.err.println(ExceptionUtils.getMessage(e));
        }
    }

    @Test
    public void testSubmitJobStartWithSavePointNoCheckpointOnWorkerReturns400() throws Exception {
        String testClassName = this.getClass().getSimpleName();
        String clusterName =
                TestUtils.getClusterName("RestApiSubmitJobStartWithSavePointTest_" + testClassName);

        int masterPort = findFreePort();
        int workerPort = findFreePortExcluding(masterPort);

        SeaTunnelConfig masterConfig = createSeaTunnelConfig(clusterName, masterPort);
        SeaTunnelConfig workerConfig = createSeaTunnelConfig(clusterName, workerPort);

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

        awaitRestReady(workerPort);

        long jobId = System.currentTimeMillis();
        String requestUrl =
                "http://localhost:"
                        + workerPort
                        + "/submit-job?format=json&jobId="
                        + jobId
                        + "&jobName=test&isStartWithSavePoint=true";

        String requestBody =
                "{\n"
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
                        + "}";

        HttpResponse response = postJson(requestUrl, requestBody);
        Assertions.assertEquals(400, response.code);
        Assertions.assertTrue(response.body.contains("\"status\":\"fail\""));
        Assertions.assertTrue(response.body.contains("No checkpoint found for jobId=" + jobId));
    }

    private SeaTunnelConfig createSeaTunnelConfig(String clusterName, int httpPort) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(clusterName);

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(true);
        httpConfig.setPort(httpPort);
        httpConfig.setEnableHttps(false);
        httpConfig.setEnableDynamicPort(false);
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

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("No free port available", e);
        }
    }

    private static int findFreePortExcluding(int exclude) {
        int port;
        do {
            port = findFreePort();
        } while (port == exclude);
        return port;
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
