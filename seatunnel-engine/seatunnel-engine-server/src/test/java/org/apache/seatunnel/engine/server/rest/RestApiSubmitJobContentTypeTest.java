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
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

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
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Verifies that {@code POST /submit-job} reads the job config from the request body and its options
 * from the query string whatever the request content type is. {@code curl -d} sends {@code
 * application/x-www-form-urlencoded} by default, for which reading the request parameters used to
 * consume the body and fail the request with {@code IllegalStateException: STREAMED}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiSubmitJobContentTypeTest {

    private static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";
    private static final int HAZELCAST_PORT = TestUtils.getAvailablePort(100);

    private HazelcastInstanceImpl instance;
    private SeaTunnelServer server;
    private int restPort;

    @BeforeAll
    public void setUp() throws Exception {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(
                TestUtils.getClusterName("SubmitJobContentTypeTest_" + System.nanoTime()));

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(true);
        httpConfig.setEnableHttps(false);
        httpConfig.setPort(26000);
        httpConfig.setEnableDynamicPort(true);
        httpConfig.setPortRange(2000);

        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        server = instance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);

        restPort = getHttpPort(server);
        awaitRestReady(restPort);
    }

    @AfterAll
    public void tearDown() {
        try {
            if (server != null) {
                server.shutdown(true);
            }
            if (instance != null) {
                instance.shutdown();
            }
            FileUtils.deleteFile(Paths.get("logs").toString());
        } catch (Exception e) {
            System.err.println(ExceptionUtils.getMessage(e));
        }
    }

    @Test
    public void testSubmitHoconJobWithFormContentType() throws Exception {
        HttpResponse response =
                post("/submit-job?format=hocon&jobName=form_hocon", FORM_CONTENT_TYPE, hoconBody());

        assertSubmitted(response, "form_hocon");
    }

    @Test
    public void testSubmitJsonJobWithFormContentType() throws Exception {
        HttpResponse response =
                post("/submit-job?jobName=form_json", FORM_CONTENT_TYPE, jsonBody());

        assertSubmitted(response, "form_json");
    }

    @Test
    public void testSubmitJsonJobWithJsonContentType() throws Exception {
        HttpResponse response =
                post("/submit-job?jobName=json_json", "application/json", jsonBody());

        assertSubmitted(response, "json_json");
    }

    @Test
    public void testQueryParametersAreUrlDecoded() throws Exception {
        HttpResponse response =
                post(
                        "/submit-job?format=hocon&jobName=sales%26ops+100%25",
                        FORM_CONTENT_TYPE, hoconBody());

        assertSubmitted(response, "sales&ops 100%");
    }

    private static void assertSubmitted(HttpResponse response, String expectedJobName) {
        Assertions.assertEquals(200, response.code, () -> "responseBody=" + response.body);
        Assertions.assertTrue(
                response.body.contains("\"jobId\""),
                () -> "Response should contain jobId, got: " + response.body);
        Assertions.assertTrue(
                response.body.contains("\"jobName\":\"" + expectedJobName + "\""),
                () -> "Response should contain jobName " + expectedJobName + ": " + response.body);
    }

    private static String hoconBody() {
        return "env {\n"
                + "  job.mode = \"BATCH\"\n"
                + "}\n"
                + "source {\n"
                + "  FakeSource {\n"
                + "    row.num = 5\n"
                + "    schema { fields { name = \"string\" } }\n"
                + "  }\n"
                + "}\n"
                + "transform {}\n"
                + "sink { Console {} }";
    }

    private static String jsonBody() {
        return "{\"env\": {\"job.mode\": \"batch\"},"
                + " \"source\": [{\"plugin_name\": \"FakeSource\", \"plugin_output\": \"fake\","
                + " \"row.num\": 5, \"schema\": {\"fields\": {\"name\": \"string\"}}}],"
                + " \"transform\": [],"
                + " \"sink\": [{\"plugin_name\": \"Console\", \"plugin_input\": [\"fake\"]}]}";
    }

    private HttpResponse post(String pathAndQuery, String contentType, String body)
            throws IOException {
        HttpURLConnection conn =
                (HttpURLConnection)
                        new URL("http://localhost:" + restPort + pathAndQuery).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", contentType);
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
            return new HttpResponse(code, in.lines().collect(Collectors.joining()));
        } finally {
            conn.disconnect();
        }
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
        org.apache.seatunnel.shade.org.eclipse.jetty.server.Server jettyServer =
                (org.apache.seatunnel.shade.org.eclipse.jetty.server.Server)
                        serverField.get(jettyService);

        return Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(
                        () -> {
                            for (Connector connector : jettyServer.getConnectors()) {
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

    private static String getHazelcastConfig() {
        // This single-member fixture must not scan nearby ports used by its REST service.
        return "hazelcast:\n"
                + "  cluster-name: seatunnel\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        CLUSTER_WRITE:\n"
                + "          enabled: true\n"
                + "    join:\n"
                + "      auto-detection:\n"
                + "        enabled: false\n"
                + "      multicast:\n"
                + "        enabled: false\n"
                + "      tcp-ip:\n"
                + "        enabled: false\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: "
                + HAZELCAST_PORT
                + "\n"
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.logging.type: log4j2\n";
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
