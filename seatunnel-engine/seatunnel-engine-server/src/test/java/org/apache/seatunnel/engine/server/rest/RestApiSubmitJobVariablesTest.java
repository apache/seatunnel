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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Submits jobs with {@code variables} through every REST submit endpoint of a running engine. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiSubmitJobVariablesTest {

    private static final int HAZELCAST_PORT = TestUtils.getAvailablePort(100);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String HOCON =
            "env {\n"
                    + "  job.mode = \"BATCH\"\n"
                    + "  job.name = \"sync_${region}\"\n"
                    + "}\n"
                    + "source {\n"
                    + "  FakeSource {\n"
                    + "    row.num = ${rows}\n"
                    + "    password = \"${db_password}\"\n"
                    + "    schema { fields { name = \"string\" } }\n"
                    + "  }\n"
                    + "}\n"
                    + "sink { Console {} }";

    private static final String SQL =
            "/* config\n"
                    + "env {\n"
                    + "  job.mode = \"BATCH\"\n"
                    + "  job.name = \"sync_${region}\"\n"
                    + "}\n"
                    + "*/\n"
                    + "CREATE TABLE source_table WITH (\n"
                    + "  'connector'='FakeSource',\n"
                    + "  'type'='source',\n"
                    + "  'row.num'='${rows}',\n"
                    + "  'schema'='{ fields { id = \"int\", name = \"string\" } }'\n"
                    + ");\n"
                    + "CREATE TABLE sink_table WITH (\n"
                    + "  'connector'='Console',\n"
                    + "  'type'='sink'\n"
                    + ");\n"
                    + "INSERT INTO sink_table SELECT source_table;";

    private static final String JSON_JOB =
            "{\"env\": {\"job.mode\": \"BATCH\", \"job.name\": \"sync_${region}\"},"
                    + " \"source\": [{\"plugin_name\": \"FakeSource\", \"row.num\": \"${rows}\","
                    + " \"schema\": {\"fields\": {\"name\": \"string\"}}}],"
                    + " \"sink\": [{\"plugin_name\": \"Console\"}]}";

    private HazelcastInstanceImpl instance;
    private SeaTunnelServer server;
    private String baseUrl;

    @BeforeAll
    public void setUp() throws Exception {
        String clusterName =
                TestUtils.getClusterName("SubmitJobVariablesTest_" + System.nanoTime());

        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(clusterName);

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

        baseUrl = "http://localhost:" + getHttpPort(server);
        awaitRestReady();
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
    public void testHoconBodyUsesQueryVariables() throws Exception {
        HttpResponse response =
                post(
                        "/submit-job?format=hocon&variables="
                                + encode("region=eu,rows=3,db_password=pw-hocon-xyz"),
                        HOCON);

        assertSubmittedAs(response, "sync_eu", "pw-hocon-xyz");
    }

    @Test
    public void testSqlBodyUsesQueryVariables() throws Exception {
        HttpResponse response =
                post("/submit-job?format=sql&variables=" + encode("region=apac,rows=2"), SQL);

        assertSubmittedAs(response, "sync_apac", null);
    }

    @Test
    public void testJsonBodyUsesBodyVariables() throws Exception {
        String body =
                JSON_JOB.replaceFirst(
                        "\\{", "{\"variables\": {\"region\": \"Zürich\", \"rows\": 4},");

        assertSubmittedAs(post("/submit-job", body), "sync_Zürich", null);
    }

    @Test
    public void testUploadUsesQueryVariables() throws Exception {
        HttpResponse response =
                postMultipart(
                        "/submit-job/upload?variables="
                                + encode("region=us,rows=1,db_password=pw-upload-xyz"),
                        "job.conf",
                        HOCON);

        assertSubmittedAs(response, "sync_us", "pw-upload-xyz");
    }

    @Test
    public void testSubmitJobsUsesVariablesPerJob() throws Exception {
        String body =
                "["
                        + JSON_JOB.replaceFirst(
                                "\\{", "{\"params\": {\"variables\": \"region=one,rows=1\"},")
                        + ","
                        + JSON_JOB.replaceFirst(
                                "\\{",
                                "{\"params\": {},"
                                        + " \"variables\": {\"region\": \"two\", \"rows\": \"2\"},")
                        + "]";

        HttpResponse response = post("/submit-jobs", body);
        Assertions.assertEquals(200, response.code, response.body);
        JsonNode jobs = MAPPER.readTree(response.body);
        Assertions.assertEquals("sync_one", jobs.get(0).get("jobName").asText());
        Assertions.assertEquals("sync_two", jobs.get(1).get("jobName").asText());
    }

    @Test
    public void testMalformedVariablesAreRejectedWithoutValues() throws Exception {
        HttpResponse noEquals =
                post(
                        "/submit-job?format=hocon&variables="
                                + encode("region=eu,rows=1,pw-noequals-xyz"),
                        HOCON);
        assertRejected(noEquals, "variables", "pw-noequals-xyz");

        HttpResponse duplicate =
                post(
                        "/submit-job?format=hocon&variables="
                                + encode("region=eu,rows=1,rows=pw-duplicate-xyz"),
                        HOCON);
        assertRejected(duplicate, "rows", "pw-duplicate-xyz");

        HttpResponse repeated =
                post(
                        "/submit-job?format=hocon&variables="
                                + encode("region=eu")
                                + "&variables="
                                + encode("rows=1"),
                        HOCON);
        assertRejected(repeated, "variables", null);

        // ${rows} has no variable
        HttpResponse unresolved =
                post(
                        "/submit-job?format=hocon&variables="
                                + encode("region=eu,db_password=pw-unresolved-xyz"),
                        HOCON);
        assertRejected(unresolved, "variables", "pw-unresolved-xyz");
    }

    private void assertSubmittedAs(HttpResponse response, String jobName, String hiddenValue)
            throws Exception {
        Assertions.assertEquals(200, response.code, response.body);
        JsonNode submitted = MAPPER.readTree(response.body);
        Assertions.assertEquals(jobName, submitted.get("jobName").asText());

        String jobId = submitted.get("jobId").asText();
        String jobInfo =
                Awaitility.await()
                        .atMost(60, TimeUnit.SECONDS)
                        .pollInterval(500, TimeUnit.MILLISECONDS)
                        .until(() -> get("/job-info/" + jobId), body -> body.contains("jobName"));
        Assertions.assertEquals(jobName, MAPPER.readTree(jobInfo).get("jobName").asText());
        Assertions.assertFalse(jobInfo.contains("variables"), jobInfo);
        if (hiddenValue != null) {
            Assertions.assertFalse(jobInfo.contains(hiddenValue), jobInfo);
        }
    }

    private static void assertRejected(HttpResponse response, String named, String hiddenValue) {
        Assertions.assertEquals(400, response.code, response.body);
        Assertions.assertTrue(response.body.contains(named), response.body);
        if (hiddenValue != null) {
            Assertions.assertFalse(response.body.contains(hiddenValue), response.body);
        }
    }

    private static String encode(String value) throws IOException {
        return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
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

    private void awaitRestReady() {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(
                        () -> {
                            try {
                                HttpURLConnection conn =
                                        (HttpURLConnection)
                                                new URL(baseUrl + "/overview").openConnection();
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

    private String get(String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + path).openConnection();
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(30000);
        return read(conn).body;
    }

    private HttpResponse post(String path, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + path).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(30000);
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return read(conn);
    }

    private HttpResponse postMultipart(String path, String fileName, String fileContent)
            throws IOException {
        String boundary = "----SeaTunnelTestBoundary" + System.currentTimeMillis();
        HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + path).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(30000);
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            String part =
                    "--"
                            + boundary
                            + "\r\n"
                            + "Content-Disposition: form-data; name=\"config_file\"; filename=\""
                            + fileName
                            + "\"\r\n"
                            + "Content-Type: application/octet-stream\r\n"
                            + "\r\n"
                            + fileContent
                            + "\r\n"
                            + "--"
                            + boundary
                            + "--\r\n";
            os.write(part.getBytes(StandardCharsets.UTF_8));
        }
        return read(conn);
    }

    private static HttpResponse read(HttpURLConnection conn) throws IOException {
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
