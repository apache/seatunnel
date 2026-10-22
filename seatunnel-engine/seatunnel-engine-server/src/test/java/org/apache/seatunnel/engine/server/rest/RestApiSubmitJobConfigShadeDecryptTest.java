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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration test verifying that REST API submit-job endpoints correctly call
 * ConfigShadeUtils.decryptConfig() for HOCON, SQL and upload formats.
 *
 * @see <a href="https://github.com/apache/seatunnel/issues/10590">#10590</a>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiSubmitJobConfigShadeDecryptTest {

    private static final String ENCRYPTED_USERNAME = "c2VhdHVubmVs";
    private static final String ENCRYPTED_PASSWORD = "c2VhdHVubmVsX3Bhc3N3b3Jk";
    private static final int HAZELCAST_PORT = TestUtils.getAvailablePort(100);

    private HazelcastInstanceImpl instance;
    private SeaTunnelServer server;
    private int restPort;

    @BeforeAll
    public void setUp() throws Exception {
        String clusterName =
                TestUtils.getClusterName("ConfigShadeDecryptTest_" + System.nanoTime());

        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(clusterName);

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(true);
        httpConfig.setEnableHttps(false);
        httpConfig.setPort(24000);
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
    public void testFixtureStartsWithoutDiscovery() {
        Assertions.assertEquals(1, instance.getCluster().getMembers().size());
        Assertions.assertNull(instance.node.getJoiner());
    }

    @ParameterizedTest
    @MethodSource("requestBodyCharsets")
    void testSubmitPreservesRequestBodyCharset(
            String format,
            String contentType,
            String charsetDeclaration,
            Charset charset,
            String jobName)
            throws Exception {
        String body;
        if ("hocon".equals(format)) {
            body = buildHoconBody().replace("env {", "env {\n  job.name = \"" + jobName + "\"");
        } else if ("sql".equals(format)) {
            body = buildSqlBody().replace("env {", "env {\n  job.name = \"" + jobName + "\"");
        } else {
            body = buildJsonBody(jobName);
        }
        boolean batch = "batch".equals(format);
        if (batch) {
            body =
                    Json.parse(body)
                            .asObject()
                            .add("params", new JsonObject().add("isStartWithSavePoint", "false"))
                            .toString();
        }
        String path = batch ? "/submit-jobs" : "/submit-job?format=" + format;
        HttpResponse response =
                post(
                        "http://localhost:" + restPort + path,
                        contentType + charsetDeclaration,
                        batch ? "[" + body + "]" : body,
                        charset);
        Assertions.assertEquals(200, response.code, () -> "responseBody=" + response.body);
        JsonObject submitted =
                batch
                        ? Json.parse(response.body).asArray().get(0).asObject()
                        : Json.parse(response.body).asObject();
        Assertions.assertEquals(jobName, submitted.getString("jobName", null));
        Assertions.assertNotNull(submitted.getString("jobId", null));
    }

    private static Stream<Arguments> requestBodyCharsets() {
        return Stream.of("hocon", "sql", "json", "batch")
                .flatMap(
                        format -> {
                            Stream<String> contentTypes =
                                    "hocon".equals(format) || "sql".equals(format)
                                            ? Stream.of("text/plain")
                                            : Stream.of("application/json", "text/plain");
                            return contentTypes.flatMap(
                                    contentType ->
                                            Stream.of(
                                                    Arguments.of(
                                                            format,
                                                            contentType,
                                                            "",
                                                            StandardCharsets.UTF_8,
                                                            "café-数据"),
                                                    Arguments.of(
                                                            format,
                                                            contentType,
                                                            "; charset=UTF-8",
                                                            StandardCharsets.UTF_8,
                                                            "café-数据"),
                                                    Arguments.of(
                                                            format,
                                                            contentType,
                                                            "; charset=ISO-8859-1",
                                                            StandardCharsets.ISO_8859_1,
                                                            "café-été"),
                                                    Arguments.of(
                                                            format,
                                                            contentType,
                                                            "; charset=UTF-16LE",
                                                            StandardCharsets.UTF_16LE,
                                                            "café-数据")));
                        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"text/plain", "application/json", "text/plain; charset=UTF-8"})
    void testEncryptConfigPreservesNonAsciiBody(String contentType) throws Exception {
        HttpResponse response =
                post(
                        "http://localhost:" + restPort + "/encrypt-config",
                        contentType,
                        buildJsonBody("café-数据"),
                        StandardCharsets.UTF_8);
        Assertions.assertEquals(200, response.code, () -> "responseBody=" + response.body);
        Assertions.assertEquals(
                "café-数据",
                Json.parse(response.body)
                        .asObject()
                        .get("env")
                        .asObject()
                        .getString("job.name", null));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "/submit-job?format=hocon",
                "/submit-jobs",
                "/stop-job",
                "/stop-jobs",
                "/encrypt-config",
                "/update-tags",
                "/loggers/charset-regression"
            })
    void testUnsupportedRequestCharsetReturnsClientError(String path) throws Exception {
        HttpResponse response =
                post(
                        "http://localhost:" + restPort + path,
                        "text/plain; charset=not-a-real-charset",
                        "{}",
                        StandardCharsets.UTF_8);
        Assertions.assertEquals(400, response.code, () -> "responseBody=" + response.body);
        Assertions.assertTrue(response.body.contains("Unsupported request charset"), response.body);
    }

    @Test
    public void testSubmitJobWithHoconFormatDecryptsConfig() throws Exception {
        String requestUrl =
                "http://localhost:"
                        + restPort
                        + "/submit-job?format=hocon&jobName=hocon_shade_test";

        HttpResponse response = post(requestUrl, "text/plain", buildHoconBody());
        Assertions.assertEquals(200, response.code, () -> "responseBody=" + response.body);
        Assertions.assertTrue(
                response.body.contains("jobId"),
                "Response should contain jobId, got: " + response.body);
    }

    @Test
    public void testSubmitJobWithHoconFormatMissingShadeIdentifier() throws Exception {
        String bodyWithoutShade =
                "env {\n"
                        + "  job.mode = \"BATCH\"\n"
                        + "}\n"
                        + "source {\n"
                        + "  FakeSource {\n"
                        + "    row.num = 5\n"
                        + "    username = \""
                        + ENCRYPTED_USERNAME
                        + "\"\n"
                        + "    password = \""
                        + ENCRYPTED_PASSWORD
                        + "\"\n"
                        + "    schema { fields { name = \"string\" } }\n"
                        + "  }\n"
                        + "}\n"
                        + "transform {}\n"
                        + "sink { Console {} }";

        String requestUrl = "http://localhost:" + restPort + "/submit-job?format=hocon";

        HttpResponse response = post(requestUrl, "text/plain", bodyWithoutShade);
        // Should handle normally (without decryption)
        Assertions.assertEquals(200, response.code);
    }

    @Test
    public void testSubmitJobByUploadSqlFileDecryptsConfig() throws Exception {
        String requestUrl = "http://localhost:" + restPort + "/submit-job/upload";

        HttpResponse response = postMultipart(requestUrl, "job.sql", buildSqlBody());
        Assertions.assertEquals(200, response.code);
        Assertions.assertTrue(response.body.contains("jobId"));
    }

    @Test
    public void testSubmitJobByUploadHoconFileDecryptsConfig() throws Exception {
        String requestUrl = "http://localhost:" + restPort + "/submit-job/upload";

        HttpResponse response = postMultipart(requestUrl, "job.conf", buildHoconBody());
        Assertions.assertEquals(200, response.code, () -> "responseBody=" + response.body);
        Assertions.assertTrue(
                response.body.contains("jobId"),
                "Response should contain jobId, got: " + response.body);
    }

    @Test
    public void testSubmitJobWithHoconFormatInvalidBase64() throws Exception {
        String invalidBase64Body = buildHoconBody().replace(ENCRYPTED_USERNAME, "invalid_base64!");

        String requestUrl = "http://localhost:" + restPort + "/submit-job?format=hocon";

        HttpResponse response = post(requestUrl, "text/plain", invalidBase64Body);
        // Should return 400 Bad Request or 500 Internal Server Error
        Assertions.assertTrue(response.code == 400 || response.code == 500);
        Assertions.assertTrue(response.body.contains("fail") || response.body.contains("Illegal"));
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

    private HttpResponse post(String requestUrl, String contentType, String body)
            throws IOException {
        return post(requestUrl, contentType + "; charset=UTF-8", body, StandardCharsets.UTF_8);
    }

    private HttpResponse post(String requestUrl, String contentType, String body, Charset charset)
            throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(requestUrl).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", contentType);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(30000);
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(charset));
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

    private HttpResponse postMultipart(String requestUrl, String fileName, String fileContent)
            throws IOException {
        String boundary = "----SeaTunnelTestBoundary" + System.currentTimeMillis();
        HttpURLConnection conn = (HttpURLConnection) new URL(requestUrl).openConnection();
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

    private String buildJsonBody(String jobName) {
        return "{\"env\":{\"job.mode\":\"BATCH\",\"job.name\":\""
                + jobName
                + "\"},\"source\":[{\"plugin_name\":\"FakeSource\",\"row.num\":1,"
                + "\"schema\":{\"fields\":{\"name\":\"string\"}}}],"
                + "\"transform\":[],\"sink\":[{\"plugin_name\":\"Console\"}]}";
    }

    private String buildHoconBody() {
        return "env {\n"
                + "  job.mode = \"BATCH\"\n"
                + "  shade.identifier = \"base64\"\n"
                + "}\n"
                + "source {\n"
                + "  FakeSource {\n"
                + "    row.num = 5\n"
                + "    username = \""
                + ENCRYPTED_USERNAME
                + "\"\n"
                + "    password = \""
                + ENCRYPTED_PASSWORD
                + "\"\n"
                + "    schema { fields { name = \"string\" } }\n"
                + "  }\n"
                + "}\n"
                + "transform {}\n"
                + "sink { Console {} }";
    }

    private String buildSqlBody() {
        return "/* config\n"
                + "env {\n"
                + "  parallelism = 1\n"
                + "  job.mode = \"BATCH\"\n"
                + "  shade.identifier = \"base64\"\n"
                + "}\n"
                + "*/\n"
                + "CREATE TABLE source_table WITH (\n"
                + "  'connector'='FakeSource',\n"
                + "  'type'='source',\n"
                + "  'username'='"
                + ENCRYPTED_USERNAME
                + "',\n"
                + "  'password'='"
                + ENCRYPTED_PASSWORD
                + "',\n"
                + "  'row.num'='3',\n"
                + "  'schema'='{ fields { id = \"int\", name = \"string\" } }'\n"
                + ");\n"
                + "CREATE TABLE sink_table WITH (\n"
                + "  'connector'='Console',\n"
                + "  'type'='sink'\n"
                + ");\n"
                + "INSERT INTO sink_table SELECT source_table;";
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
