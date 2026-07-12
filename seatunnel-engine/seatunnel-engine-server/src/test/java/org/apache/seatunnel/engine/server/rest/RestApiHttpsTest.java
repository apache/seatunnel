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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.serialization.Data;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/** Test for Rest API with HTTPS. */
@DisabledOnOs(OS.WINDOWS)
public class RestApiHttpsTest extends AbstractSeaTunnelServerTest {
    private static final int HTTP_PORT = 28080;
    private static final int HTTPS_PORT = 28443;

    private static final int HTTP_PORT2 = 28088;
    private static final int HTTPS_PORT2 = 28543;
    private static final String SERVER_KEYSTORE_PASSWORD = "server_keystore_password";
    private static final String CLIENT_KEYSTORE_PASSWORD = "client_keystore_password";

    @Override
    @BeforeAll
    public void before() {
        String name = this.getClass().getName();
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(TestUtils.getClusterName("RestApiHttpsTest_" + name));
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(true);
        httpConfig.setPort(HTTP_PORT);
        httpConfig.setHttpsPort(HTTPS_PORT);
        httpConfig.setEnableHttps(true);

        httpConfig.setKeyStorePath(getPath("server_keystore.jks"));
        httpConfig.setKeyManagerPassword(SERVER_KEYSTORE_PASSWORD);
        httpConfig.setKeyStorePassword(SERVER_KEYSTORE_PASSWORD);

        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    public String getPath(String confFile) {
        return System.getProperty("user.dir") + "/src/test/resources/https/" + confFile;
    }

    @Test
    public void testRestApiHttp() throws Exception {
        restApiRequestHttp(
                "http://localhost:" + HTTP_PORT + "/overview",
                (code, content) -> {
                    Assertions.assertEquals(200, code);
                    Assertions.assertTrue(content.contains("projectVersion"));
                });
    }

    @Test
    public void testRestApiHttps() throws Exception {
        SSLContext sslContext =
                SSLUtils.createSSLContext(getPath("client_keystore.jks"), CLIENT_KEYSTORE_PASSWORD);

        HttpsURLConnection conn =
                (HttpsURLConnection)
                        new java.net.URL("https://localhost:" + HTTPS_PORT + "/overview")
                                .openConnection();
        conn.setSSLSocketFactory(sslContext.getSocketFactory());

        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            Assertions.assertEquals(200, conn.getResponseCode());
            String response = in.lines().collect(Collectors.joining());
            Assertions.assertTrue(response.contains("projectVersion"));
        } finally {
            conn.disconnect();
        }
    }

    @Test
    public void testRestApiHttpsFailed() {
        Assertions.assertThrows(
                SSLHandshakeException.class,
                () -> {
                    java.net.URL url =
                            new java.net.URL("https://localhost:" + HTTPS_PORT + "/overview");
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.getResponseCode();
                });
    }

    @Test
    public void testFinishedJobsApi() throws Exception {
        JobInformation jobInformation = getSeatunnelServer("testFinishedJobs");
        int jobNum = 7;
        int pageSize = 5;
        long jobId = 1000L;
        for (int i = 0; i < jobNum; i++) {
            startJob(i + jobId, "fake_to_console.conf", jobInformation);
        }

        // wait until all jobs are finished
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertEquals(
                                        jobNum,
                                        jobInformation
                                                .coordinatorService
                                                .getJobCountMetrics()
                                                .getFinishedJobCount()));

        // pagination test
        // page 1
        restApiRequestHttp(
                "http://localhost:" + HTTP_PORT2 + "/finished-jobs?page=1&rows=" + pageSize,
                (code, content) -> {
                    Assertions.assertEquals(200, code);
                    JsonObject resultJson = (JsonObject) Json.parse(content);
                    Assertions.assertTrue(
                            resultJson.get("data") != null && resultJson.get("total") != null);
                    int total = resultJson.getInt("total", 0);
                    JsonArray data = (JsonArray) resultJson.get("data");
                    Assertions.assertTrue(total == jobNum && data.size() == pageSize);
                });
        // page 2
        restApiRequestHttp(
                "http://localhost:" + HTTP_PORT2 + "/finished-jobs?page=2&rows=" + pageSize,
                (code, content) -> {
                    Assertions.assertEquals(200, code);
                    JsonObject resultJson = (JsonObject) Json.parse(content);
                    Assertions.assertTrue(
                            resultJson.get("data") != null && resultJson.get("total") != null);
                    int total = resultJson.getInt("total", 0);
                    JsonArray data = (JsonArray) resultJson.get("data");
                    Assertions.assertTrue(total == jobNum && data.size() == 2);
                });
        // no pagination test
        restApiRequestHttp(
                "http://localhost:" + HTTP_PORT2 + "/finished-jobs",
                (code, content) -> {
                    Assertions.assertEquals(200, code);
                    JsonArray resultJson = (JsonArray) Json.parse(content);
                    Assertions.assertTrue(resultJson != null);
                    Assertions.assertTrue(resultJson.size() == jobNum);
                });
        shutdown(jobInformation);
    }

    @Test
    public void testRunningJobsApi() throws Exception {
        JobInformation jobInformation = getSeatunnelServer("testRunningJobs");
        int jobNum = 20;
        int pageSize = 5;
        long jobId = 2000L;
        for (int i = 0; i < jobNum; i++) {
            startJob(i + jobId, "stream_fake_to_console.conf", jobInformation);
        }

        // wait until all jobs are running
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertEquals(
                                        jobNum,
                                        jobInformation
                                                .coordinatorService
                                                .getRunningJobMetrics()
                                                .size()));

        // pagination test
        restApiRequestHttp(
                "http://localhost:" + HTTP_PORT2 + "/running-jobs?page=1&rows=" + pageSize,
                (code, content) -> {
                    Assertions.assertEquals(200, code);
                    JsonObject resultJson = (JsonObject) Json.parse(content);
                    Assertions.assertTrue(
                            resultJson.get("data") != null && resultJson.get("total") != null);
                    int total = resultJson.getInt("total", 0);
                    JsonArray data = (JsonArray) resultJson.get("data");
                    Assertions.assertTrue(total == jobNum && data.size() == pageSize);
                });
        // no pagination test
        restApiRequestHttp(
                "http://localhost:" + HTTP_PORT2 + "/running-jobs",
                (code, content) -> {
                    Assertions.assertEquals(200, code);
                    JsonArray resultJson = (JsonArray) Json.parse(content);
                    Assertions.assertTrue(resultJson != null);
                    Assertions.assertTrue(resultJson.size() == jobNum);
                });
        shutdown(jobInformation);
    }

    @Test
    public void testPageNumberOutOfRange() throws Exception {
        JobInformation jobInformation = getSeatunnelServer("testPageNumberOutOfRange");
        int jobNum = 7;
        int pageSize = 5;
        long jobId = 3000L;
        for (int i = 0; i < jobNum; i++) {
            startJob(i + jobId, "fake_to_console.conf", jobInformation);
        }

        // wait until all jobs are finished
        await().atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                assertEquals(
                                        jobNum,
                                        jobInformation
                                                .coordinatorService
                                                .getJobCountMetrics()
                                                .getFinishedJobCount()));

        restApiRequestHttp(
                "http://localhost:" + HTTP_PORT2 + "/finished-jobs?page=10&rows=" + pageSize,
                (code, content) -> {
                    Assertions.assertEquals(400, code);
                    Assertions.assertTrue(content.contains("Page number exceeds total pages"));
                });
        shutdown(jobInformation);
    }

    private void restApiRequestHttp(String url, RestApiRequestCallback callback) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new java.net.URL(url).openConnection();
        if (conn.getResponseCode() != 200) {
            try (BufferedReader in =
                    new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
                String response = in.lines().collect(Collectors.joining());
                if (callback != null) {
                    callback.callback(conn.getResponseCode(), response);
                }
            } finally {
                conn.disconnect();
            }
        } else {
            try (BufferedReader in =
                    new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String response = in.lines().collect(Collectors.joining());
                if (callback != null) {
                    callback.callback(conn.getResponseCode(), response);
                }
            } finally {
                conn.disconnect();
            }
        }
    }

    private void startJob(Long jobId, String path, JobInformation jobInformation) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobId.toString(), jobId);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        jobInformation.healcastInstance.node.nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data =
                jobInformation
                        .healcastInstance
                        .node
                        .nodeEngine
                        .getSerializationService()
                        .toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                jobInformation.coordinatorService.submitJob(
                        jobId, data, jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }

    private JobInformation getSeatunnelServer(String testClassName) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(
                TestUtils.getClusterName("RestApiHttpsTest_" + testClassName));
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(true);
        httpConfig.setPort(HTTP_PORT2);
        httpConfig.setHttpsPort(HTTPS_PORT2);
        httpConfig.setEnableHttps(false);

        HazelcastInstanceImpl healcastInstance =
                SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

        SeaTunnelServer server1 =
                healcastInstance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        CoordinatorService coordinatorService = server1.getCoordinatorService();
        Assertions.assertTrue(coordinatorService.isCoordinatorActive());
        return new JobInformation(healcastInstance, coordinatorService, server1);
    }

    private void shutdown(JobInformation jobInformation) {
        if (jobInformation.server != null) {
            jobInformation.server.shutdown(true);
        }
        if (jobInformation.healcastInstance != null) {
            jobInformation.healcastInstance.shutdown();
        }
    }

    private static class JobInformation {

        public final HazelcastInstanceImpl healcastInstance;
        public final CoordinatorService coordinatorService;
        public final SeaTunnelServer server;

        public JobInformation(
                HazelcastInstanceImpl coordinatorServiceTest,
                CoordinatorService coordinatorService,
                SeaTunnelServer server) {
            this.healcastInstance = coordinatorServiceTest;
            this.coordinatorService = coordinatorService;
            this.server = server;
        }
    }
}
