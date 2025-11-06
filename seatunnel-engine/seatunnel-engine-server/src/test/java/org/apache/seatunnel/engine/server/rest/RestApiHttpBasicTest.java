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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOGS;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_OVERVIEW;

/** Test for Rest API with Basic. */
class RestApiHttpBasicTest extends AbstractSeaTunnelServerTest {

    private static final int HTTP_PORT = 18081;
    private static final Long JOB_1 = System.currentTimeMillis() + 1L;
    private static final String USER = "admin";
    private static final String PASS = "admin";
    private static final String DOMAIN = "http://localhost:" + HTTP_PORT;

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BASIC_PREFIX = "Basic ";

    @BeforeAll
    void setUp() {
        String name = this.getClass().getName();
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(
                TestUtils.getClusterName("RestApiServletHttpBasicTest_" + name));
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(Boolean.TRUE);
        httpConfig.setPort(HTTP_PORT);

        httpConfig.setEnableBasicAuth(Boolean.TRUE);
        httpConfig.setBasicAuthUsername(USER);
        httpConfig.setBasicAuthPassword(PASS);

        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    @AfterAll
    public void after() {
        // Disable basic auth
        // Because of the ConfigProvider.locateAndGetSeaTunnelConfig() single-case,
        // if you change, other use cases will also change
        // managed via org.apache.seatunnel.engine.common.config.YamlSeaTunnelDomConfigProcessor
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnableBasicAuth(Boolean.FALSE);
        httpConfig.setBasicAuthUsername("");
        httpConfig.setBasicAuthPassword("");
    }

    @Test
    public void testRestApiOverview() throws Exception {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(DOMAIN + REST_URL_OVERVIEW);
            conn = (HttpURLConnection) url.openConnection();
            setBasicAuth(conn);

            Assertions.assertEquals(200, conn.getResponseCode());
            Assertions.assertTrue(
                    conn.getHeaderFields()
                            .get("Content-Type")
                            .toString()
                            .contains("charset=utf-8"));
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    @Test
    void testLogRestApiResponseFailure() throws IOException {
        startJob();
        HttpURLConnection conn = null;
        try {
            URL url = new URL(DOMAIN + REST_URL_LOGS + "?format=JSON");
            conn = (HttpURLConnection) url.openConnection();

            Assertions.assertEquals(401, conn.getResponseCode());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    @Test
    void testLogRestApiResponseSuccess() throws IOException {
        startJob();
        testLogRestApiResponse("JSON");
    }

    public void setBasicAuth(HttpURLConnection connection) {
        // Basic Auth
        Encoder encoder = Base64.getEncoder();
        String auth = USER + ":" + PASS;
        String token = encoder.encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty(AUTHORIZATION_HEADER, BASIC_PREFIX + token);
    }

    public void testLogRestApiResponse(String format) throws IOException {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(DOMAIN + REST_URL_LOGS + "?format=" + format);
            conn = (HttpURLConnection) url.openConnection();
            setBasicAuth(conn);

            Assertions.assertEquals(200, conn.getResponseCode());
            Assertions.assertTrue(
                    conn.getHeaderFields()
                            .get("Content-Type")
                            .toString()
                            .contains("charset=utf-8"));

            try (BufferedReader in =
                    new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                // [ {
                //  "node" : "localhost:18080",
                //  "logLink" : "http://localhost:18080/logs/job-1760939539658.log",
                //  "logName" : "job-1760939539658.log"
                // }, {
                //  "node" : "localhost:18080",
                //  "logLink" : "http://localhost:18080/logs/job-${ctx:ST-JID}.log",
                //  "logName" : "job-${ctx:ST-JID}.log"
                // } ]
                String response = in.lines().collect(Collectors.joining());
                Assertions.assertFalse(StringUtils.isBlank(response));
            }

        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private void startJob() {
        LogicalDag testLogicalDag =
                TestUtils.createTestLogicalPlan(
                        "fake_to_console.conf",
                        RestApiHttpBasicTest.JOB_1.toString(),
                        RestApiHttpBasicTest.JOB_1);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        RestApiHttpBasicTest.JOB_1,
                        "Test",
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService()
                        .submitJob(
                                RestApiHttpBasicTest.JOB_1,
                                data,
                                jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }
}
