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
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.config.Config;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.stream.Collectors;

/** Test for Rest API with HTTPS. */
@DisabledOnOs(OS.WINDOWS)
public class RestApiHttpsTest extends AbstractSeaTunnelServerTest {
    private static final int HTTP_PORT = 28080;
    private static final int HTTPS_PORT = 28443;
    private static final String SERVER_KEYSTORE_PASSWORD = "server_keystore_password";
    private static final String CLIENT_KEYSTORE_PASSWORD = "client_keystore_password";

    @BeforeAll
    public void setUp() {
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
        HttpURLConnection conn =
                (HttpURLConnection)
                        new java.net.URL("http://localhost:" + HTTP_PORT + "/overview")
                                .openConnection();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {

            Assertions.assertEquals(200, conn.getResponseCode());
            String response = in.lines().collect(Collectors.joining());
            Assertions.assertTrue(response.contains("projectVersion"));
        } finally {
            conn.disconnect();
        }
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
}
