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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.shade.org.eclipse.jetty.server.Connector;
import org.apache.seatunnel.shade.org.eclipse.jetty.server.ServerConnector;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

@DisabledOnOs(OS.WINDOWS)
public class MetricsApiTest {

    private static final int HTTP_PORT = 25000;

    private static HazelcastInstanceImpl instance;
    private static SeaTunnelConfig seaTunnelConfig;
    private static boolean originalMetricEnabled;
    private static boolean originalHttpEnabled;
    private static boolean originalEnableHttps;
    private static boolean originalEnableDynamicPort;
    private static boolean originalEnableBasicAuth;
    private static int originalHttpPort;
    private static int originalPortRange;
    private static String originalContextPath;
    private static ExecutionMode originalExecutionMode;
    private static int restPort;

    @BeforeAll
    public static void before() throws Exception {
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        originalMetricEnabled =
                seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().isEnabled();
        originalHttpEnabled = httpConfig.isEnabled();
        originalEnableHttps = httpConfig.isEnableHttps();
        originalEnableDynamicPort = httpConfig.isEnableDynamicPort();
        originalEnableBasicAuth = httpConfig.isEnableBasicAuth();
        originalHttpPort = httpConfig.getPort();
        originalPortRange = httpConfig.getPortRange();
        originalContextPath = httpConfig.getContextPath();
        originalExecutionMode = seaTunnelConfig.getEngineConfig().getMode();

        seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().setEnabled(true);
        httpConfig.setEnabled(true);
        httpConfig.setEnableHttps(false);
        httpConfig.setEnableBasicAuth(false);
        httpConfig.setContextPath("");
        httpConfig.setPort(HTTP_PORT);
        httpConfig.setEnableDynamicPort(true);
        httpConfig.setPortRange(2000);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        restPort = getHttpPort(instance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME));
    }

    @Test
    public void metricsApiTest() {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                given().get(
                                                "http://localhost:"
                                                        + restPort
                                                        + RestConstant.REST_URL_METRICS)
                                        .then()
                                        .statusCode(200)
                                        .body(containsString("process_start_time_seconds"))
                                        .body(
                                                containsString(
                                                        "engine_state_store_local_owned_entries"))
                                        .body(
                                                containsString(
                                                        "engine_state_store_checkpoint_monitor_jobs")));
    }

    @AfterAll
    public static void after() {
        if (instance != null) {
            instance.shutdown();
        }
        if (seaTunnelConfig != null) {
            seaTunnelConfig
                    .getEngineConfig()
                    .getTelemetryConfig()
                    .getMetric()
                    .setEnabled(originalMetricEnabled);
            seaTunnelConfig.getEngineConfig().setMode(originalExecutionMode);
            HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
            httpConfig.setEnabled(originalHttpEnabled);
            httpConfig.setEnableHttps(originalEnableHttps);
            httpConfig.setEnableDynamicPort(originalEnableDynamicPort);
            httpConfig.setEnableBasicAuth(originalEnableBasicAuth);
            httpConfig.setPort(originalHttpPort);
            httpConfig.setPortRange(originalPortRange);
            httpConfig.setContextPath(originalContextPath);
        }
    }

    private static int getHttpPort(SeaTunnelServer seaTunnelServer) throws Exception {
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
}
