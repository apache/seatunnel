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

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

class OptionRulesApiTest {

    private static final Logger LOG = LogManager.getLogger(OptionRulesApiTest.class);
    private static final int HTTP_PORT = 18082;
    private static final int HAZELCAST_PORT = TestUtils.getAvailablePort(100);

    private static HazelcastInstanceImpl instance;
    private static Config originalHazelcastConfig;
    private static boolean originalHttpEnabled;
    private static int originalHttpPort;
    private static boolean originalEnableDynamicPort;
    private static ExecutionMode originalExecutionMode;

    @BeforeAll
    static void before() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        originalHazelcastConfig = seaTunnelConfig.getHazelcastConfig();
        originalHttpEnabled = httpConfig.isEnabled();
        originalHttpPort = httpConfig.getPort();
        originalEnableDynamicPort = httpConfig.isEnableDynamicPort();
        originalExecutionMode = seaTunnelConfig.getEngineConfig().getMode();

        String hazelcastYaml = getHazelcastConfig();
        Config hazelcastConfig = Config.loadFromString(hazelcastYaml);
        String clusterName = TestUtils.getClusterName("OptionRulesApiTest");
        hazelcastConfig.setClusterName(clusterName);
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        httpConfig.setEnabled(true);
        httpConfig.setPort(HTTP_PORT);
        httpConfig.setEnableDynamicPort(false);

        logStartupContext(clusterName, hazelcastYaml, httpConfig);

        try {
            instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            LOG.info(
                    "OptionRulesApiTest started successfully. clusterName={}, localMemberAddress={},"
                            + " actualHazelcastPort={}, clusterSize={}",
                    clusterName,
                    instance.getCluster().getLocalMember().getAddress(),
                    instance.getCluster().getLocalMember().getAddress().getPort(),
                    instance.getCluster().getMembers().size());
        } catch (Throwable t) {
            LOG.error(
                    "OptionRulesApiTest failed to start. clusterName={}, hazelcastBasePort={}, httpPort={},"
                            + " hazelcastPortInUse={}, httpPortInUse={}",
                    clusterName,
                    HAZELCAST_PORT,
                    HTTP_PORT,
                    isPortInUse(HAZELCAST_PORT),
                    isPortInUse(HTTP_PORT),
                    t);
            logCauseChain(t);
            logThreadDump();
            throw t;
        }
    }

    @Test
    void shouldReturnOptionRulesViaJettyRest() {
        given().queryParam("type", "source")
                .queryParam("plugin", "FakeSource")
                .when()
                .get("http://localhost:" + HTTP_PORT + RestConstant.REST_URL_OPTION_RULES)
                .then()
                .statusCode(200)
                .body("engineType", equalTo("seatunnel"))
                .body("pluginType", equalTo("source"))
                .body("pluginName", equalTo("FakeSource"))
                .body("optionRule.optionalOptions.key", hasItem("row.num"))
                .body("optionRule.requiredOptions.ruleType", hasItem("EXCLUSIVE"))
                .body("optionRule.requiredOptions.ruleType", hasItem("CONDITIONAL"))
                .body("optionRule.conditionRules.size()", equalTo(0));
    }

    @Test
    void shouldReturnOptionRulesViaHazelcastRest() {
        int hazelcastPort = instance.getCluster().getLocalMember().getAddress().getPort();
        given().queryParam("type", "source")
                .queryParam("plugin", "FakeSource")
                .when()
                .get(
                        "http://localhost:"
                                + hazelcastPort
                                + RestConstant.CONTEXT_PATH
                                + RestConstant.REST_URL_OPTION_RULES)
                .then()
                .statusCode(200)
                .body("pluginName", equalTo("FakeSource"))
                .body("optionRule.requiredOptions.ruleType", hasItem("EXCLUSIVE"))
                .body("optionRule.conditionRules.size()", equalTo(0));
    }

    @Test
    void shouldRejectMissingTypeParameter() {
        given().queryParam("plugin", "FakeSource")
                .when()
                .get("http://localhost:" + HTTP_PORT + RestConstant.REST_URL_OPTION_RULES)
                .then()
                .statusCode(400)
                .body("message", containsString("Parameter 'type' cannot be empty"));
    }

    @Test
    void shouldReturnNotFoundForUnknownPlugin() {
        given().queryParam("type", "source")
                .queryParam("plugin", "MissingPlugin")
                .when()
                .get("http://localhost:" + HTTP_PORT + RestConstant.REST_URL_OPTION_RULES)
                .then()
                .statusCode(404)
                .body("message", containsString("MissingPlugin"));
    }

    @AfterAll
    static void after() {
        if (instance != null) {
            instance.shutdown();
        }
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(originalHazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(originalExecutionMode);
        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(originalHttpEnabled);
        httpConfig.setPort(originalHttpPort);
        httpConfig.setEnableDynamicPort(originalEnableDynamicPort);

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.close();
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
                + "        DATA:\n"
                + "          enabled: true\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - localhost\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: "
                + HAZELCAST_PORT
                + "\n"
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 100\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n";
    }

    private static void logStartupContext(
            String clusterName, String hazelcastYaml, HttpConfig httpConfig) {
        LOG.info(
                "OptionRulesApiTest startup context: clusterName={}, hazelcastBasePort={},"
                        + " hazelcastPortCount=100, hazelcastJoinPortTryCount=100, httpPort={},"
                        + " httpDynamicPortEnabled={}, hazelcastBasePortInUse={}, httpPortInUse={}",
                clusterName,
                HAZELCAST_PORT,
                HTTP_PORT,
                httpConfig.isEnableDynamicPort(),
                isPortInUse(HAZELCAST_PORT),
                isPortInUse(HTTP_PORT));
        LOG.info("OptionRulesApiTest Hazelcast YAML:\n{}", hazelcastYaml);
    }

    private static void logCauseChain(Throwable throwable) {
        Throwable current = throwable;
        int depth = 0;
        while (current != null) {
            LOG.error(
                    "OptionRulesApiTest startup failure cause[{}]: {}: {}",
                    depth,
                    current.getClass().getName(),
                    current.getMessage());
            current = current.getCause();
            depth++;
        }
    }

    private static void logThreadDump() {
        LOG.error("OptionRulesApiTest thread dump start");
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            Thread thread = entry.getKey();
            StackTraceElement[] stackTrace = entry.getValue();
            LOG.error(
                    "Thread dump: name='{}', state={}, daemon={}, interrupted={}",
                    thread.getName(),
                    thread.getState(),
                    thread.isDaemon(),
                    thread.isInterrupted());
            for (StackTraceElement stackTraceElement : stackTrace) {
                LOG.error("    at {}", stackTraceElement);
            }
        }
        LOG.error("OptionRulesApiTest thread dump end");
    }

    private static boolean isPortInUse(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port);
                DatagramSocket datagramSocket = new DatagramSocket(port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }
}
