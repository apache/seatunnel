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

package org.apache.seatunnel.engine.e2e.joblog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.engine.e2e.SeaTunnelEngineContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import io.restassured.response.Response;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

/**
 * Verifies that each node's logLink URL uses that node's own REST port, not the master's port.
 *
 * <p>Bug: when nodes have different REST ports, allLogNameList() always used the master's port for
 * every URL. Fix: GetNodeHttpPortOperation fetches the REST port from the target node itself.
 */
public class JobLogUrlPortIT extends SeaTunnelEngineContainer {

    private static final int MASTER_HTTP_PORT = 8080;
    private static final int WORKER_HTTP_PORT = 8081;

    private static final Path BIN_PATH = Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL);
    private static final Path CONFIG_PATH = Paths.get(SEATUNNEL_HOME, "config");
    private static final Path HADOOP_JAR_PATH =
            Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar");

    private static final String MULTIPORT_RESOURCES =
            PROJECT_ROOT_PATH
                    + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base"
                    + "/src/test/resources/";

    private GenericContainer<?> masterServer;
    private GenericContainer<?> workerServer;
    private final Network CLUSTER_NETWORK = Network.newNetwork();

    @Override
    @BeforeEach
    public void startUp() throws Exception {
        masterServer = createServer("server", "job-log-multiport/seatunnel-master.yaml");
        workerServer = createServer("secondServer", "job-log-multiport/seatunnel-worker.yaml");

        // wait until both members joined
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Response response =
                                    given().get(
                                                    "http://"
                                                            + masterServer.getHost()
                                                            + ":"
                                                            + masterServer.getMappedPort(5801)
                                                            + "/hazelcast/rest/cluster");
                            response.then().statusCode(200);
                            Assertions.assertEquals(
                                    2, response.jsonPath().getList("members").size());
                        });
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        if (masterServer != null) {
            masterServer.close();
        }
        if (workerServer != null) {
            workerServer.close();
        }
        CLUSTER_NETWORK.close();
    }

    /**
     * Verifies that the /logs?format=JSON response covers all cluster nodes and that every logLink
     * URL is reachable (HTTP 200). If a logLink used the master's port instead of the worker's
     * port, the curl would fail — proving the fix is effective.
     */
    @Test
    public void testLogUrlUsesPerNodePort() throws IOException, InterruptedException {
        // wait for seatunnel.log to appear on both nodes
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Container.ExecResult r1 =
                                    masterServer.execInContainer(
                                            "sh",
                                            "-c",
                                            "ls /tmp/seatunnel/logs/ 2>/dev/null | head -1");
                            Container.ExecResult r2 =
                                    workerServer.execInContainer(
                                            "sh",
                                            "-c",
                                            "ls /tmp/seatunnel/logs/ 2>/dev/null | head -1");
                            Assertions.assertFalse(
                                    r1.getStdout().isBlank(), "master has no log files yet");
                            Assertions.assertFalse(
                                    r2.getStdout().isBlank(), "worker has no log files yet");
                        });

        // call /logs?format=JSON from inside the master container
        Container.ExecResult logsResult =
                masterServer.execInContainer(
                        "sh",
                        "-c",
                        "curl -sf 'http://localhost:" + MASTER_HTTP_PORT + "/logs?format=JSON'");

        Assertions.assertEquals(
                0,
                logsResult.getExitCode(),
                "curl /logs?format=JSON failed: " + logsResult.getStderr());

        String jsonBody = logsResult.getStdout();
        Assertions.assertFalse(jsonBody.isBlank(), "Log list JSON is empty");

        ArrayNode logArray = JsonUtils.parseArray(jsonBody);
        Assertions.assertFalse(logArray.isEmpty(), "No logs returned from master");

        // Step 1: all cluster nodes must be represented in the response
        Set<String> respondedHosts = new HashSet<>();
        for (JsonNode entry : logArray) {
            respondedHosts.add(extractHost(entry.get("node").asText()));
        }
        Assertions.assertEquals(
                2,
                respondedHosts.size(),
                "Expected both cluster nodes in /logs response, got: " + respondedHosts);

        // Step 2: every logLink URL must return HTTP 200 — wrong port means connection refused
        for (JsonNode entry : logArray) {
            String link = entry.get("logLink").asText();
            Container.ExecResult curlResult =
                    masterServer.execInContainer(
                            "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", link);
            Assertions.assertEquals(
                    "200", curlResult.getStdout(), "logLink not reachable (wrong port?): " + link);
        }
    }

    private GenericContainer<?> createServer(String networkAlias, String seatunnelYamlRelPath)
            throws IOException, InterruptedException {
        GenericContainer<?> container =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(CLUSTER_NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(ContainerUtil.adaptPathForWin(BIN_PATH.toString()))
                        .withNetworkAliases(networkAlias)
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());

        copySeaTunnelStarterToContainer(container);
        container.setExposedPorts(Collections.singletonList(5801));

        // base config (hazelcast + default seatunnel)
        container.withCopyFileToContainer(
                MountableFile.forHostPath(MULTIPORT_RESOURCES), CONFIG_PATH.toString());

        // cluster hazelcast config (both nodes use the same topology config)
        container.withCopyFileToContainer(
                MountableFile.forHostPath(MULTIPORT_RESOURCES + "job-log-multiport/hazelcast.yaml"),
                CONFIG_PATH.resolve("hazelcast.yaml").toString());

        // node-specific seatunnel config (different REST port per node)
        container.withCopyFileToContainer(
                MountableFile.forHostPath(MULTIPORT_RESOURCES + seatunnelYamlRelPath),
                CONFIG_PATH.resolve("seatunnel.yaml").toString());

        // log4j2 config that writes per-job log files to /tmp/seatunnel/logs/
        container.withCopyFileToContainer(
                MountableFile.forHostPath(
                        MULTIPORT_RESOURCES + "job-log-multiport/log4j2.properties"),
                CONFIG_PATH.resolve("log4j2.properties").toString());

        container.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                HADOOP_JAR_PATH.toString());

        container.start();
        executeExtraCommands(container);
        ContainerUtil.copyConnectorJarToContainer(
                container,
                "/fakesource_to_console.conf",
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);

        return container;
    }

    /** Extracts the host part from a "host:port" string. */
    private String extractHost(String nodeField) {
        if (nodeField == null) {
            return "";
        }
        int colon = nodeField.lastIndexOf(':');
        return colon >= 0 ? nodeField.substring(0, colon) : nodeField;
    }
}
