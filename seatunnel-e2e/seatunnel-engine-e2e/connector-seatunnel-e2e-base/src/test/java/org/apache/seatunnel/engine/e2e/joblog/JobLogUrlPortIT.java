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
 * Verifies that each node's logLink URL uses that node's own REST port when nodes in the cluster
 * are configured with different REST ports.
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
        masterServer = createServer("master", "job-log-multiport/seatunnel-master.yaml");
        workerServer = createServer("worker", "job-log-multiport/seatunnel-worker.yaml");

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

    @Test
    public void testLogUrlUsesPerNodePort() throws IOException, InterruptedException {
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
                                    r1.getStdout().trim().isEmpty(), "master has no log files yet");
                            Assertions.assertFalse(
                                    r2.getStdout().trim().isEmpty(), "worker has no log files yet");
                        });

        final String[] jsonBodyHolder = {null};
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(3, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Container.ExecResult logsResult =
                                    masterServer.execInContainer(
                                            "sh",
                                            "-c",
                                            "curl -sf 'http://localhost:"
                                                    + MASTER_HTTP_PORT
                                                    + "/logs?format=JSON'");
                            Assertions.assertEquals(
                                    0,
                                    logsResult.getExitCode(),
                                    "curl /logs?format=JSON failed: " + logsResult.getStderr());
                            String body = logsResult.getStdout();
                            Assertions.assertFalse(body.trim().isEmpty(), "Log list JSON is empty");
                            ArrayNode nodes = JsonUtils.parseArray(body);
                            Assertions.assertFalse(nodes.isEmpty(), "No logs returned from master");
                            Set<String> respondedHosts = new HashSet<>();
                            for (JsonNode entry : nodes) {
                                respondedHosts.add(extractHost(entry.get("node").asText()));
                            }
                            Assertions.assertEquals(
                                    2,
                                    respondedHosts.size(),
                                    "Expected both cluster nodes in /logs response, got: "
                                            + respondedHosts);
                            jsonBodyHolder[0] = body;
                        });

        ArrayNode logArray = JsonUtils.parseArray(jsonBodyHolder[0]);
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

        container.withCopyFileToContainer(
                MountableFile.forHostPath(MULTIPORT_RESOURCES), CONFIG_PATH.toString());

        container.withCopyFileToContainer(
                MountableFile.forHostPath(MULTIPORT_RESOURCES + "job-log-multiport/hazelcast.yaml"),
                CONFIG_PATH.resolve("hazelcast.yaml").toString());

        // each node uses a different REST port, so seatunnel.yaml is node-specific
        container.withCopyFileToContainer(
                MountableFile.forHostPath(MULTIPORT_RESOURCES + seatunnelYamlRelPath),
                CONFIG_PATH.resolve("seatunnel.yaml").toString());

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
