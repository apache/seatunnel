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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/** Integration test for basic authentication in SeaTunnel Engine using command line arguments. */
public class BasicAuthenticationCommandLineIT extends SeaTunnelEngineContainer {

    private static final String HTTP = "http://";
    private static final String COLON = ":";
    private static final String USERNAME = "cliuser";
    private static final String PASSWORD = "clipassword";
    private static final String BASIC_AUTH_HEADER = "Authorization";
    private static final String BASIC_AUTH_PREFIX = "Basic ";
    private static final Path binPath = Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL);
    private static final Path config = Paths.get(SEATUNNEL_HOME, "config");
    private static final Path hadoopJar =
            Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar");
    private static final String confFile = "/fakesource_to_console.conf";
    private final Network NETWORK = Network.newNetwork();

    @Override
    @BeforeEach
    public void startUp() throws Exception {
        // Create server with basic authentication enabled via command line
        server = createServerWithCommandLineAuth();

        // Wait for server to be ready
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .until(
                        () -> {
                            try {
                                // Try to access with correct credentials
                                String credentials = USERNAME + ":" + PASSWORD;
                                String encodedCredentials =
                                        Base64.getEncoder().encodeToString(credentials.getBytes());

                                given().header(
                                                BASIC_AUTH_HEADER,
                                                BASIC_AUTH_PREFIX + encodedCredentials)
                                        .get(
                                                HTTP
                                                        + server.getHost()
                                                        + COLON
                                                        + server.getMappedPort(8080)
                                                        + "/")
                                        .then()
                                        .statusCode(200);
                                return true;
                            } catch (Exception e) {
                                return false;
                            }
                        });
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test that accessing the web UI without authentication credentials returns 401 Unauthorized.
     */
    @Test
    public void testAccessWithoutCredentials() {
        given().get(HTTP + server.getHost() + COLON + server.getMappedPort(8080) + "/")
                .then()
                .statusCode(401);
    }

    /** Test that accessing the web UI with correct credentials returns 200 OK. */
    @Test
    public void testAccessWithCorrectCredentials() {
        String credentials = USERNAME + ":" + PASSWORD;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                .get(HTTP + server.getHost() + COLON + server.getMappedPort(8080) + "/")
                .then()
                .statusCode(200)
                .contentType(containsString("text/html"))
                .body(containsString("<title>Seatunnel Engine UI</title>"));
    }

    /** Test that accessing the REST API with correct credentials returns 200 OK. */
    @Test
    public void testRestApiAccessWithCorrectCredentials() {
        String credentials = USERNAME + ":" + PASSWORD;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());

        given().header(BASIC_AUTH_HEADER, BASIC_AUTH_PREFIX + encodedCredentials)
                .get(
                        HTTP
                                + server.getHost()
                                + COLON
                                + server.getMappedPort(8080)
                                + RestConstant.REST_URL_OVERVIEW)
                .then()
                .statusCode(200)
                .body("projectVersion", notNullValue());
    }

    /**
     * Create a SeaTunnel container with basic authentication enabled via command line arguments.
     */
    private GenericContainer<?> createServerWithCommandLineAuth()
            throws IOException, InterruptedException {
        // Command with basic auth parameters
        String command =
                ContainerUtil.adaptPathForWin(binPath.toString())
                        + " --enable-basic-auth --basic-auth-username "
                        + USERNAME
                        + " --basic-auth-password "
                        + PASSWORD;

        GenericContainer<?> server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(command)
                        .withNetworkAliases("server")
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());

        copySeaTunnelStarterToContainer(server);
        server.setExposedPorts(Arrays.asList(5801, 8080));
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                config.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/cluster/"),
                config.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                hadoopJar.toString());

        server.start();

        // Execute extra commands
        executeExtraCommands(server);
        ContainerUtil.copyConnectorJarToContainer(
                server,
                confFile,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);

        return server;
    }
}
