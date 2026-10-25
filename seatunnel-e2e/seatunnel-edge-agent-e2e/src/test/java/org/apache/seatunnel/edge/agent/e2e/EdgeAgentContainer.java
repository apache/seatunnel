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

package org.apache.seatunnel.edge.agent.e2e;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class EdgeAgentContainer extends GenericContainer<EdgeAgentContainer> {

    private static final String AGENT_LOGGER_NAME = "edge-agent-e2e";
    private static final String JDK_IMAGE = "eclipse-temurin:11-jre";
    private static final String AGENT_HOME = "/opt/edge-agent";
    private static final String APP_LOG_FILE = AGENT_HOME + "/log/edge-agent.log";
    private static final String STARTUP_LOG_FILE = AGENT_HOME + "/edge-agent.out";
    private static final int STARTUP_TIMEOUT_SECONDS = 120;

    public EdgeAgentContainer(Network network, Path hostWorkDir) throws IOException {
        super(DockerImageName.parse(JDK_IMAGE));
        EdgeAgentDistPaths.validateAgentDistribution();

        withNetwork(network);
        withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(AGENT_LOGGER_NAME)));
        withFileSystemBind(hostWorkDir.toAbsolutePath().toString(), "/e2e", BindMode.READ_WRITE);
        withWorkingDirectory("/e2e");
        withCommand(buildStartCommand());
        waitingFor(
                Wait.forLogMessage(".*Connected and authenticated to edge ingress.*", 1)
                        .withStartupTimeout(java.time.Duration.ofSeconds(STARTUP_TIMEOUT_SECONDS)));

        copyDistributionIntoImage();
    }

    private void copyDistributionIntoImage() throws IOException {
        withCopyFileToContainer(
                MountableFile.forHostPath(EdgeAgentDistPaths.startScript()),
                AGENT_HOME + "/bin/seatunnel-edge-agent.sh");
        withCopyFileToContainer(
                MountableFile.forHostPath(EdgeAgentDistPaths.starterJar()),
                AGENT_HOME + "/starter/seatunnel-edge-agent-starter.jar");
        withCopyFileToContainer(
                MountableFile.forHostPath(EdgeAgentDistPaths.log4jConfig()),
                AGENT_HOME + "/config/log4j2.properties");

        Path loggingDir = EdgeAgentDistPaths.loggingDirectory();
        try (Stream<Path> jars = Files.list(loggingDir)) {
            jars.filter(path -> path.toString().endsWith(".jar"))
                    .forEach(
                            jar ->
                                    withCopyFileToContainer(
                                            MountableFile.forHostPath(jar),
                                            AGENT_HOME
                                                    + "/starter/logging/"
                                                    + jar.getFileName().toString()));
        }
    }

    private static String[] buildStartCommand() {
        String shellCommand =
                "chmod +x "
                        + AGENT_HOME
                        + "/bin/seatunnel-edge-agent.sh"
                        + " && mkdir -p "
                        + AGENT_HOME
                        + "/log /e2e"
                        + " && export EDGE_AGENT_CONFIG=/e2e/agent.yaml"
                        + " && export EDGE_AGENT_PID_FILE=/e2e/edge-agent.pid"
                        + " && export EDGE_AGENT_LOG_DIR="
                        + AGENT_HOME
                        + "/log"
                        + " && export EDGE_AGENT_LOG_CONFIG="
                        + AGENT_HOME
                        + "/config/log4j2.properties"
                        + " && export EDGE_AGENT_STARTUP_READY_TIMEOUT_S="
                        + STARTUP_TIMEOUT_SECONDS
                        + " && "
                        + AGENT_HOME
                        + "/bin/seatunnel-edge-agent.sh start"
                        + " && exec tail -F "
                        + APP_LOG_FILE
                        + " "
                        + STARTUP_LOG_FILE;
        return new String[] {"sh", "-c", shellCommand};
    }

    public void copyLogsTo(Path hostDir) {
        if (!isRunning()) {
            return;
        }
        try {
            Files.createDirectories(hostDir);
            copyFileFromContainer(APP_LOG_FILE, hostDir.resolve("edge-agent.log").toString());
        } catch (Exception ignored) {
            // Best-effort for failed tests.
        }
        try {
            copyFileFromContainer(STARTUP_LOG_FILE, hostDir.resolve("edge-agent.out").toString());
        } catch (Exception ignored) {
            // Startup log may be absent when start failed early.
        }
    }

    @Override
    public void stop() {
        try {
            execInContainer(
                    "sh",
                    "-c",
                    "export EDGE_AGENT_PID_FILE=/e2e/edge-agent.pid && "
                            + AGENT_HOME
                            + "/bin/seatunnel-edge-agent.sh stop || true");
        } catch (Exception ignored) {
            // Best-effort graceful stop before the container is removed.
        }
        super.stop();
    }
}
