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

package org.apache.seatunnel.e2e.common.container.flink;

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * This class is the base class of FlinkEnvironment test for new seatunnel connector API. The before
 * method will create a Flink cluster, and after method will close the Flink cluster. You can use
 * {@link Flink20Container#executeJob} to submit a seatunnel config and run a seatunnel job.
 */
@NoArgsConstructor
@AutoService(TestContainer.class)
public class Flink20Container extends AbstractTestFlinkContainer {

    @Override
    public TestContainerId identifier() {
        return TestContainerId.FLINK_1_20;
    }

    @Override
    protected String getDockerImage() {
        return "tyrantlucifer/flink:1.20.1-scala_2.12_hadoop27";
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-flink-starter" + File.separator + "seatunnel-flink-20-starter";
    }

    @Override
    protected String getStartShellName() {
        return "start-seatunnel-flink-20-connector-v2.sh";
    }

    @Override
    protected String getConnectorType() {
        return "seatunnel";
    }

    @Override
    protected String getConnectorModulePath() {
        return "seatunnel-connectors-v2";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "connector-";
    }

    @Override
    protected List<String> getFlinkProperties() {
        // CRITICAL: For Flink 1.20.1, we need to completely replace the config file
        // instead of appending to it, because SnakeYAML requires the entire file
        // to start with a YAML document marker.
        //
        // We use a special marker that will be processed by our custom startup script

        List<String> properties =
                Arrays.asList(
                        "# SEATUNNEL_FLINK20_CONFIG_REPLACE_START",
                        "---", // YAML document start required by SnakeYAML engine
                        "# SeaTunnel Flink 1.20.1 Complete Configuration",
                        "# Generated to ensure YAML compliance with SnakeYAML engine",
                        "",
                        "# Memory Configuration",
                        "jobmanager.memory.process.size: 1600m",
                        "taskmanager.memory.process.size: 1728m",
                        "taskmanager.memory.flink.size: 1280m",
                        "",
                        "# Network Buffer Configuration - Fix for insufficient network buffers",
                        "taskmanager.memory.network.fraction: 0.2",
                        "taskmanager.memory.network.min: 128mb",
                        "taskmanager.memory.network.max: 512mb",
                        "",
                        "# Network Configuration",
                        "jobmanager.rpc.address: jobmanager",
                        "taskmanager.numberOfTaskSlots: 10",
                        "",
                        "# Execution Configuration",
                        "parallelism.default: 4",
                        "",
                        "# JVM Configuration",
                        "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false",
                        "# SEATUNNEL_FLINK20_CONFIG_REPLACE_END");

        // Debug logging
        System.out.println("=== Flink20Container Debug Information ===");
        System.out.println("Docker Image: " + getDockerImage());
        System.out.println(
                "Using config replacement mode for Flink 1.20.1 SnakeYAML compatibility");
        String joinedProperties = String.join("\n", properties);
        System.out.println("Final FLINK_PROPERTIES environment variable content:");
        System.out.println("--- START FLINK_PROPERTIES ---");
        System.out.println(joinedProperties);
        System.out.println("--- END FLINK_PROPERTIES ---");
        System.out.println("=== End Debug Information ===");

        return properties;
    }

    @Override
    public void startUp() throws Exception {
        // Override startup to handle Flink 1.20.1 specific YAML configuration requirements
        final String dockerImage = getDockerImage();
        final String properties = String.join("\n", getFlinkProperties());

        System.out.println("=== Flink20Container Custom Startup ===");
        System.out.println("Starting Flink 1.20.1 with custom configuration handling");

        jobManager =
                new org.testcontainers.containers.GenericContainer<>(dockerImage)
                        .withCommand("sh", "-c", createJobManagerStartupCommand())
                        .withNetwork(NETWORK)
                        .withNetworkAliases("jobmanager")
                        .withExposedPorts()
                        .withEnv("FLINK_PROPERTIES", properties)
                        .withLogConsumer(
                                new org.testcontainers.containers.output.Slf4jLogConsumer(
                                        org.testcontainers.utility.DockerLoggerFactory.getLogger(
                                                dockerImage + ":jobmanager")))
                        .waitingFor(
                                new org.testcontainers.containers.wait.strategy
                                                .LogMessageWaitStrategy()
                                        .withRegEx(".*Starting the resource manager.*")
                                        .withStartupTimeout(java.time.Duration.ofMinutes(2)))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                org.testcontainers.containers.BindMode.READ_WRITE);

        copySeaTunnelStarterToContainer(jobManager);
        copySeaTunnelStarterLoggingToContainer(jobManager);

        jobManager.setPortBindings(java.util.Arrays.asList(String.format("%s:%s", 8081, 8081)));

        taskManager =
                new org.testcontainers.containers.GenericContainer<>(dockerImage)
                        .withCommand("sh", "-c", createTaskManagerStartupCommand())
                        .withNetwork(NETWORK)
                        .withNetworkAliases("taskmanager")
                        .withEnv("FLINK_PROPERTIES", properties)
                        .dependsOn(jobManager)
                        .withLogConsumer(
                                new org.testcontainers.containers.output.Slf4jLogConsumer(
                                        org.testcontainers.utility.DockerLoggerFactory.getLogger(
                                                dockerImage + ":taskmanager")))
                        .waitingFor(
                                new org.testcontainers.containers.wait.strategy
                                                .LogMessageWaitStrategy()
                                        .withRegEx(
                                                ".*Successful registration at resource manager.*")
                                        .withStartupTimeout(java.time.Duration.ofMinutes(2)))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                org.testcontainers.containers.BindMode.READ_WRITE);

        org.testcontainers.lifecycle.Startables.deepStart(java.util.stream.Stream.of(jobManager))
                .join();

        org.testcontainers.lifecycle.Startables.deepStart(java.util.stream.Stream.of(taskManager))
                .join();

        // execute extra commands
        executeExtraCommands(jobManager);

        System.out.println("=== Flink20Container Startup Complete ===");
    }

    private String createJobManagerStartupCommand() {
        // Create a complete startup command for JobManager that avoids shell operator issues
        return createFlink20StartupScript()
                + "\n"
                + "echo 'Starting Flink JobManager...'\n"
                + "exec /docker-entrypoint.sh jobmanager\n";
    }

    private String createTaskManagerStartupCommand() {
        // Create a complete startup command for TaskManager that avoids shell operator issues
        return createFlink20StartupScript()
                + "\n"
                + "echo 'Starting Flink TaskManager...'\n"
                + "exec /docker-entrypoint.sh taskmanager\n";
    }

    private String createFlink20StartupScript() {
        // Create a script that properly handles YAML configuration replacement
        return "#!/bin/bash\n"
                + "set -e\n"
                + "echo 'SeaTunnel Flink 1.20.1 custom startup script'\n"
                + "echo 'Handling YAML configuration for SnakeYAML compatibility'\n"
                + "\n"
                + "CONF_DIR=\"${FLINK_HOME}/conf\"\n"
                + "CONF_FILE=\"${CONF_DIR}/flink-conf.yaml\"\n"
                + "CONFIG_FILE=\"${CONF_DIR}/config.yaml\"\n"
                + "\n"
                + "echo 'Original configuration directory:'\n"
                + "ls -la \"${CONF_DIR}\"\n"
                + "\n"
                + "if [ -n \"${FLINK_PROPERTIES}\" ]; then\n"
                + "  if echo \"${FLINK_PROPERTIES}\" | grep -q 'SEATUNNEL_FLINK20_CONFIG_REPLACE_START'; then\n"
                + "    echo 'Replacing configuration files with YAML-compliant content'\n"
                + "    \n"
                + "    # Extract the actual config content (between markers)\n"
                + "    # Use printf to handle special characters and quotes properly\n"
                + "    printf '%s\\n' \"${FLINK_PROPERTIES}\" | sed -n '/SEATUNNEL_FLINK20_CONFIG_REPLACE_START/,/SEATUNNEL_FLINK20_CONFIG_REPLACE_END/p' | sed '1d;$d' > \"${CONF_FILE}\"\n"
                + "    \n"
                + "    # Copy to config.yaml as well\n"
                + "    cp \"${CONF_FILE}\" \"${CONFIG_FILE}\"\n"
                + "    \n"
                + "    echo 'Configuration files replaced successfully'\n"
                + "  else\n"
                + "    echo 'Using standard append mode'\n"
                + "    echo \"${FLINK_PROPERTIES}\" >> \"${CONF_FILE}\"\n"
                + "    [ -f \"${CONFIG_FILE}\" ] && echo \"${FLINK_PROPERTIES}\" >> \"${CONFIG_FILE}\"\n"
                + "  fi\n"
                + "else\n"
                + "  echo 'No FLINK_PROPERTIES provided'\n"
                + "fi\n"
                + "\n"
                + "echo 'Final configuration files:'\n"
                + "echo '=== flink-conf.yaml ==='\n"
                + "cat \"${CONF_FILE}\" 2>/dev/null || echo 'flink-conf.yaml not found'\n"
                + "echo '=== config.yaml ==='\n"
                + "cat \"${CONFIG_FILE}\" 2>/dev/null || echo 'config.yaml not found'\n"
                + "echo '=== End configuration files ==='\n";
    }
}
