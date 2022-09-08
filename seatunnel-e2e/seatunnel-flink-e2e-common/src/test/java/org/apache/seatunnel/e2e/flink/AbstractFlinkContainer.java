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

package org.apache.seatunnel.e2e.flink;

import static org.apache.seatunnel.e2e.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.ContainerUtil.adaptPathForWin;
import static org.apache.seatunnel.e2e.ContainerUtil.copyConfigFileToContainer;
import static org.apache.seatunnel.e2e.ContainerUtil.copyConnectorJarToContainer;
import static org.apache.seatunnel.e2e.ContainerUtil.copySeaTunnelStarter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class is the base class of FlinkEnvironment test.
 * The before method will create a Flink cluster, and after method will close the Flink cluster.
 * You can use {@link AbstractFlinkContainer#executeSeaTunnelFlinkJob} to submit a seatunnel config and run a seatunnel job.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractFlinkContainer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFlinkContainer.class);

    protected static final String START_ROOT_MODULE_NAME = "seatunnel-core";

    protected static final String SEATUNNEL_HOME = "/tmp/flink/seatunnel";

    protected final String dockerImage;

    protected final String startShellName;

    protected final String startModuleName;

    protected final String startModulePath;

    protected final String connectorsRootPath;

    protected final String connectorType;

    protected final String connectorNamePrefix;
    protected static final Network NETWORK = Network.newNetwork();

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;

    public AbstractFlinkContainer(String dockerImage,
                                  String startShellName,
                                  String startModuleNameInSeaTunnelCore,
                                  String connectorsRootPath,
                                  String connectorType,
                                  String connectorNamePrefix) {
        this.dockerImage = dockerImage;
        this.startShellName = startShellName;
        this.connectorsRootPath = connectorsRootPath;
        this.connectorType = connectorType;
        this.connectorNamePrefix = connectorNamePrefix;
        String[] moudules = startModuleNameInSeaTunnelCore.split(File.separator);
        this.startModuleName = moudules[moudules.length - 1];
        this.startModulePath = PROJECT_ROOT_PATH + File.separator +
            START_ROOT_MODULE_NAME + File.separator + startModuleNameInSeaTunnelCore;
    }

    private static final String FLINK_PROPERTIES = String.join(
        "\n",
        Arrays.asList(
            "jobmanager.rpc.address: jobmanager",
            "taskmanager.numberOfTaskSlots: 10",
            "parallelism.default: 4",
            "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false"));

    @BeforeAll
    public void before() {
        jobManager = new GenericContainer<>(dockerImage)
            .withCommand("jobmanager")
            .withNetwork(NETWORK)
            .withNetworkAliases("jobmanager")
            .withExposedPorts()
            .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
            .withLogConsumer(new Slf4jLogConsumer(LOG));

        taskManager =
            new GenericContainer<>(dockerImage)
                .withCommand("taskmanager")
                .withNetwork(NETWORK)
                .withNetworkAliases("taskmanager")
                .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                .dependsOn(jobManager)
                .withLogConsumer(new Slf4jLogConsumer(LOG));

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        copySeaTunnelStarter(jobManager, startModuleName, startModulePath, SEATUNNEL_HOME, startShellName);
        LOG.info("Flink containers are started.");
    }

    @AfterAll
    public void close() {
        if (taskManager != null) {
            taskManager.stop();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
    }

    public Container.ExecResult executeSeaTunnelFlinkJob(String confFile) throws IOException, InterruptedException {
        String confInContainerPath = copyConfigFileToContainer(jobManager, confFile);
        // copy connectors
        copyConnectorJarToContainer(jobManager, confFile, connectorsRootPath, connectorNamePrefix, connectorType, SEATUNNEL_HOME);
        return executeCommand(confInContainerPath);
    }

    protected Container.ExecResult executeCommand(String configPath) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", startShellName).toString();
        command.add(adaptPathForWin(binPath));
        command.add("--config " + adaptPathForWin(configPath));

        Container.ExecResult execResult = jobManager.execInContainer("bash", "-c", String.join(" ", command));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        return execResult;
    }
}
