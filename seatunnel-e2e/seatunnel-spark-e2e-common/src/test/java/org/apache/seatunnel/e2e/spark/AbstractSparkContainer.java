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

package org.apache.seatunnel.e2e.spark;

import static org.apache.seatunnel.e2e.ContainerUtil.PROJECT_ROOT_PATH;
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
import java.util.List;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractSparkContainer {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkContainer.class);
    protected static final String START_ROOT_MODULE_NAME = "seatunnel-core";

    private static final String SEATUNNEL_HOME = "/tmp/spark/seatunnel";
    private static final String DOCKER_IMAGE = "bitnami/spark:2.4.3";
    public static final Network NETWORK = Network.newNetwork();

    protected GenericContainer<?> master;

    protected final String startShellName;

    protected final String startModuleName;

    protected final String startModulePath;

    protected final String connectorsRootPath;

    protected final String connectorType;

    protected final String connectorNamePrefix;

    public AbstractSparkContainer(
                                  String startShellName,
                                  String startModuleNameInSeaTunnelCore,
                                  String connectorsRootPath,
                                  String connectorType,
                                  String connectorNamePrefix) {
        this.startShellName = startShellName;
        this.connectorsRootPath = connectorsRootPath;
        this.connectorType = connectorType;
        this.connectorNamePrefix = connectorNamePrefix;
        String[] moudules = startModuleNameInSeaTunnelCore.split(File.separator);
        this.startModuleName = moudules[moudules.length - 1];
        this.startModulePath = PROJECT_ROOT_PATH + File.separator +
            START_ROOT_MODULE_NAME + File.separator + startModuleNameInSeaTunnelCore;
    }

    @BeforeAll
    public void before() {
        master = new GenericContainer<>(DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("spark-master")
            .withExposedPorts()
            .withEnv("SPARK_MODE", "master")
            .withLogConsumer(new Slf4jLogConsumer(LOG));
        // In most case we can just use standalone mode to execute a spark job, if we want to use cluster mode, we need to
        // start a worker.
        Startables.deepStart(Stream.of(master)).join();
        copySeaTunnelStarter(master, startModuleName, startModulePath, SEATUNNEL_HOME, startShellName);
        LOG.info("Spark container started");
    }

    @AfterAll
    public void close() {
        if (master != null) {
            master.stop();
        }
    }

    public Container.ExecResult executeSeaTunnelSparkJob(String confFile) throws IOException, InterruptedException {
        final String confInContainerPath = copyConfigFileToContainer(master, confFile);
        // copy connectors
        copyConnectorJarToContainer(master, confFile, connectorsRootPath, connectorNamePrefix, connectorType, SEATUNNEL_HOME);

        // Running IT use cases under Windows requires replacing \ with /
        String conf = confInContainerPath.replaceAll("\\\\", "/");
        final List<String> command = new ArrayList<>();
        command.add(Paths.get(SEATUNNEL_HOME, "bin", startShellName).toString());
        command.add("--master");
        command.add("local");
        command.add("--deploy-mode");
        command.add("client");
        command.add("--config " + conf);

        Container.ExecResult execResult = master.execInContainer("bash", "-c", String.join(" ", command));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        return execResult;
    }
}
