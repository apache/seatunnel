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

package org.apache.seatunnel.e2e.common.container.seatunnel;

import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

/**
 * This class is the base class of SeatunnelEnvironment test for connector package service. The
 * before method will create a Seatunnel Zeta cluster with connector package service enabled, and
 * after method will close the Seatunnel Zeta cluster. You can use {@link
 * ConnectorPackageServiceContainer#executeJob} to submit a seatunnel config and run a seatunnel
 * job.
 */
@NoArgsConstructor
@Slf4j
@AutoService(TestContainer.class)
public class ConnectorPackageServiceContainer extends AbstractTestContainer {
    private static final String JDK_DOCKER_IMAGE = "openjdk:8";
    private static final String CLIENT_SHELL = "seatunnel.sh";
    private static final String SERVER_SHELL = "seatunnel-cluster.sh";
    private GenericContainer<?> server1;
    private GenericContainer<?> server2;
    private GenericContainer<?> server3;

    @Override
    public void startUp() throws Exception {
        server1 =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withCommand(
                                ContainerUtil.adaptPathForWin(
                                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()))
                        .withNetworkAliases("server1")
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server1);
        server1.setExposedPorts(Arrays.asList(5801));
        server1.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/connector-package-service-test-server1-resources"),
                Paths.get(SEATUNNEL_HOME, "config").toString());
        server1.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());

        server2 =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withCommand(
                                ContainerUtil.adaptPathForWin(
                                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()))
                        .withNetworkAliases("server2")
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server2);
        server2.setExposedPorts(Arrays.asList(5802));
        server2.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/connector-package-service-test-server2-resources"),
                Paths.get(SEATUNNEL_HOME, "config").toString());
        server2.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());

        server3 =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withCommand(
                                ContainerUtil.adaptPathForWin(
                                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()))
                        .withNetworkAliases("server3")
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server3);
        server3.setExposedPorts(Arrays.asList(5803));
        server3.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/connector-package-service-test-server3-resources"),
                Paths.get(SEATUNNEL_HOME, "config").toString());
        server3.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar").toString());

        Startables.deepStart(Stream.of(server1)).join();
        Startables.deepStart(Stream.of(server2)).join();
        Startables.deepStart(Stream.of(server3)).join();
        // execute extra commands
        executeExtraCommands(server1);
    }

    @Override
    public void tearDown() throws Exception {
        if (server1 != null) {
            server1.close();
        }
        if (server2 != null) {
            server2.close();
        }
        if (server3 != null) {
            server3.close();
        }
    }

    @Override
    protected String getDockerImage() {
        return JDK_DOCKER_IMAGE;
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-starter";
    }

    @Override
    protected String getStartShellName() {
        return CLIENT_SHELL;
    }

    @Override
    protected String getConnectorModulePath() {
        return "seatunnel-connectors-v2";
    }

    @Override
    protected String getConnectorType() {
        return "seatunnel";
    }

    @Override
    protected String getSavePointCommand() {
        return "-s";
    }

    @Override
    protected String getRestoreCommand() {
        return "-r";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "connector-";
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Collections.emptyList();
    }

    @Override
    public TestContainerId identifier() {
        return TestContainerId.SEATUNNEL;
    }

    @Override
    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(server1);
        extendedFactory.extend(server2);
        extendedFactory.extend(server3);
    }

    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, null);
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        return executeJob(server1, confFile, variables);
    }

    @Override
    public String getServerLogs() {
        return server1.getLogs();
    }

    @Override
    public void copyFileToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(
                ContainerUtil.getResourcesFile(path).toPath(), targetPath, server1);
    }
}
