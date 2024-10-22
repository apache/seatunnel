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

package org.apache.seatunnel.e2e.common.container.spark;

import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public abstract class AbstractTestSparkContainer extends AbstractTestContainer {

    private static final String DEFAULT_DOCKER_IMAGE = "bitnami/spark:2.4.6";

    protected GenericContainer<?> master;

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    public void startUp() throws Exception {
        master =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withNetworkAliases("spark-master")
                        .withExposedPorts()
                        .withEnv("SPARK_MODE", "master")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(getDockerImage())))
                        .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(".*Master: Starting Spark master at.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        copySeaTunnelStarterToContainer(master);
        copySeaTunnelStarterLoggingToContainer(master);

        // In most case we can just use standalone mode to execute a spark job, if we want to use
        // cluster mode, we need to
        // start a worker.
        Startables.deepStart(Stream.of(master)).join();
        // execute extra commands
        executeExtraCommands(master);
    }

    @Override
    public void tearDown() throws Exception {
        if (master != null) {
            master.stop();
        }
    }

    @Override
    protected String getSavePointCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getCancelJobCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getRestoreCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Arrays.asList("--master local", "--deploy-mode client");
    }

    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(master);
    }

    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, Collections.emptyList());
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        return executeJob(master, confFile, null, variables);
    }

    @Override
    public String getServerLogs() {
        return master.getLogs();
    }

    @Override
    public void copyFileToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(
                ContainerUtil.getResourcesFile(path).toPath(), targetPath, master);
    }
}
