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

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is the base class of FlinkEnvironment test. The before method will create a Flink
 * cluster, and after method will close the Flink cluster. You can use {@link
 * TestContainer#executeJob} to submit a seatunnel config and run a seatunnel job.
 */
@NoArgsConstructor
@Slf4j
public abstract class AbstractTestFlinkContainer extends AbstractTestContainer {

    protected static final List<String> DEFAULT_FLINK_PROPERTIES =
            Arrays.asList(
                    "jobmanager.rpc.address: jobmanager",
                    "taskmanager.numberOfTaskSlots: 10",
                    "parallelism.default: 4",
                    "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false",
                    // One TaskManager serves every job of a test class, and each SeaTunnel job
                    // loads its connector jars through a fresh user-code ClassLoader. With the
                    // Flink default of 256mb the metaspace is exhausted part-way through a long
                    // class ("OutOfMemoryError: Metaspace"), which kills the TaskManager and fails
                    // whichever job happens to be running. Raise the process budget alongside it so
                    // the extra metaspace is not taken out of the derived heap/network/managed
                    // pools.
                    "taskmanager.memory.process.size: 2048m",
                    "taskmanager.memory.jvm-metaspace.size: 512m",
                    // CI runners host several containers at once, so a TaskManager can be starved
                    // of CPU for longer than the 50s Flink default before it answers a heartbeat.
                    // A missed heartbeat fails the job outright, because SeaTunnel jobs run with
                    // NoRestartBackoffTimeStrategy unless the job config asks for restarts.
                    "heartbeat.timeout: 120000",
                    "heartbeat.interval: 10000",
                    // limit restart attempts in e2e to avoid infinite retries
                    "restart-strategy: fixed-delay",
                    "restart-strategy.fixed-delay.attempts: 2",
                    "restart-strategy.fixed-delay.delay: 1000");

    protected static final String DEFAULT_DOCKER_IMAGE = "flink:1.13.6-scala_2.11";
    private static final int FLINK_REST_PORT = 8081;

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;
    protected final List<GenericContainer<?>> additionalTaskManagers = new ArrayList<>();

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    public void startUp() throws Exception {
        FileUtils.createNewDir(HOST_VOLUME_MOUNT_PATH);
        final String dockerImage = getDockerImage();
        final String properties = String.join("\n", getFlinkProperties());
        jobManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("jobmanager")
                        .withExposedPorts(FLINK_REST_PORT)
                        .withEnv("FLINK_PROPERTIES", properties)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(dockerImage + ":jobmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(".*Starting the resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                BindMode.READ_WRITE);
        copySeaTunnelStarterToContainer(jobManager);
        copySeaTunnelStarterLoggingToContainer(jobManager);

        taskManager = createTaskManagerContainer(dockerImage, properties, "taskmanager");

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        executeExtraCommands(jobManager);
    }

    protected List<String> getFlinkProperties() {
        return DEFAULT_FLINK_PROPERTIES;
    }

    protected GenericContainer<?> createTaskManagerContainer(
            String dockerImage, String properties, String networkAlias) {
        return new GenericContainer<>(dockerImage)
                .withCommand("taskmanager")
                .withNetwork(NETWORK)
                .withNetworkAliases(networkAlias)
                .withEnv("FLINK_PROPERTIES", properties)
                .dependsOn(jobManager)
                .withLogConsumer(
                        new Slf4jLogConsumer(
                                DockerLoggerFactory.getLogger(dockerImage + ":" + networkAlias)))
                .waitingFor(
                        new LogMessageWaitStrategy()
                                .withRegEx(".*Successful registration at resource manager.*")
                                .withStartupTimeout(Duration.ofMinutes(2)))
                .withFileSystemBind(
                        HOST_VOLUME_MOUNT_PATH, CONTAINER_VOLUME_MOUNT_PATH, BindMode.READ_WRITE);
    }

    /**
     * Replaces the default TaskManager with a fixed-size multi-JVM cluster for distribution tests.
     * This must be called before submitting a job.
     */
    public void replaceTaskManagers(
            int taskManagerCount, int slotsPerTaskManager, ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        if (taskManagerCount < 1 || slotsPerTaskManager < 1) {
            throw new IllegalArgumentException("TaskManager count and slots must be positive");
        }

        stopTaskManagers();
        String dockerImage = getDockerImage();
        String properties =
                String.join(
                        "\n",
                        getFlinkProperties().stream()
                                .map(
                                        property ->
                                                property.trim()
                                                                .startsWith(
                                                                        "taskmanager.numberOfTaskSlots:")
                                                        ? "taskmanager.numberOfTaskSlots: "
                                                                + slotsPerTaskManager
                                                        : property)
                                .collect(Collectors.toList()));

        List<GenericContainer<?>> taskManagers = new ArrayList<>();
        for (int index = 0; index < taskManagerCount; index++) {
            taskManagers.add(
                    createTaskManagerContainer(dockerImage, properties, "taskmanager-" + index));
        }
        Startables.deepStart(taskManagers.stream()).join();
        for (GenericContainer<?> manager : taskManagers) {
            extendedFactory.extend(manager);
        }

        taskManager = taskManagers.get(0);
        additionalTaskManagers.addAll(taskManagers.subList(1, taskManagers.size()));
    }

    @Override
    public void tearDown() throws Exception {
        stopTaskManagers();
        if (jobManager != null) {
            // delete the volume
            jobManager.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            jobManager.stop();
        }
        FileUtils.deleteFile(HOST_VOLUME_MOUNT_PATH);
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
        return Collections.emptyList();
    }

    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(jobManager);
        extendedFactory.extend(taskManager);
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
        return executeJob(jobManager, confFile, null, variables);
    }

    @Override
    public String getServerLogs() {
        StringBuilder logs = new StringBuilder(jobManager.getLogs());
        if (taskManager != null) {
            logs.append('\n').append(taskManager.getLogs());
        }
        for (GenericContainer<?> manager : additionalTaskManagers) {
            logs.append('\n').append(manager.getLogs());
        }
        return logs.toString();
    }

    public String executeJobManagerInnerCommand(String command)
            throws IOException, InterruptedException {
        return jobManager.execInContainer("bash", "-c", command).getStdout();
    }

    public String getJobManagerHost() {
        return jobManager.getHost();
    }

    public int getJobManagerRestPort() {
        return jobManager.getMappedPort(FLINK_REST_PORT);
    }

    /** Restarts the TaskManager process so streaming recovery tests can exercise a fresh JVM. */
    public void restartTaskManager() {
        if (taskManager == null || taskManager.getContainerId() == null) {
            throw new IllegalStateException("Flink TaskManager is not running");
        }
        taskManager
                .getDockerClient()
                .restartContainerCmd(taskManager.getContainerId())
                .withtTimeout(10)
                .exec();
    }

    private void stopTaskManagers() {
        if (taskManager != null) {
            taskManager.stop();
            taskManager = null;
        }
        for (GenericContainer<?> manager : additionalTaskManagers) {
            manager.stop();
        }
        additionalTaskManagers.clear();
    }

    @Override
    public void copyFileToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(
                ContainerUtil.getResourcesFile(path).toPath(), targetPath, jobManager);
    }

    @Override
    public void copyAbsolutePathToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(Paths.get(path), targetPath, jobManager);
    }
}
