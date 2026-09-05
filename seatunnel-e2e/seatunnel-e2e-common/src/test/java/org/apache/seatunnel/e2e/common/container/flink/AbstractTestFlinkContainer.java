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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
        applyJavaToolOptions(jobManager);
        copySeaTunnelStarterToContainer(jobManager);
        copySeaTunnelStarterLoggingToContainer(jobManager);

        taskManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("taskmanager")
                        .withEnv("FLINK_PROPERTIES", properties)
                        .dependsOn(jobManager)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                dockerImage + ":taskmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(
                                                ".*Successful registration at resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                BindMode.READ_WRITE);
        applyJavaToolOptions(taskManager);

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        executeExtraCommands(jobManager);
    }

    protected List<String> getFlinkProperties() {
        return DEFAULT_FLINK_PROPERTIES;
    }

    /**
     * Returns test-scoped JVM options injected through the standard launcher hook for every Java
     * process started in the Flink containers.
     *
     * @return JVM option string or {@code null} when no extra options are required
     */
    protected String getJavaToolOptions() {
        return null;
    }

    @Override
    public void tearDown() throws Exception {
        if (taskManager != null) {
            // delete the volume
            taskManager.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            taskManager.stop();
        }
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
        return jobManager.getLogs() + "\n" + taskManager.getLogs();
    }

    public String executeJobManagerInnerCommand(String command)
            throws IOException, InterruptedException {
        return jobManager.execInContainer("bash", "-c", command).getStdout();
    }

    /**
     * Executes a shell command inside the TaskManager container after the cluster has started.
     *
     * @param command shell command evaluated by bash
     * @return standard output captured from the TaskManager container
     * @throws IOException when docker exec fails
     * @throws InterruptedException when the docker exec call is interrupted
     */
    public String executeTaskManagerInnerCommand(String command)
            throws IOException, InterruptedException {
        return taskManager.execInContainer("bash", "-c", command).getStdout();
    }

    public String getJobManagerHost() {
        return jobManager.getHost();
    }

    public int getJobManagerRestPort() {
        return jobManager.getMappedPort(FLINK_REST_PORT);
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

    /**
     * Uses the standard JVM launcher environment hook so both Flink daemons and helper Java
     * processes observe the same system properties in E2E tests.
     *
     * @param container Flink runtime container being prepared before startup
     */
    protected void applyJavaToolOptions(GenericContainer<?> container) {
        String javaToolOptions = getJavaToolOptions();
        if (javaToolOptions != null && !javaToolOptions.trim().isEmpty()) {
            container.withEnv("JAVA_TOOL_OPTIONS", javaToolOptions);
        }
    }
}
