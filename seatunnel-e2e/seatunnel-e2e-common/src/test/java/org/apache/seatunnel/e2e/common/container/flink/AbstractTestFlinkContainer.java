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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This class is the base class of FlinkEnvironment test. The before method will create a Flink
 * cluster, and after method will close the Flink cluster. You can use {@link
 * TestContainer#executeJob} to submit a seatunnel config and run a seatunnel job.
 */
@NoArgsConstructor
@Slf4j
public abstract class AbstractTestFlinkContainer extends AbstractTestContainer {

    private static final Pattern FLINK_JOB_ID_PATTERN =
            Pattern.compile("(?i)(?:JobID|Job ID)[: ]+([0-9a-f]{32})");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int FLINK_REST_PORT = 8081;
    private static final String SAVEPOINT_DIRECTORY =
            "file://" + CONTAINER_VOLUME_MOUNT_PATH + "/flink-savepoints";
    private static final long SAVEPOINT_TIMEOUT_MILLIS = 120_000L;

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

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;
    private final List<GenericContainer<?>> additionalTaskManagers = new ArrayList<>();
    private final List<ContainerExtendedFactory> extendedFactories = new ArrayList<>();

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

    /** Creates a TaskManager container that can join the currently running JobManager. */
    protected GenericContainer<?> createTaskManagerContainer(
            String dockerImage, String properties, String logName) {
        return new GenericContainer<>(dockerImage)
                .withCommand("taskmanager")
                .withNetwork(NETWORK)
                .withNetworkAliases(logName)
                .withEnv("FLINK_PROPERTIES", properties)
                .dependsOn(jobManager)
                .withLogConsumer(
                        new Slf4jLogConsumer(
                                DockerLoggerFactory.getLogger(dockerImage + ":" + logName)))
                .waitingFor(
                        new LogMessageWaitStrategy()
                                .withRegEx(".*Successful registration at resource manager.*")
                                .withStartupTimeout(Duration.ofMinutes(2)))
                .withFileSystemBind(
                        HOST_VOLUME_MOUNT_PATH, CONTAINER_VOLUME_MOUNT_PATH, BindMode.READ_WRITE);
    }

    @Override
    public void tearDown() throws Exception {
        for (GenericContainer<?> additionalTaskManager : additionalTaskManagers) {
            additionalTaskManager.stop();
        }
        additionalTaskManagers.clear();
        if (taskManager != null) {
            if (taskManager.isRunning()) {
                // delete the volume
                taskManager.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            }
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
        for (GenericContainer<?> additionalTaskManager : additionalTaskManagers) {
            extendedFactory.extend(additionalTaskManager);
        }
        extendedFactories.add(extendedFactory);
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
        StringBuilder logs = new StringBuilder(jobManager.getLogs()).append('\n');
        logs.append(taskManager.getLogs());
        for (GenericContainer<?> additionalTaskManager : additionalTaskManagers) {
            logs.append('\n').append(additionalTaskManager.getLogs());
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

    /** Submits a detached Flink job and returns its Flink job ID. */
    public String submitDetachedJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        return submitDetachedJob(confFile, variables, null);
    }

    /** Submits a detached Flink job restored from the supplied savepoint. */
    public String submitDetachedJob(String confFile, List<String> variables, String savepointPath)
            throws IOException, InterruptedException {
        List<String> engineArguments = new ArrayList<>();
        engineArguments.add("-d");
        if (savepointPath != null) {
            // Passing -s reaches Flink's savepoint-specific commons-cli path. Connector jars can
            // carry an older commons-cli version, so use Flink's equivalent dynamic configuration
            // to keep savepoint restore compatible across all tested Flink versions.
            engineArguments.add("-Dexecution.savepoint.path=" + savepointPath);
        }
        Container.ExecResult result =
                executeJob(jobManager, confFile, null, variables, engineArguments);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "Failed to submit detached Flink job: " + result.getStderr());
        }
        Matcher matcher = FLINK_JOB_ID_PATTERN.matcher(result.getStdout());
        if (!matcher.find()) {
            throw new IllegalStateException(
                    "Cannot find Flink job ID in submission output: " + result.getStdout());
        }
        return matcher.group(1);
    }

    /** Triggers a savepoint through Flink's REST API and returns its external path. */
    public String triggerSavepoint(String jobId) throws IOException, InterruptedException {
        prepareSavepointDirectory(jobManager);
        prepareSavepointDirectory(taskManager);
        ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
        requestBody.put("target-directory", SAVEPOINT_DIRECTORY);
        requestBody.put("cancel-job", false);
        HttpPost request = new HttpPost(flinkRestEndpoint() + "/jobs/" + jobId + "/savepoints");
        request.setEntity(new StringEntity(requestBody.toString(), ContentType.APPLICATION_JSON));
        JsonNode triggerResponse = executeFlinkRestRequest(request);
        String triggerId = triggerResponse.path("request-id").asText();
        if (triggerId.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot find savepoint request ID in Flink response: " + triggerResponse);
        }

        long deadline = System.currentTimeMillis() + SAVEPOINT_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            JsonNode status =
                    executeFlinkRestRequest(
                            new HttpGet(
                                    flinkRestEndpoint()
                                            + "/jobs/"
                                            + jobId
                                            + "/savepoints/"
                                            + triggerId));
            String statusId = status.path("status").path("id").asText();
            if ("COMPLETED".equals(statusId)) {
                JsonNode operation = status.path("operation");
                JsonNode failureCause = operation.path("failure-cause");
                if (!failureCause.isMissingNode() && !failureCause.isNull()) {
                    throw new IllegalStateException("Failed to trigger savepoint: " + failureCause);
                }
                String location = operation.path("location").asText();
                if (location.isEmpty()) {
                    throw new IllegalStateException(
                            "Completed savepoint has no external path: " + status);
                }
                return location;
            }
            if ("FAILED".equals(statusId)) {
                throw new IllegalStateException("Failed to trigger savepoint: " + status);
            }
            Thread.sleep(200L);
        }
        throw new IllegalStateException(
                "Timed out waiting for savepoint " + triggerId + " of job " + jobId);
    }

    private void prepareSavepointDirectory(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.execInContainer(
                        "bash",
                        "-c",
                        "mkdir -p "
                                + CONTAINER_VOLUME_MOUNT_PATH
                                + "/flink-savepoints && chmod 777 "
                                + CONTAINER_VOLUME_MOUNT_PATH
                                + "/flink-savepoints");
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "Failed to prepare savepoint directory: " + result.getStderr());
        }
    }

    /** Cancels a running Flink job through Flink's REST API. */
    public void cancelFlinkJob(String jobId) throws IOException {
        executeFlinkRestRequest(
                new HttpPatch(flinkRestEndpoint() + "/jobs/" + jobId + "?mode=cancel"));
    }

    /** Returns the number of TaskManagers currently registered with the Flink cluster. */
    public int getRegisteredTaskManagerCount() throws IOException {
        return executeFlinkRestRequest(new HttpGet(flinkRestEndpoint() + "/taskmanagers"))
                .path("taskmanagers")
                .size();
    }

    private String flinkRestEndpoint() {
        return "http://" + jobManager.getHost() + ":" + jobManager.getMappedPort(FLINK_REST_PORT);
    }

    private JsonNode executeFlinkRestRequest(HttpRequestBase request) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault();
                CloseableHttpResponse response = client.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            String responseBody = entity == null ? "" : EntityUtils.toString(entity);
            if (statusCode < 200 || statusCode >= 300) {
                throw new IllegalStateException(
                        "Flink REST request failed with status "
                                + statusCode
                                + ": "
                                + responseBody);
            }
            return responseBody.isEmpty()
                    ? OBJECT_MAPPER.createObjectNode()
                    : OBJECT_MAPPER.readTree(responseBody);
        }
    }

    /**
     * Restarts the TaskManager container and reapplies connector extensions. The JobManager stays
     * alive so Flink must recover the running job from its latest completed checkpoint.
     */
    public synchronized void restartTaskManager() throws Exception {
        taskManager.stop();
        Startables.deepStart(Stream.of(taskManager)).join();
        applyExtendedFactories(taskManager);
    }

    /** Starts another TaskManager while leaving the primary TaskManager running. */
    public synchronized void startAdditionalTaskManager() throws Exception {
        int taskManagerNumber = additionalTaskManagers.size() + 2;
        GenericContainer<?> additionalTaskManager =
                createTaskManagerContainer(
                        getDockerImage(),
                        String.join("\n", getFlinkProperties()),
                        "taskmanager-" + taskManagerNumber);
        try {
            Startables.deepStart(Stream.of(additionalTaskManager)).join();
            applyExtendedFactories(additionalTaskManager);
            additionalTaskManagers.add(additionalTaskManager);
        } catch (Exception e) {
            additionalTaskManager.stop();
            throw e;
        }
    }

    /** Stops only the primary TaskManager, leaving any additional TaskManagers available. */
    public synchronized void stopTaskManager() {
        taskManager.stop();
    }

    /** Stops and forgets all additional TaskManagers created for a test. */
    public synchronized void stopAdditionalTaskManagers() {
        for (GenericContainer<?> additionalTaskManager : additionalTaskManagers) {
            additionalTaskManager.stop();
        }
        additionalTaskManagers.clear();
    }

    private void applyExtendedFactories(GenericContainer<?> container) throws Exception {
        for (ContainerExtendedFactory extendedFactory : extendedFactories) {
            extendedFactory.extend(container);
        }
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
