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

package org.apache.seatunnel.engine.e2e.telemetry;

import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesRegex;

public class MasterWorkerClusterSeaTunnelWithTelemetryIT extends SeaTunnelContainer {

    private GenericContainer<?> secondServer;

    private final Network NETWORK = Network.newNetwork();

    private static final String jobName = "test测试";
    private static final String paramJobName = "param_test测试";

    private static final String http = "http://";

    private static final String colon = ":";

    private static final String confFile = "/fakesource_to_console.conf";

    private static final Path binPath = Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL);
    private static final Path config = Paths.get(SEATUNNEL_HOME, "config");
    private static final Path hadoopJar =
            Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar");

    @Test
    public void testSubmitJobs() throws InterruptedException {
        testGetMetrics(server, "seatunnel", true);
        testGetMetrics(secondServer, "seatunnel", false);
    }

    @Override
    @BeforeEach
    public void startUp() throws Exception {

        server = createServer("server", "master");
        secondServer = createServer("secondServer", "worker");

        // check cluster
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Response response =
                                    given().get(
                                                    http
                                                            + server.getHost()
                                                            + colon
                                                            + server.getFirstMappedPort()
                                                            + "/hazelcast/rest/cluster");
                            response.then().statusCode(200);
                            Assertions.assertEquals(
                                    2, response.jsonPath().getList("members").size());
                        });
        String JobId =
                submitJob(
                                server,
                                server.getMappedPort(5801),
                                RestConstant.CONTEXT_PATH,
                                "STREAMING",
                                jobName,
                                paramJobName)
                        .getBody()
                        .jsonPath()
                        .getString("jobId");

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Assertions.assertNotNull(JobId);
                            given().get(
                                            http
                                                    + server.getHost()
                                                    + colon
                                                    + server.getFirstMappedPort()
                                                    + CONTEXT_PATH
                                                    + RestConstant.REST_URL_JOB_INFO
                                                    + "/"
                                                    + JobId)
                                    .then()
                                    .statusCode(200)
                                    .body("jobStatus", equalTo("RUNNING"));
                        });
    }

    public void testGetMetrics(GenericContainer<?> server, String testClusterName, boolean isMaster)
            throws InterruptedException {
        Response response =
                given().get(
                                http
                                        + server.getHost()
                                        + colon
                                        + server.getFirstMappedPort()
                                        + "/hazelcast/rest/instance/metrics");
        ValidatableResponse validatableResponse =
                response.then()
                        .statusCode(200)
                        // Use regular expressions to verify whether the response body is the
                        // indicator data
                        // of Prometheus
                        // Metric data is usually multi-line, use newlines for validation
                        .body(matchesRegex("(?s)^.*# HELP.*# TYPE.*$"))
                        // Verify that the response body contains a specific metric
                        // JVM metrics
                        .body(containsString("jvm_threads"))
                        .body(containsString("jvm_memory_pool"))
                        .body(containsString("jvm_gc"))
                        .body(containsString("jvm_info"))
                        .body(containsString("jvm_memory_bytes"))
                        .body(containsString("jvm_classes"))
                        .body(containsString("jvm_buffer_pool"))
                        .body(containsString("process_start"))
                        // cluster_info
                        .body(containsString("cluster_info{cluster=\"" + testClusterName))
                        // cluster_time
                        .body(containsString("cluster_time{cluster=\"" + testClusterName));

        if (isMaster) {
            validatableResponse
                    // Job thread pool metrics
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_activeCount\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_completedTask_total\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_corePoolSize\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_maximumPoolSize\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_poolSize\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_task_total\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_queueTaskCount\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    .body(
                            matchesRegex(
                                    "(?s)^.*job_thread_pool_rejection_total\\{cluster=\""
                                            + testClusterName
                                            + "\",address=.*$"))
                    // Job count metrics
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"canceled\",} 0.0"))
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"cancelling\",} 0.0"))
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"created\",} 0.0"))
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"failed\",} 0.0"))
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"failing\",} 0.0"))
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"finished\",} 0.0"))
                    // Running job count is 1
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"running\",} 1.0"))
                    .body(
                            containsString(
                                    "job_count{cluster=\""
                                            + testClusterName
                                            + "\",type=\"scheduled\",} 0.0"));
        }
        // Node
        validatableResponse
                .body(
                        matchesRegex(
                                "(?s)^.*node_state\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_executor_executedCount
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_executedCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))
                // hazelcast_executor_isShutdown

                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isShutdown\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_isTerminated
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_isTerminated\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_maxPoolSize
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_maxPoolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_poolSize
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_poolSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_queueRemainingCapacity
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueRemainingCapacity\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_executor_queueSize
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"async\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"client\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientBlocking\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"clientQuery\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"io\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"offloadable\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"scheduled\".*$"))
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_executor_queueSize\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*,type=\"system\".*$"))

                // hazelcast_partition_partitionCount
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_partitionCount\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_partition_activePartition
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_activePartition\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_partition_isClusterSafe
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_isClusterSafe\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"))
                // hazelcast_partition_isLocalMemberSafe
                .body(
                        matchesRegex(
                                "(?s)^.*hazelcast_partition_isLocalMemberSafe\\{cluster=\""
                                        + testClusterName
                                        + "\",address=.*$"));
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
        if (secondServer != null) {
            secondServer.close();
        }
    }

    private Response submitJob(
            GenericContainer<?> container,
            int port,
            String contextPath,
            String jobMode,
            String jobName,
            String paramJobName) {
        return submitJob(jobMode, container, port, contextPath, false, jobName, paramJobName);
    }

    private Response submitJob(
            String jobMode,
            GenericContainer<?> container,
            int port,
            String contextPath,
            boolean isStartWithSavePoint,
            String jobName,
            String paramJobName) {
        String requestBody =
                "{\n"
                        + "    \"env\": {\n"
                        + "        \"job.name\": \""
                        + jobName
                        + "\",\n"
                        + "        \"job.mode\": \""
                        + jobMode
                        + "\"\n"
                        + "    },\n"
                        + "    \"source\": [\n"
                        + "        {\n"
                        + "            \"plugin_name\": \"FakeSource\",\n"
                        + "            \"plugin_output\": \"fake\",\n"
                        + "            \"row.num\": 100,\n"
                        + "            \"schema\": {\n"
                        + "                \"fields\": {\n"
                        + "                    \"name\": \"string\",\n"
                        + "                    \"age\": \"int\",\n"
                        + "                    \"card\": \"int\"\n"
                        + "                }\n"
                        + "            }\n"
                        + "        }\n"
                        + "    ],\n"
                        + "    \"transform\": [\n"
                        + "    ],\n"
                        + "    \"sink\": [\n"
                        + "        {\n"
                        + "            \"plugin_name\": \"Console\",\n"
                        + "            \"plugin_input\": [\"fake\"]\n"
                        + "        }\n"
                        + "    ]\n"
                        + "}";
        String parameters = null;
        if (paramJobName != null) {
            parameters = "jobName=" + paramJobName;
        }
        if (isStartWithSavePoint) {
            parameters = parameters + "&isStartWithSavePoint=true";
        }
        Response response =
                given().body(requestBody)
                        .header("Content-Type", "application/json; charset=utf-8")
                        .post(
                                parameters == null
                                        ? http
                                                + container.getHost()
                                                + colon
                                                + port
                                                + contextPath
                                                + RestConstant.REST_URL_SUBMIT_JOB
                                        : http
                                                + container.getHost()
                                                + colon
                                                + port
                                                + contextPath
                                                + RestConstant.REST_URL_SUBMIT_JOB
                                                + "?"
                                                + parameters);
        return response;
    }

    private GenericContainer<?> createServer(String networkAlias, String role)
            throws IOException, InterruptedException {

        GenericContainer<?> server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(
                                ContainerUtil.adaptPathForWin(binPath.toString()) + " -r " + role)
                        .withNetworkAliases(networkAlias)
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server);
        server.setExposedPorts(Arrays.asList(5801));
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                config.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/master-worker-cluster/"),
                config.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                hadoopJar.toString());
        server.start();
        // execute extra commands
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
