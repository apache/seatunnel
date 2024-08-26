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

package org.apache.seatunnel.engine.e2e;

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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.hamcrest.Matchers.equalTo;

public class ClusterSeaTunnelContainer extends SeaTunnelContainer {

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

    private static final long CUSTOM_JOB_ID = 862969647010611201L;

    @Override
    @BeforeEach
    public void startUp() throws Exception {

        server = createServer("server");
        secondServer = createServer("secondServer");

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
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
        if (secondServer != null) {
            secondServer.close();
        }
    }

    @Test
    public void testSubmitJobWithCustomJobId() {
        AtomicInteger i = new AtomicInteger();
        Arrays.asList(server, secondServer)
                .forEach(
                        container ->
                                submitJobAndAssertResponse(
                                        container,
                                        i,
                                        paramJobName + "&jobId=" + CUSTOM_JOB_ID,
                                        true));
    }

    @Test
    public void testSubmitJobWithoutCustomJobId() {
        AtomicInteger i = new AtomicInteger();
        Arrays.asList(server, secondServer)
                .forEach(
                        container -> submitJobAndAssertResponse(container, i, paramJobName, false));
    }

    @Test
    public void testStartWithSavePointWithoutJobId() {
        Arrays.asList(server, secondServer)
                .forEach(
                        container -> {
                            Response response =
                                    submitJob("BATCH", container, true, jobName, paramJobName);
                            response.then()
                                    .statusCode(400)
                                    .body(
                                            "message",
                                            equalTo(
                                                    "Please provide jobId when start with save point."));
                        });
    }

    @Test
    public void testStopJob() {

        Arrays.asList(server, secondServer)
                .forEach(
                        container -> {
                            String jobId =
                                    submitJob(container, "STREAMING", jobName, paramJobName)
                                            .getBody()
                                            .jsonPath()
                                            .getString("jobId");

                            Awaitility.await()
                                    .atMost(2, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    given().get(
                                                                    http
                                                                            + container.getHost()
                                                                            + colon
                                                                            + container
                                                                                    .getFirstMappedPort()
                                                                            + RestConstant
                                                                                    .RUNNING_JOB_URL
                                                                            + "/"
                                                                            + jobId)
                                                            .then()
                                                            .statusCode(200)
                                                            .body("jobStatus", equalTo("RUNNING")));

                            String parameters =
                                    "{"
                                            + "\"jobId\":"
                                            + jobId
                                            + ","
                                            + "\"isStopWithSavePoint\":true}";

                            given().body(parameters)
                                    .post(
                                            http
                                                    + container.getHost()
                                                    + colon
                                                    + container.getFirstMappedPort()
                                                    + RestConstant.STOP_JOB_URL)
                                    .then()
                                    .statusCode(200)
                                    .body("jobId", equalTo(jobId));

                            Awaitility.await()
                                    .atMost(6, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    given().get(
                                                                    http
                                                                            + container.getHost()
                                                                            + colon
                                                                            + container
                                                                                    .getFirstMappedPort()
                                                                            + RestConstant
                                                                                    .FINISHED_JOBS_INFO
                                                                            + "/SAVEPOINT_DONE")
                                                            .then()
                                                            .statusCode(200)
                                                            .body("[0].jobId", equalTo(jobId)));

                            String jobId2 =
                                    submitJob(container, "STREAMING", jobName, paramJobName)
                                            .getBody()
                                            .jsonPath()
                                            .getString("jobId");

                            Awaitility.await()
                                    .atMost(2, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    given().get(
                                                                    http
                                                                            + container.getHost()
                                                                            + colon
                                                                            + container
                                                                                    .getFirstMappedPort()
                                                                            + RestConstant
                                                                                    .RUNNING_JOB_URL
                                                                            + "/"
                                                                            + jobId2)
                                                            .then()
                                                            .statusCode(200)
                                                            .body("jobStatus", equalTo("RUNNING")));
                            parameters =
                                    "{"
                                            + "\"jobId\":"
                                            + jobId2
                                            + ","
                                            + "\"isStopWithSavePoint\":false}";

                            given().body(parameters)
                                    .post(
                                            http
                                                    + container.getHost()
                                                    + colon
                                                    + container.getFirstMappedPort()
                                                    + RestConstant.STOP_JOB_URL)
                                    .then()
                                    .statusCode(200)
                                    .body("jobId", equalTo(jobId2));

                            Awaitility.await()
                                    .atMost(2, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    given().get(
                                                                    http
                                                                            + container.getHost()
                                                                            + colon
                                                                            + container
                                                                                    .getFirstMappedPort()
                                                                            + RestConstant
                                                                                    .FINISHED_JOBS_INFO
                                                                            + "/CANCELED")
                                                            .then()
                                                            .statusCode(200)
                                                            .body("[0].jobId", equalTo(jobId2)));
                        });
    }

    private Response submitJob(
            GenericContainer<?> container, String jobMode, String jobName, String paramJobName) {
        return submitJob(jobMode, container, false, jobName, paramJobName);
    }

    private Response submitJob(
            String jobMode,
            GenericContainer<?> container,
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
                        + "            \"result_table_name\": \"fake\",\n"
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
                        + "            \"source_table_name\": [\"fake\"]\n"
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
                                                + container.getFirstMappedPort()
                                                + RestConstant.SUBMIT_JOB_URL
                                        : http
                                                + container.getHost()
                                                + colon
                                                + container.getFirstMappedPort()
                                                + RestConstant.SUBMIT_JOB_URL
                                                + "?"
                                                + parameters);
        return response;
    }

    private GenericContainer<?> createServer(String networkAlias)
            throws IOException, InterruptedException {
        GenericContainer<?> server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(ContainerUtil.adaptPathForWin(binPath.toString()))
                        .withNetworkAliases(networkAlias)
                        .withExposedPorts()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forListeningPort());
        copySeaTunnelStarterToContainer(server);
        server.setExposedPorts(Collections.singletonList(5801));
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                config.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/cluster/"),
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

    private void submitJobAndAssertResponse(
            GenericContainer<? extends GenericContainer<?>> container,
            AtomicInteger i,
            String customParam,
            boolean isCustomJobId) {
        Response response = submitJobAndResponse(container, i, customParam);
        String jobId = response.getBody().jsonPath().getString("jobId");
        assertResponse(container, i, jobId, isCustomJobId);
        i.getAndIncrement();
    }

    private Response submitJobAndResponse(
            GenericContainer<? extends GenericContainer<?>> container,
            AtomicInteger i,
            String customParam) {
        Response response =
                i.get() == 0
                        ? submitJob(container, "BATCH", jobName, customParam)
                        : submitJob(container, "BATCH", jobName, null);
        if (i.get() == 0) {
            response.then().statusCode(200).body("jobName", equalTo(paramJobName));
        } else {
            response.then().statusCode(200).body("jobName", equalTo(jobName));
        }
        return response;
    }

    private void assertResponse(
            GenericContainer<? extends GenericContainer<?>> container,
            AtomicInteger i,
            String jobId,
            boolean isCustomJobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertWithStatusParameterOrNot(
                                    container, i, jobId, isCustomJobId, true);

                            // test for without status parameter.
                            assertWithStatusParameterOrNot(
                                    container, i, jobId, isCustomJobId, false);
                        });
    }

    private void assertWithStatusParameterOrNot(
            GenericContainer<? extends GenericContainer<?>> container,
            AtomicInteger i,
            String jobId,
            boolean isCustomJobId,
            boolean isStatusWithSubmitJob) {
        String baseRestUrl = getBaseRestUrl(container);
        String restUrl = isStatusWithSubmitJob ? baseRestUrl + "/FINISHED" : baseRestUrl;
        given().get(restUrl)
                .then()
                .statusCode(200)
                .body("[" + i.get() + "].jobName", equalTo(i.get() == 0 ? paramJobName : jobName))
                .body("[" + i.get() + "].errorMsg", equalTo(null))
                .body(
                        "[" + i.get() + "].jobId",
                        equalTo(
                                i.get() == 0 && isCustomJobId
                                        ? Long.toString(CUSTOM_JOB_ID)
                                        : jobId))
                .body("[" + i.get() + "].metrics.SourceReceivedCount", equalTo("100"))
                .body("[" + i.get() + "].metrics.SinkWriteCount", equalTo("100"))
                .body("[" + i.get() + "].jobStatus", equalTo("FINISHED"));
    }

    private String getBaseRestUrl(GenericContainer<? extends GenericContainer<?>> container) {
        return http
                + container.getHost()
                + colon
                + container.getFirstMappedPort()
                + RestConstant.FINISHED_JOBS_INFO;
    }
}
