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
import org.jetbrains.annotations.NotNull;
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

import com.hazelcast.jet.json.JsonUtil;
import io.restassured.response.Response;
import scala.Tuple3;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static final long CUSTOM_JOB_ID_1 = 862969647010611201L;

    private static final long CUSTOM_JOB_ID_2 = 862969647010611202L;

    private static List<Tuple3<Integer, String, Long>> tasks;

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

        tasks = new ArrayList<>();
        tasks.add(
                new Tuple3<>(
                        server.getMappedPort(5801), RestConstant.CONTEXT_PATH, CUSTOM_JOB_ID_1));
        tasks.add(new Tuple3<>(server.getMappedPort(8080), "/seatunnel", CUSTOM_JOB_ID_2));
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
                                tasks.forEach(
                                        task -> {
                                            submitJobAndAssertResponse(
                                                    container,
                                                    task._1(),
                                                    task._2(),
                                                    i,
                                                    paramJobName + "&jobId=" + task._3(),
                                                    true,
                                                    task._3().toString());
                                        }));
    }

    @Test
    public void testSubmitJobWithoutCustomJobId() {
        AtomicInteger i = new AtomicInteger();
        Arrays.asList(server, secondServer)
                .forEach(
                        container ->
                                tasks.forEach(
                                        task -> {
                                            submitJobAndAssertResponse(
                                                    container,
                                                    task._1(),
                                                    task._2(),
                                                    i,
                                                    paramJobName,
                                                    false,
                                                    task._3().toString());
                                        }));
    }

    @Test
    public void testStartWithSavePointWithoutJobId() {
        Arrays.asList(server, secondServer)
                .forEach(
                        container -> {
                            tasks.forEach(
                                    task -> {
                                        Response response =
                                                submitJob(
                                                        "BATCH",
                                                        container,
                                                        task._1(),
                                                        task._2(),
                                                        true,
                                                        jobName,
                                                        paramJobName);
                                        response.then()
                                                .statusCode(400)
                                                .body(
                                                        "message",
                                                        equalTo(
                                                                "Please provide jobId when start with save point."));
                                    });
                        });
    }

    @Test
    public void testStopJob() {

        Arrays.asList(server, secondServer)
                .forEach(
                        container -> {
                            tasks.forEach(
                                    task -> {
                                        String jobId =
                                                submitJob(
                                                                container,
                                                                task._1(),
                                                                task._2(),
                                                                "STREAMING",
                                                                jobName,
                                                                paramJobName)
                                                        .getBody()
                                                        .jsonPath()
                                                        .getString("jobId");

                                        Awaitility.await()
                                                .atMost(2, TimeUnit.MINUTES)
                                                .untilAsserted(
                                                        () ->
                                                                given().get(
                                                                                http
                                                                                        + container
                                                                                                .getHost()
                                                                                        + colon
                                                                                        + container
                                                                                                .getFirstMappedPort()
                                                                                        + RestConstant
                                                                                                .RUNNING_JOB_URL
                                                                                        + "/"
                                                                                        + jobId)
                                                                        .then()
                                                                        .statusCode(200)
                                                                        .body(
                                                                                "jobStatus",
                                                                                equalTo(
                                                                                        "RUNNING")));

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
                                                                                        + container
                                                                                                .getHost()
                                                                                        + colon
                                                                                        + container
                                                                                                .getFirstMappedPort()
                                                                                        + RestConstant
                                                                                                .FINISHED_JOBS_INFO
                                                                                        + "/SAVEPOINT_DONE")
                                                                        .then()
                                                                        .statusCode(200)
                                                                        .body(
                                                                                "[0].jobId",
                                                                                equalTo(jobId)));

                                        String jobId2 =
                                                submitJob(
                                                                container,
                                                                task._1(),
                                                                task._2(),
                                                                "STREAMING",
                                                                jobName,
                                                                paramJobName)
                                                        .getBody()
                                                        .jsonPath()
                                                        .getString("jobId");

                                        Awaitility.await()
                                                .atMost(2, TimeUnit.MINUTES)
                                                .untilAsserted(
                                                        () ->
                                                                given().get(
                                                                                http
                                                                                        + container
                                                                                                .getHost()
                                                                                        + colon
                                                                                        + container
                                                                                                .getFirstMappedPort()
                                                                                        + RestConstant
                                                                                                .RUNNING_JOB_URL
                                                                                        + "/"
                                                                                        + jobId2)
                                                                        .then()
                                                                        .statusCode(200)
                                                                        .body(
                                                                                "jobStatus",
                                                                                equalTo(
                                                                                        "RUNNING")));
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
                                                                                        + container
                                                                                                .getHost()
                                                                                        + colon
                                                                                        + container
                                                                                                .getFirstMappedPort()
                                                                                        + RestConstant
                                                                                                .FINISHED_JOBS_INFO
                                                                                        + "/CANCELED")
                                                                        .then()
                                                                        .statusCode(200)
                                                                        .body(
                                                                                "[0].jobId",
                                                                                equalTo(jobId2)));
                                    });
                        });
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

    @Test
    public void testStopJobs() {
        Arrays.asList(server)
                .forEach(
                        container -> {
                            try {
                                submitJobs("STREAMING", container, false, CUSTOM_JOB_ID_1);

                                String parameters =
                                        "[{\"jobId\":"
                                                + CUSTOM_JOB_ID_1
                                                + ",\"isStopWithSavePoint\":false},{\"jobId\":"
                                                + (CUSTOM_JOB_ID_1 - 1)
                                                + ",\"isStopWithSavePoint\":false}]";

                                given().body(parameters)
                                        .post(
                                                http
                                                        + container.getHost()
                                                        + colon
                                                        + container.getFirstMappedPort()
                                                        + RestConstant.STOP_JOBS_URL)
                                        .then()
                                        .statusCode(200)
                                        .body("[0].jobId", equalTo(CUSTOM_JOB_ID_1))
                                        .body("[1].jobId", equalTo(CUSTOM_JOB_ID_1 - 1));

                                Awaitility.await()
                                        .atMost(2, TimeUnit.MINUTES)
                                        .untilAsserted(
                                                () ->
                                                        given().get(
                                                                        http
                                                                                + container
                                                                                        .getHost()
                                                                                + colon
                                                                                + container
                                                                                        .getFirstMappedPort()
                                                                                + RestConstant
                                                                                        .FINISHED_JOBS_INFO
                                                                                + "/CANCELED")
                                                                .then()
                                                                .statusCode(200)
                                                                .body(
                                                                        "[0].jobId",
                                                                        equalTo(
                                                                                String.valueOf(
                                                                                        CUSTOM_JOB_ID_1)))
                                                                .body(
                                                                        "[0].jobId",
                                                                        equalTo(
                                                                                String.valueOf(
                                                                                        CUSTOM_JOB_ID_1
                                                                                                - 1))));

                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Test
    public void testSubmitJobs() {
        AtomicInteger i = new AtomicInteger();
        Arrays.asList(server, secondServer)
                .forEach(
                        container -> {
                            try {
                                submitJobs("BATCH", container, false, CUSTOM_JOB_ID_1);
                                submitJobs("BATCH", container, true, CUSTOM_JOB_ID_1);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private void submitJobs(
            String jobMode, GenericContainer<?> container, boolean isStartWithSavePoint, Long jobId)
            throws IOException {

        String requestBody = getJobJson(jobMode, isStartWithSavePoint, jobId);

        Response response =
                given().body(requestBody)
                        .header("Content-Type", "application/json; charset=utf-8")
                        .post(
                                http
                                        + container.getHost()
                                        + colon
                                        + container.getFirstMappedPort()
                                        + RestConstant.SUBMIT_JOBS_URL);

        response.then()
                .statusCode(200)
                .body("[0].jobId", equalTo(String.valueOf(jobId)))
                .body("[1].jobId", equalTo(String.valueOf(jobId - 1)));

        Response jobInfoResponse =
                given().header("Content-Type", "application/json; charset=utf-8")
                        .get(
                                http
                                        + container.getHost()
                                        + colon
                                        + container.getFirstMappedPort()
                                        + RestConstant.JOB_INFO_URL
                                        + "/"
                                        + jobId);
        jobInfoResponse.then().statusCode(200).body("jobStatus", equalTo("RUNNING"));
    }

    private static @NotNull String getJobJson(
            String jobMode, boolean isStartWithSavePoint, Long jobId) throws IOException {
        List<Map<String, Object>> jobList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> job = new HashMap<>();
            Map<String, String> params = new HashMap<>();
            params.put("jobId", String.valueOf(jobId - i));
            if (isStartWithSavePoint) {
                params.put("isStartWithSavePoint", "true");
            }
            job.put("params", params);

            Map<String, String> env = new HashMap<>();
            env.put("job.mode", jobMode);
            job.put("env", env);

            List<Map<String, Object>> sourceList = new ArrayList<>();
            Map<String, Object> source = new HashMap<>();
            source.put("plugin_name", "FakeSource");
            source.put("result_table_name", "fake");
            source.put("row.num", 1000);

            Map<String, Object> schema = new HashMap<>();
            Map<String, String> fields = new HashMap<>();
            fields.put("name", "string");
            fields.put("age", "int");
            fields.put("card", "int");
            schema.put("fields", fields);
            source.put("schema", schema);

            sourceList.add(source);
            job.put("source", sourceList);

            List<Map<String, Object>> transformList = new ArrayList<>();
            job.put("transform", transformList);

            List<Map<String, Object>> sinkList = new ArrayList<>();
            Map<String, Object> sink = new HashMap<>();
            sink.put("plugin_name", "Console");
            List<String> sourceTableName = new ArrayList<>();
            sourceTableName.add("fake");
            sink.put("source_table_name", sourceTableName);

            sinkList.add(sink);
            job.put("sink", sinkList);

            jobList.add(job);
        }
        return JsonUtil.toJson(jobList);
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
                                                + port
                                                + contextPath
                                                + RestConstant.SUBMIT_JOB_URL
                                        : http
                                                + container.getHost()
                                                + colon
                                                + port
                                                + contextPath
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
        server.setExposedPorts(Arrays.asList(5801, 8080));
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
            int port,
            String contextPath,
            AtomicInteger i,
            String customParam,
            boolean isCustomJobId,
            String customJobId) {
        Response response = submitJobAndResponse(container, port, contextPath, i, customParam);
        String jobId = response.getBody().jsonPath().getString("jobId");
        assertResponse(container, port, contextPath, i, jobId, customJobId, isCustomJobId);
        i.getAndIncrement();
    }

    private Response submitJobAndResponse(
            GenericContainer<? extends GenericContainer<?>> container,
            int port,
            String contextPath,
            AtomicInteger i,
            String customParam) {
        Response response =
                i.get() == 0
                        ? submitJob(container, port, contextPath, "BATCH", jobName, customParam)
                        : submitJob(container, port, contextPath, "BATCH", jobName, null);
        if (i.get() == 0) {
            response.then().statusCode(200).body("jobName", equalTo(paramJobName));
        } else {
            response.then().statusCode(200).body("jobName", equalTo(jobName));
        }
        return response;
    }

    private void assertResponse(
            GenericContainer<? extends GenericContainer<?>> container,
            int port,
            String contextPath,
            AtomicInteger i,
            String jobId,
            String customJobId,
            boolean isCustomJobId) {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            assertWithStatusParameterOrNot(
                                    container,
                                    port,
                                    contextPath,
                                    i,
                                    jobId,
                                    customJobId,
                                    isCustomJobId,
                                    true);

                            // test for without status parameter.
                            assertWithStatusParameterOrNot(
                                    container,
                                    port,
                                    contextPath,
                                    i,
                                    jobId,
                                    customJobId,
                                    isCustomJobId,
                                    false);
                        });
    }

    private void assertWithStatusParameterOrNot(
            GenericContainer<? extends GenericContainer<?>> container,
            int port,
            String contextPath,
            AtomicInteger i,
            String jobId,
            String customJobId,
            boolean isCustomJobId,
            boolean isStatusWithSubmitJob) {
        String baseRestUrl = getBaseRestUrl(container, port, contextPath);
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
                                        ? Long.toString(CUSTOM_JOB_ID_1)
                                        : jobId))
                .body("[" + i.get() + "].metrics.SourceReceivedCount", equalTo("100"))
                .body("[" + i.get() + "].metrics.SinkWriteCount", equalTo("100"))
                .body("[" + i.get() + "].jobStatus", equalTo("FINISHED"));
    }

    private String getBaseRestUrl(
            GenericContainer<? extends GenericContainer<?>> container,
            int port,
            String contextPath) {
        return http
                + container.getHost()
                + colon
                + port
                + contextPath
                + RestConstant.FINISHED_JOBS_INFO;
    }
}
