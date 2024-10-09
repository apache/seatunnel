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

package org.apache.seatunnel.engine.e2e.joblog;

import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.engine.e2e.SeaTunnelContainer;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.testcontainers.containers.Container;
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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.hamcrest.Matchers.equalTo;

public class JobLogIT extends SeaTunnelContainer {

    private static final String CUSTOM_JOB_NAME = "test-job-log-file";
    private static final long CUSTOM_JOB_ID = 862969647010611201L;

    private static final String confFile = "/fakesource_to_console.conf";
    private static final Path BIN_PATH = Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL);
    private static final Path CONFIG_PATH = Paths.get(SEATUNNEL_HOME, "config");
    private static final Path HADOOP_JAR_PATH =
            Paths.get(SEATUNNEL_HOME, "lib/seatunnel-hadoop3-3.1.4-uber.jar");

    private GenericContainer<?> secondServer;
    private final Network NETWORK = Network.newNetwork();

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
                                                    "http://"
                                                            + server.getHost()
                                                            + ":"
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
    public void testJobLogFile() throws Exception {
        submitJobAndAssertResponse(
                server, JobMode.STREAMING.name(), false, CUSTOM_JOB_NAME, CUSTOM_JOB_ID);

        assertConsoleLog();
        assertFileLog();
        assertFileLogClean(false);
        Thread.sleep(90000);
        assertFileLogClean(true);
    }

    private void assertConsoleLog() {
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            String serverLogs = server.getLogs();
                            String secondServerLogs = secondServer.getLogs();
                            Stream.of(
                                            // [862969647010611201] 2024-09-21 17:11:41,919 INFO
                                            // [.f.s.FakeSourceSplitEnumerator]
                                            // [BlockingWorker-TaskGroupLocation{jobId=862969647010611201, pipelineId=1, taskGroupId=1}] - Starting to calculate splits.
                                            "\\[862969647010611201\\].*INFO\\s+\\[.f.s.FakeSourceSplitEnumerator\\].*Starting to calculate splits",
                                            // [862969647010611201] 2024-09-21 17:11:41,757 INFO
                                            // [.a.s.c.s.c.s.ConsoleSinkWriter]
                                            // [hz.main.seaTunnel.task.thread-4] - output rowType:
                                            // name<STRING>, age<INT>, card<INT>
                                            "\\[862969647010611201\\].*INFO\\s+\\[.a.s.c.s.c.s.ConsoleSinkWriter\\].*output rowType: name<STRING>, age<INT>, card<INT>")
                                    .forEach(
                                            regex -> {
                                                Pattern pattern = Pattern.compile(regex);
                                                Assertions.assertTrue(
                                                        pattern.matcher(serverLogs).find()
                                                                || pattern.matcher(secondServerLogs)
                                                                        .find());
                                            });
                        });
    }

    private void assertFileLog() throws IOException, InterruptedException {
        String catLog = "cat /tmp/seatunnel/logs/job-862969647010611201.log";
        String apiGetLog = "curl http://localhost:8080/log/job-862969647010611201.log";
        Container.ExecResult execResult = server.execInContainer("sh", "-c", catLog);
        String serverLogs = execResult.getStdout();

        Container.ExecResult apiExecResult = server.execInContainer("sh", "-c", apiGetLog);

        execResult = secondServer.execInContainer("sh", "-c", catLog);
        String secondServerLogs = execResult.getStdout();
        Container.ExecResult apiSecondExecResult =
                secondServer.execInContainer("sh", "-c", apiGetLog);

        Stream.of(
                        // 2024-09-21 16:37:44,503 INFO  [.f.s.FakeSourceSplitEnumerator]
                        // [BlockingWorker-TaskGroupLocation{jobId=862969647010611201, pipelineId=1,
                        // taskGroupId=1}] - Starting to calculate splits.
                        "INFO\\s+\\[.f.s.FakeSourceSplitEnumerator\\].*Starting to calculate splits",
                        // 2024-09-21 16:37:44,295 INFO  [.a.s.c.s.c.s.ConsoleSinkWriter]
                        // [hz.main.seaTunnel.task.thread-4] - output rowType: name<STRING>,
                        // age<INT>, card<INT>
                        "INFO\\s+\\[.a.s.c.s.c.s.ConsoleSinkWriter\\].*output rowType: name<STRING>, age<INT>, card<INT>")
                .forEach(
                        regex -> {
                            Pattern pattern = Pattern.compile(regex);
                            Assertions.assertTrue(
                                    pattern.matcher(serverLogs).find()
                                            || pattern.matcher(secondServerLogs).find());
                            Assertions.assertTrue(
                                    pattern.matcher(apiExecResult.getStdout()).find()
                                            || pattern.matcher(apiSecondExecResult.getStdout())
                                                    .find());
                        });
    }

    private void assertFileLogClean(boolean b) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                server.execInContainer(
                        "sh",
                        "-c",
                        "find /tmp/seatunnel/logs -name \"job-862969647010611201.log\"\n");
        String file = execResult.getStdout();
        execResult =
                secondServer.execInContainer(
                        "sh",
                        "-c",
                        "find /tmp/seatunnel/logs -name \"job-862969647010611201.log\"\n");
        String file1 = execResult.getStdout();
        Assertions.assertEquals(b, StringUtils.isBlank(file) && StringUtils.isBlank(file1));
    }

    private Response submitJob(
            GenericContainer<?> container,
            String jobMode,
            boolean isStartWithSavePoint,
            String jobName,
            long jobId) {
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
        String parameters = "jobId=" + jobId;
        if (isStartWithSavePoint) {
            parameters = parameters + "&isStartWithSavePoint=true";
        }
        Response response =
                given().body(requestBody)
                        .header("Content-Type", "application/json; charset=utf-8")
                        .post(
                                parameters == null
                                        ? "http://"
                                                + container.getHost()
                                                + ":"
                                                + container.getFirstMappedPort()
                                                + RestConstant.CONTEXT_PATH
                                                + RestConstant.SUBMIT_JOB_URL
                                        : "http://"
                                                + container.getHost()
                                                + ":"
                                                + container.getFirstMappedPort()
                                                + RestConstant.CONTEXT_PATH
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
                        .withCommand(ContainerUtil.adaptPathForWin(BIN_PATH.toString()))
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
                CONFIG_PATH.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/cluster/"),
                CONFIG_PATH.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                HADOOP_JAR_PATH.toString());
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/job-log-file/"),
                CONFIG_PATH.toString());
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
            GenericContainer<?> container,
            String jobMode,
            boolean isStartWithSavePoint,
            String jobName,
            long jobId) {
        Response response = submitJob(container, jobMode, isStartWithSavePoint, jobName, jobId);
        response.then()
                .statusCode(200)
                .body("jobName", equalTo(jobName))
                .body("jobId", equalTo(String.valueOf(jobId)));
    }
}
