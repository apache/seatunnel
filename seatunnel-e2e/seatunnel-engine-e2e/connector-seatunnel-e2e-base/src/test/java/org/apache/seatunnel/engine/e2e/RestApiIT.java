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

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import io.restassured.response.Response;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.engine.server.rest.RestConstant.LOG_FILE_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.LOG_PATH;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

@Slf4j
public class RestApiIT {

    private static final String HOST = "http://localhost:";

    private static ClientJobProxy clientJobProxy;

    private static HazelcastInstanceImpl node1;

    private static HazelcastInstanceImpl node2;

    private static final String jobName = "test测试";
    private static final String paramJobName = "param_test测试";

    private static final String content = "Hello, World!";

    @BeforeEach
    void beforeClass() throws Exception {
        String testClusterName = TestUtils.getClusterName("RestApiIT");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

        node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

        String filePath = TestUtils.getResource("stream_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

        clientJobProxy = jobExecutionEnv.execute();

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus()));
    }

    @Test
    public void testGetRunningJobById() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            given().get(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.RUNNING_JOB_URL
                                                    + "/"
                                                    + clientJobProxy.getJobId())
                                    .then()
                                    .statusCode(200)
                                    .body("jobName", equalTo("fake_to_file"))
                                    .body("jobStatus", equalTo("RUNNING"));
                        });
    }

    @Test
    public void testGetRunningJobs() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            given().get(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.RUNNING_JOBS_URL)
                                    .then()
                                    .statusCode(200)
                                    .body("[0].jobName", equalTo("fake_to_file"))
                                    .body("[0].jobStatus", equalTo("RUNNING"));
                        });
    }

    @Test
    public void testSystemMonitoringInformation() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            given().get(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.SYSTEM_MONITORING_INFORMATION)
                                    .then()
                                    .assertThat()
                                    .time(lessThan(5000L))
                                    .statusCode(200);
                        });
    }

    @Test
    public void testSubmitJob() {
        AtomicInteger i = new AtomicInteger();

        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            Response response =
                                    i.get() == 0
                                            ? submitJob(instance, "BATCH", jobName, paramJobName)
                                            : submitJob(instance, "BATCH", jobName, null);
                            if (i.get() == 0) {
                                response.then()
                                        .statusCode(200)
                                        .body("jobName", equalTo(paramJobName));
                            } else {
                                response.then().statusCode(200).body("jobName", equalTo(jobName));
                            }
                            String jobId = response.getBody().jsonPath().getString("jobId");
                            SeaTunnelServer seaTunnelServer = null;

                            for (HazelcastInstance hazelcastInstance :
                                    Hazelcast.getAllHazelcastInstances()) {
                                SeaTunnelServer server =
                                        (SeaTunnelServer)
                                                ((HazelcastInstanceProxy) hazelcastInstance)
                                                        .getOriginal()
                                                        .node
                                                        .getNodeExtension()
                                                        .createExtensionServices()
                                                        .get(Constant.SEATUNNEL_SERVICE_NAME);

                                if (server.isMasterNode()) {
                                    seaTunnelServer = server;
                                }
                            }

                            SeaTunnelServer finalSeaTunnelServer = seaTunnelServer;
                            Awaitility.await()
                                    .atMost(2, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            JobStatus.FINISHED,
                                                            finalSeaTunnelServer
                                                                    .getCoordinatorService()
                                                                    .getJobStatus(
                                                                            Long.parseLong(
                                                                                    jobId))));

                            given().get(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.FINISHED_JOBS_INFO
                                                    + "/FINISHED")
                                    .then()
                                    .statusCode(200)
                                    .body(
                                            "[" + i.get() + "].jobName",
                                            equalTo(i.get() == 0 ? paramJobName : jobName))
                                    .body("[" + i.get() + "].errorMsg", equalTo(null))
                                    .body(
                                            "[" + i.get() + "].jobDag.jobId",
                                            equalTo(Long.parseLong(jobId)))
                                    .body(
                                            "[" + i.get() + "].metrics.SourceReceivedCount",
                                            equalTo("100"))
                                    .body(
                                            "[" + i.get() + "].metrics.SinkWriteCount",
                                            equalTo("100"))
                                    .body("[" + i.get() + "].jobStatus", equalTo("FINISHED"));

                            // test for without status parameter.
                            given().get(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.FINISHED_JOBS_INFO)
                                    .then()
                                    .statusCode(200)
                                    .body(
                                            "[" + i.get() + "].jobName",
                                            equalTo(i.get() == 0 ? paramJobName : jobName))
                                    .body("[" + i.get() + "].errorMsg", equalTo(null))
                                    .body(
                                            "[" + i.get() + "].jobDag.jobId",
                                            equalTo(Long.parseLong(jobId)))
                                    .body(
                                            "[" + i.get() + "].metrics.SourceReceivedCount",
                                            equalTo("100"))
                                    .body(
                                            "[" + i.get() + "].metrics.SinkWriteCount",
                                            equalTo("100"))
                                    .body("[" + i.get() + "].jobStatus", equalTo("FINISHED"));
                            i.getAndIncrement();
                        });
    }

    @Test
    public void testStopJob() {

        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            String jobId =
                                    submitJob(instance, "STREAMING", jobName, paramJobName)
                                            .getBody()
                                            .jsonPath()
                                            .getString("jobId");
                            SeaTunnelServer seaTunnelServer = null;

                            for (HazelcastInstance hazelcastInstance :
                                    Hazelcast.getAllHazelcastInstances()) {
                                SeaTunnelServer server =
                                        (SeaTunnelServer)
                                                ((HazelcastInstanceProxy) hazelcastInstance)
                                                        .getOriginal()
                                                        .node
                                                        .getNodeExtension()
                                                        .createExtensionServices()
                                                        .get(Constant.SEATUNNEL_SERVICE_NAME);

                                if (server.isMasterNode()) {
                                    seaTunnelServer = server;
                                }
                            }

                            SeaTunnelServer finalSeaTunnelServer = seaTunnelServer;
                            Awaitility.await()
                                    .atMost(2, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            JobStatus.RUNNING,
                                                            finalSeaTunnelServer
                                                                    .getCoordinatorService()
                                                                    .getJobStatus(
                                                                            Long.parseLong(
                                                                                    jobId))));

                            String parameters =
                                    "{"
                                            + "\"jobId\":"
                                            + jobId
                                            + ","
                                            + "\"isStopWithSavePoint\":true}";

                            given().body(parameters)
                                    .post(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.STOP_JOB_URL)
                                    .then()
                                    .statusCode(200)
                                    .body("jobId", equalTo(jobId));

                            Awaitility.await()
                                    .atMost(6, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            JobStatus.SAVEPOINT_DONE,
                                                            finalSeaTunnelServer
                                                                    .getCoordinatorService()
                                                                    .getJobStatus(
                                                                            Long.parseLong(
                                                                                    jobId))));

                            String jobId2 =
                                    submitJob(instance, "STREAMING", jobName, paramJobName)
                                            .getBody()
                                            .jsonPath()
                                            .getString("jobId");

                            Awaitility.await()
                                    .atMost(2, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            JobStatus.RUNNING,
                                                            finalSeaTunnelServer
                                                                    .getCoordinatorService()
                                                                    .getJobStatus(
                                                                            Long.parseLong(
                                                                                    jobId2))));
                            parameters =
                                    "{"
                                            + "\"jobId\":"
                                            + jobId2
                                            + ","
                                            + "\"isStopWithSavePoint\":false}";

                            given().body(parameters)
                                    .post(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.STOP_JOB_URL)
                                    .then()
                                    .statusCode(200)
                                    .body("jobId", equalTo(jobId2));

                            Awaitility.await()
                                    .atMost(2, TimeUnit.MINUTES)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            JobStatus.CANCELED,
                                                            finalSeaTunnelServer
                                                                    .getCoordinatorService()
                                                                    .getJobStatus(
                                                                            Long.parseLong(
                                                                                    jobId2))));
                        });
    }

    @Test
    public void testStartWithSavePointWithoutJobId() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            Response response =
                                    submitJob("BATCH", instance, true, jobName, paramJobName);
                            response.then()
                                    .statusCode(400)
                                    .body(
                                            "message",
                                            equalTo(
                                                    "Please provide jobId when start with save point."));
                        });
    }

    @Test
    public void testEncryptConfig() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            String config =
                                    "{\n"
                                            + "    \"env\": {\n"
                                            + "        \"parallelism\": 1,\n"
                                            + "        \"shade.identifier\":\"base64\"\n"
                                            + "    },\n"
                                            + "    \"source\": [\n"
                                            + "        {\n"
                                            + "            \"plugin_name\": \"MySQL-CDC\",\n"
                                            + "            \"schema\" : {\n"
                                            + "                \"fields\": {\n"
                                            + "                    \"name\": \"string\",\n"
                                            + "                    \"age\": \"int\"\n"
                                            + "                }\n"
                                            + "            },\n"
                                            + "            \"result_table_name\": \"fake\",\n"
                                            + "            \"parallelism\": 1,\n"
                                            + "            \"hostname\": \"127.0.0.1\",\n"
                                            + "            \"username\": \"seatunnel\",\n"
                                            + "            \"password\": \"seatunnel_password\",\n"
                                            + "            \"table-name\": \"inventory_vwyw0n\"\n"
                                            + "        }\n"
                                            + "    ],\n"
                                            + "    \"transform\": [\n"
                                            + "    ],\n"
                                            + "    \"sink\": [\n"
                                            + "        {\n"
                                            + "            \"plugin_name\": \"Clickhouse\",\n"
                                            + "            \"host\": \"localhost:8123\",\n"
                                            + "            \"database\": \"default\",\n"
                                            + "            \"table\": \"fake_all\",\n"
                                            + "            \"username\": \"seatunnel\",\n"
                                            + "            \"password\": \"seatunnel_password\"\n"
                                            + "        }\n"
                                            + "    ]\n"
                                            + "}";
                            given().body(config)
                                    .post(
                                            HOST
                                                    + instance.getCluster()
                                                            .getLocalMember()
                                                            .getAddress()
                                                            .getPort()
                                                    + RestConstant.ENCRYPT_CONFIG)
                                    .then()
                                    .statusCode(200)
                                    .body("source[0].result_table_name", equalTo("fake"))
                                    .body("source[0].username", equalTo("c2VhdHVubmVs"))
                                    .body(
                                            "source[0].password",
                                            equalTo("c2VhdHVubmVsX3Bhc3N3b3Jk"));
                        });
    }

    @Test
    public void testLog() throws IOException {

        System.setProperty(LOG_PATH, "/tmp/seatunnel");
        System.setProperty(LOG_FILE_NAME, "seatunnel");

        String logPath = System.getProperty(LOG_PATH);
        String logName = System.getProperty(LOG_FILE_NAME) + ".log";
        File file = new File(Paths.get(logPath, logName).toString());
        FileOutputStream fos = new FileOutputStream(file);
        try (OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
            writer.write(content);
        }

        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            String response =
                                    given().get(
                                                    HOST
                                                            + instance.getCluster()
                                                                    .getLocalMember()
                                                                    .getAddress()
                                                                    .getPort()
                                                            + RestConstant.GET_LOG)
                                            .then()
                                            .contentType("text/plain")
                                            .statusCode(200)
                                            .extract()
                                            .asString();
                            Assertions.assertEquals(content, response);
                        });

        file.delete();
    }

    @AfterEach
    void afterClass() {
        if (node1 != null) {
            node1.shutdown();
        }
        if (node2 != null) {
            node2.shutdown();
        }
    }

    private Response submitJob(
            HazelcastInstanceImpl hazelcastInstance,
            String jobMode,
            String jobName,
            String paramJobName) {
        return submitJob(jobMode, hazelcastInstance, false, jobName, paramJobName);
    }

    private Response submitJob(
            String jobMode,
            HazelcastInstanceImpl hazelcastInstance,
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
                                        ? HOST
                                                + hazelcastInstance
                                                        .getCluster()
                                                        .getLocalMember()
                                                        .getAddress()
                                                        .getPort()
                                                + RestConstant.SUBMIT_JOB_URL
                                        : HOST
                                                + hazelcastInstance
                                                        .getCluster()
                                                        .getLocalMember()
                                                        .getAddress()
                                                        .getPort()
                                                + RestConstant.SUBMIT_JOB_URL
                                                + "?"
                                                + parameters);
        return response;
    }
}
