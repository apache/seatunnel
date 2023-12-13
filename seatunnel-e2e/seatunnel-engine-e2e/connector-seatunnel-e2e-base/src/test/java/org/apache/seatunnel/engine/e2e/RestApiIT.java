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
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.restassured.response.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

@Slf4j
public class RestApiIT {

    private static final String HOST = "http://localhost:";

    private static ClientJobProxy clientJobProxy;

    private static HazelcastInstanceImpl hazelcastInstance;

    @BeforeEach
    void beforeClass() throws Exception {
        String testClusterName = TestUtils.getClusterName("RestApiIT");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
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
        given().get(
                        HOST
                                + hazelcastInstance
                                        .getCluster()
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
    }

    @Test
    public void testGetRunningJobs() {
        given().get(
                        HOST
                                + hazelcastInstance
                                        .getCluster()
                                        .getLocalMember()
                                        .getAddress()
                                        .getPort()
                                + RestConstant.RUNNING_JOBS_URL)
                .then()
                .statusCode(200)
                .body("[0].jobName", equalTo("fake_to_file"))
                .body("[0].jobStatus", equalTo("RUNNING"));
    }

    @Test
    public void testSystemMonitoringInformation() {
        given().get(
                        HOST
                                + hazelcastInstance
                                        .getCluster()
                                        .getLocalMember()
                                        .getAddress()
                                        .getPort()
                                + RestConstant.SYSTEM_MONITORING_INFORMATION)
                .then()
                .assertThat()
                .time(lessThan(5000L))
                .statusCode(200);
    }

    @Test
    public void testSubmitJob() {
        Response response = submitJob("BATCH");
        response.then().statusCode(200).body("jobName", equalTo("test测试"));
        String jobId = response.getBody().jsonPath().getString("jobId");
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer)
                        hazelcastInstance
                                .node
                                .getNodeExtension()
                                .createExtensionServices()
                                .get(Constant.SEATUNNEL_SERVICE_NAME);

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobStatus(Long.parseLong(jobId))));

        given().get(
                        HOST
                                + hazelcastInstance
                                        .getCluster()
                                        .getLocalMember()
                                        .getAddress()
                                        .getPort()
                                + RestConstant.FINISHED_JOBS_INFO
                                + "/FINISHED")
                .then()
                .statusCode(200)
                .body("[0].jobName", equalTo("test测试"))
                .body("[0].errorMsg", equalTo(null))
                .body("[0].jobDag.jobId", equalTo(Long.parseLong(jobId)))
                .body("[0].metrics.SourceReceivedCount", equalTo("100"))
                .body("[0].metrics.SinkWriteCount", equalTo("100"))
                .body("[0].jobStatus", equalTo("FINISHED"));
    }

    @Test
    public void testStopJob() {
        String jobId = submitJob("STREAMING").getBody().jsonPath().getString("jobId");
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer)
                        hazelcastInstance
                                .node
                                .getNodeExtension()
                                .createExtensionServices()
                                .get(Constant.SEATUNNEL_SERVICE_NAME);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobStatus(Long.parseLong(jobId))));

        String parameters = "{" + "\"jobId\":" + jobId + "," + "\"isStopWithSavePoint\":true}";

        given().body(parameters)
                .post(
                        HOST
                                + hazelcastInstance
                                        .getCluster()
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
                                        JobStatus.FINISHED,
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobStatus(Long.parseLong(jobId))));

        String jobId2 = submitJob("STREAMING").getBody().jsonPath().getString("jobId");

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobStatus(Long.parseLong(jobId2))));
        parameters = "{" + "\"jobId\":" + jobId2 + "," + "\"isStopWithSavePoint\":false}";

        given().body(parameters)
                .post(
                        HOST
                                + hazelcastInstance
                                        .getCluster()
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
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobStatus(Long.parseLong(jobId2))));
    }

    @Test
    public void testStartWithSavePointWithoutJobId() {
        Response response = submitJob("BATCH", true);
        response.then()
                .statusCode(400)
                .body("message", equalTo("Please provide jobId when start with save point."));
    }

    @Test
    public void testEncryptConfig() {
        String config =
                "{\n"
                        + "    \"env\": {\n"
                        + "        \"execution.parallelism\": 1,\n"
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
                                + hazelcastInstance
                                        .getCluster()
                                        .getLocalMember()
                                        .getAddress()
                                        .getPort()
                                + RestConstant.ENCRYPT_CONFIG)
                .then()
                .statusCode(200)
                .body("source[0].result_table_name", equalTo("fake"))
                .body("source[0].username", equalTo("c2VhdHVubmVs"))
                .body("source[0].password", equalTo("c2VhdHVubmVsX3Bhc3N3b3Jk"));
    }

    @AfterEach
    void afterClass() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    private Response submitJob(String jobMode) {
        return submitJob(jobMode, false);
    }

    private Response submitJob(String jobMode, boolean isStartWithSavePoint) {
        String requestBody =
                "{\n"
                        + "    \"env\": {\n"
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
        String parameters = "jobName=test测试";
        if (isStartWithSavePoint) {
            parameters = parameters + "&isStartWithSavePoint=true";
        }
        Response response =
                given().body(requestBody)
                        .post(
                                HOST
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
