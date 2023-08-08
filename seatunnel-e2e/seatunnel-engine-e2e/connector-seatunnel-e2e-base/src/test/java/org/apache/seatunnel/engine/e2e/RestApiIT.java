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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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

    @BeforeAll
    static void beforeClass() throws Exception {
        String testClusterName = TestUtils.getClusterName("RestApiIT");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("stream_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig);

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
        String requestBody =
                "{\n"
                        + "    \"env\": {\n"
                        + "        \"job.mode\": \"batch\"\n"
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
        String parameters = "jobId=1&jobName=test&isStartWithSavePoint=false";
        // Only jobName is compared because jobId is randomly generated if isStartWithSavePoint is
        // false
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

        response.then().statusCode(200).body("jobName", equalTo("test"));
        String jobId = response.getBody().jsonPath().getString("jobId");
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer)
                        hazelcastInstance
                                .node
                                .getNodeExtension()
                                .createExtensionServices()
                                .get(Constant.SEATUNNEL_SERVICE_NAME);
        JobStatus jobStatus =
                seaTunnelServer.getCoordinatorService().getJobStatus(Long.parseLong(jobId));
        Assertions.assertEquals(JobStatus.RUNNING, jobStatus);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        seaTunnelServer
                                                .getCoordinatorService()
                                                .getJobStatus(Long.parseLong(jobId))));
    }

    @AfterAll
    static void afterClass() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }
}
