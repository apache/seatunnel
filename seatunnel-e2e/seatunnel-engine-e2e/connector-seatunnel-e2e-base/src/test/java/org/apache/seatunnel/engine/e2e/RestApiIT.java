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
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
public class RestApiIT {

    private static final String HOST = "http://localhost:";

    private static ClientJobProxy clientJobProxy;

    private static ClientJobProxy batchJobProxy;

    private static HazelcastInstanceImpl node1;

    private static HazelcastInstanceImpl node2;

    private static SeaTunnelClient engineClient;

    private static SeaTunnelConfig node1Config;

    private static SeaTunnelConfig node2Config;

    private static Map<Integer, Integer> ports;

    @BeforeEach
    void beforeClass() throws Exception {

        String testClusterName = TestUtils.getClusterName("RestApiIT");
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(
                Paths.get(
                                PROJECT_ROOT_PATH
                                        + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/job-log-file/log4j2.properties")
                        .toUri());
        node1Config = ConfigProvider.locateAndGetSeaTunnelConfig();
        node1Config.getEngineConfig().getHttpConfig().setPort(8080);
        node1Config.getEngineConfig().getHttpConfig().setEnabled(true);
        node1Config.getHazelcastConfig().setClusterName(testClusterName);
        node1Config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        node1Config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        MemberAttributeConfig node1Tags = new MemberAttributeConfig();
        node1Tags.setAttribute("node", "node1");
        node1Config.getHazelcastConfig().setMemberAttributeConfig(node1Tags);
        node1 = SeaTunnelServerStarter.createHazelcastInstance(node1Config);

        MemberAttributeConfig node2Tags = new MemberAttributeConfig();
        node2Tags.setAttribute("node", "node2");
        Config node2hzconfig = node1Config.getHazelcastConfig().setMemberAttributeConfig(node2Tags);
        node2Config = ConfigProvider.locateAndGetSeaTunnelConfig();
        node2Config.getEngineConfig().getHttpConfig().setPort(8081);
        node2Config.getEngineConfig().getHttpConfig().setEnabled(true);
        node2Config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        node2Config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        node2Config.setHazelcastConfig(node2hzconfig);
        node2 = SeaTunnelServerStarter.createHazelcastInstance(node2Config);

        String filePath = TestUtils.getResource("stream_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, node1Config);

        clientJobProxy = jobExecutionEnv.execute();

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus()));

        String batchFilePath = TestUtils.getResource("fakesource_to_console.conf");
        JobConfig batchConf = new JobConfig();
        batchConf.setName("fake_to_console");
        ClientJobExecutionEnvironment batchJobExecutionEnv =
                engineClient.createExecutionContext(batchFilePath, batchConf, node1Config);
        batchJobProxy = batchJobExecutionEnv.execute();
        Awaitility.await()
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, batchJobProxy.getJobStatus()));
        ports = new HashMap<>();
        ports.put(
                node1.getCluster().getLocalMember().getAddress().getPort(),
                node1Config.getEngineConfig().getHttpConfig().getPort());
        ports.put(
                node2.getCluster().getLocalMember().getAddress().getPort(),
                node2Config.getEngineConfig().getHttpConfig().getPort());
    }

    @Test
    public void testGetLog() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance ->
                                ports.forEach(
                                        (key, value) -> {
                                            // Verify log list interface logs/
                                            given().get(
                                                            HOST
                                                                    + key
                                                                    + CONTEXT_PATH
                                                                    + RestConstant.GET_LOGS)
                                                    .then()
                                                    .statusCode(200)
                                                    .body(
                                                            containsString(
                                                                    clientJobProxy.getJobId()
                                                                            + ".log"));

                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant.GET_LOGS)
                                                    .then()
                                                    .statusCode(200)
                                                    .body(
                                                            containsString(
                                                                    clientJobProxy.getJobId()
                                                                            + ".log"));

                                            // Verify log list interface logs/:jobId
                                            String logListV1 =
                                                    given().get(
                                                                    HOST
                                                                            + key
                                                                            + CONTEXT_PATH
                                                                            + RestConstant.GET_LOGS
                                                                            + "/"
                                                                            + clientJobProxy
                                                                                    .getJobId())
                                                            .body()
                                                            .prettyPrint();
                                            Assertions.assertTrue(
                                                    logListV1.contains(
                                                            clientJobProxy.getJobId() + ".log"));

                                            String logListV2 =
                                                    given().get(
                                                                    HOST
                                                                            + value
                                                                            + node1Config
                                                                                    .getEngineConfig()
                                                                                    .getHttpConfig()
                                                                                    .getContextPath()
                                                                            + RestConstant.GET_LOGS
                                                                            + "/"
                                                                            + clientJobProxy
                                                                                    .getJobId())
                                                            .body()
                                                            .prettyPrint();
                                            Assertions.assertTrue(
                                                    logListV2.contains(
                                                            clientJobProxy.getJobId() + ".log"));

                                            // verify access log link
                                            verifyLogLink(logListV1);
                                            verifyLogLink(logListV2);
                                        }));
    }

    private static void verifyLogLink(String logListV1) {
        Pattern pattern = Pattern.compile("href\\s*=\\s*\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(logListV1);
        while (matcher.find()) {
            String link = matcher.group(1);
            Assertions.assertTrue(
                    given().get(link)
                            .body()
                            .prettyPrint()
                            .contains("Init JobMaster for Job fake_to_file"));
        }
    }

    @Test
    public void testClearTags() {

        String config = "{}";
        given().get(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.OVERVIEW
                                + "?node=node1")
                .then()
                .statusCode(200)
                .body("projectVersion", notNullValue())
                .body("totalSlot", equalTo("20"))
                .body("workers", equalTo("1"));
        given().body(config)
                .put(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.UPDATE_TAGS_URL)
                .then()
                .statusCode(200)
                .body("message", equalTo("update node tags done."));

        given().get(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.OVERVIEW
                                + "?node=node1")
                .then()
                .statusCode(200)
                .body("projectVersion", notNullValue())
                .body("totalSlot", equalTo("0"))
                .body("workers", equalTo("0"));
    }

    @Test
    public void testGetRunningThreads() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance ->
                                ports.forEach(
                                        (key, value) -> {
                                            given().get(
                                                            HOST
                                                                    + key
                                                                    + CONTEXT_PATH
                                                                    + RestConstant.RUNNING_THREADS)
                                                    .then()
                                                    .statusCode(200)
                                                    .body("[0].threadName", notNullValue())
                                                    .body("[0].classLoader", notNullValue());
                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant.RUNNING_THREADS)
                                                    .then()
                                                    .statusCode(200)
                                                    .body("[0].threadName", notNullValue())
                                                    .body("[0].classLoader", notNullValue());
                                        }));
    }

    @Test
    public void testSystemMonitoringInformation() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance ->
                                ports.forEach(
                                        (key, value) -> {
                                            given().get(
                                                            HOST
                                                                    + key
                                                                    + CONTEXT_PATH
                                                                    + RestConstant
                                                                            .SYSTEM_MONITORING_INFORMATION)
                                                    .then()
                                                    .assertThat()
                                                    .time(lessThan(5000L))
                                                    .body("[0].host", equalTo("localhost"))
                                                    .body("[0].port", notNullValue())
                                                    .body("[0].isMaster", notNullValue())
                                                    .statusCode(200);
                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant
                                                                            .SYSTEM_MONITORING_INFORMATION)
                                                    .then()
                                                    .assertThat()
                                                    .time(lessThan(5000L))
                                                    .body("[0].host", equalTo("localhost"))
                                                    .body("[0].port", notNullValue())
                                                    .body("[0].isMaster", notNullValue())
                                                    .statusCode(200);
                                        }));
    }

    @Test
    public void testEncryptConfig() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
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
                                                                + key
                                                                + CONTEXT_PATH
                                                                + RestConstant.ENCRYPT_CONFIG)
                                                .then()
                                                .statusCode(200)
                                                .body(
                                                        "source[0].result_table_name",
                                                        equalTo("fake"))
                                                .body("source[0].username", equalTo("c2VhdHVubmVs"))
                                                .body(
                                                        "source[0].password",
                                                        equalTo("c2VhdHVubmVsX3Bhc3N3b3Jk"));

                                        given().body(config)
                                                .post(
                                                        HOST
                                                                + value
                                                                + node1Config
                                                                        .getEngineConfig()
                                                                        .getHttpConfig()
                                                                        .getContextPath()
                                                                + RestConstant.ENCRYPT_CONFIG)
                                                .then()
                                                .statusCode(200)
                                                .body(
                                                        "source[0].result_table_name",
                                                        equalTo("fake"))
                                                .body("source[0].username", equalTo("c2VhdHVubmVs"))
                                                .body(
                                                        "source[0].password",
                                                        equalTo("c2VhdHVubmVsX3Bhc3N3b3Jk"));
                                    });
                        });
    }

    @Test
    public void testGetThreadDump() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().get(
                                                        HOST
                                                                + key
                                                                + CONTEXT_PATH
                                                                + RestConstant.THREAD_DUMP)
                                                .then()
                                                .statusCode(200)
                                                .body("[0].threadName", notNullValue())
                                                .body("[0].threadState", notNullValue())
                                                .body("[0].stackTrace", notNullValue())
                                                .body("[0].threadId", notNullValue());
                                        given().get(
                                                        HOST
                                                                + value
                                                                + node1Config
                                                                        .getEngineConfig()
                                                                        .getHttpConfig()
                                                                        .getContextPath()
                                                                + RestConstant.THREAD_DUMP)
                                                .then()
                                                .statusCode(200)
                                                .body("[0].threadName", notNullValue())
                                                .body("[0].threadState", notNullValue())
                                                .body("[0].stackTrace", notNullValue())
                                                .body("[0].threadId", notNullValue());
                                    });
                        });
    }

    @AfterEach
    void afterClass() {
        if (engineClient != null) {
            engineClient.close();
        }

        if (node1 != null) {
            node1.shutdown();
        }
        if (node2 != null) {
            node2.shutdown();
        }
    }
}
