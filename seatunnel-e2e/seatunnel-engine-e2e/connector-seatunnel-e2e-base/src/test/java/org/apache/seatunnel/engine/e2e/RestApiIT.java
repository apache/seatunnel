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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
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
import com.hazelcast.instance.impl.Node;
import io.restassured.common.mapper.TypeRef;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
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

    private static CheckpointMonitorService checkpointMonitorService;

    @BeforeEach
    void beforeClass() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(
                Paths.get(
                                PROJECT_ROOT_PATH
                                        + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/job-log-file/log4j2.properties")
                        .toUri());
        String testClusterName = TestUtils.getClusterName("RestApiIT");
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
        // Dynamically generated port
        node2Config.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);
        node2Config.getEngineConfig().getHttpConfig().setEnabled(true);
        node2Config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        node2Config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        node2Config.setHazelcastConfig(node2hzconfig);
        node2 = SeaTunnelServerStarter.createHazelcastInstance(node2Config);

        checkpointMonitorService = resolveCheckpointMonitorService(node1);

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
                                                                    + RestConstant.REST_URL_LOGS)
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
                                                                    + RestConstant.REST_URL_LOGS)
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
                                                                            + RestConstant
                                                                                    .REST_URL_LOGS
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
                                                                            + RestConstant
                                                                                    .REST_URL_LOGS
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

    private CheckpointMonitorService resolveCheckpointMonitorService(
            HazelcastInstanceImpl instance) {
        try {
            Field nodeField = HazelcastInstanceImpl.class.getDeclaredField("node");
            nodeField.setAccessible(true);
            Node node = (Node) nodeField.get(instance);
            SeaTunnelServer seaTunnelServer =
                    (SeaTunnelServer)
                            node.getNodeExtension()
                                    .createExtensionServices()
                                    .get(Constant.SEATUNNEL_SERVICE_NAME);
            return seaTunnelServer.getCheckpointMonitorService();
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve CheckpointMonitorService", e);
        }
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
    public void testGetRunningJobById() {

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
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/"
                                                                    + clientJobProxy.getJobId())
                                                    .then()
                                                    .statusCode(200)
                                                    .body("jobName", equalTo("fake_to_file"))
                                                    .body("jobStatus", equalTo("RUNNING"));

                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/"
                                                                    + clientJobProxy.getJobId())
                                                    .then()
                                                    .statusCode(200)
                                                    .body("jobName", equalTo("fake_to_file"))
                                                    .body("jobStatus", equalTo("RUNNING"));
                                        }));
    }

    @Test
    public void testGetJobById() {
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
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/"
                                                                    + batchJobProxy.getJobId())
                                                    .then()
                                                    .statusCode(200)
                                                    .body("jobName", equalTo("fake_to_console"))
                                                    .body("jobStatus", equalTo("FINISHED"));

                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/"
                                                                    + batchJobProxy.getJobId())
                                                    .then()
                                                    .statusCode(200)
                                                    .body("jobName", equalTo("fake_to_console"))
                                                    .body("jobStatus", equalTo("FINISHED"));
                                        }));
    }

    @Test
    public void testGetAnNotExistJobById() {
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
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/"
                                                                    + 123)
                                                    .then()
                                                    .statusCode(200)
                                                    .body("jobId", equalTo("123"));

                                            given().get(
                                                            HOST
                                                                    + key
                                                                    + CONTEXT_PATH
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/")
                                                    .then()
                                                    .statusCode(400);

                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/"
                                                                    + 123)
                                                    .then()
                                                    .statusCode(200)
                                                    .body("jobId", equalTo("123"));

                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_JOB
                                                                    + "/")
                                                    .then()
                                                    .statusCode(400);
                                        }));
    }

    @Test
    public void testGetRunningJobs() {
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
                                                                            .REST_URL_RUNNING_JOBS)
                                                    .then()
                                                    .statusCode(200)
                                                    .body(
                                                            "[0].jobDag.jobId",
                                                            equalTo(
                                                                    Long.toString(
                                                                            clientJobProxy
                                                                                    .getJobId())))
                                                    .body("[0].jobDag.pipelineEdges", hasKey("1"))
                                                    .body(
                                                            "[0].jobDag.pipelineEdges['1']",
                                                            hasSize(1))
                                                    .body(
                                                            "[0].jobDag.pipelineEdges['1'][0].inputVertexId",
                                                            equalTo("1"))
                                                    .body(
                                                            "[0].jobDag.pipelineEdges['1'][0].targetVertexId",
                                                            equalTo("2"))
                                                    .body("[0].jobDag.vertexInfoMap", hasSize(2))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].vertexId",
                                                            equalTo(1))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].type",
                                                            equalTo("source"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].vertexName",
                                                            equalTo(
                                                                    "pipeline-1 [Source[0]-FakeSource]"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].tablePaths[0]",
                                                            equalTo("fake"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].vertexId",
                                                            equalTo(2))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].type",
                                                            equalTo("sink"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].vertexName",
                                                            equalTo(
                                                                    "pipeline-1 [Sink[0]-LocalFile-MultiTableSink]"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].tablePaths[0]",
                                                            equalTo("fake"))
                                                    .body(
                                                            "[0].jobDag.envOptions.'job.mode'",
                                                            equalTo("STREAMING"))
                                                    .body(
                                                            "[0].jobDag.envOptions.'checkpoint.interval'",
                                                            equalTo("5000"))
                                                    .body("[0].jobName", equalTo("fake_to_file"))
                                                    .body("[0].jobStatus", equalTo("RUNNING"));

                                            given().get(
                                                            HOST
                                                                    + value
                                                                    + node1Config
                                                                            .getEngineConfig()
                                                                            .getHttpConfig()
                                                                            .getContextPath()
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_JOBS)
                                                    .then()
                                                    .statusCode(200)
                                                    .body(
                                                            "[0].jobDag.jobId",
                                                            equalTo(
                                                                    Long.toString(
                                                                            clientJobProxy
                                                                                    .getJobId())))
                                                    .body("[0].jobDag.pipelineEdges", hasKey("1"))
                                                    .body(
                                                            "[0].jobDag.pipelineEdges['1']",
                                                            hasSize(1))
                                                    .body(
                                                            "[0].jobDag.pipelineEdges['1'][0].inputVertexId",
                                                            equalTo("1"))
                                                    .body(
                                                            "[0].jobDag.pipelineEdges['1'][0].targetVertexId",
                                                            equalTo("2"))
                                                    .body("[0].jobDag.vertexInfoMap", hasSize(2))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].vertexId",
                                                            equalTo(1))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].type",
                                                            equalTo("source"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].vertexName",
                                                            equalTo(
                                                                    "pipeline-1 [Source[0]-FakeSource]"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[0].tablePaths[0]",
                                                            equalTo("fake"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].vertexId",
                                                            equalTo(2))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].type",
                                                            equalTo("sink"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].vertexName",
                                                            equalTo(
                                                                    "pipeline-1 [Sink[0]-LocalFile-MultiTableSink]"))
                                                    .body(
                                                            "[0].jobDag.vertexInfoMap[1].tablePaths[0]",
                                                            equalTo("fake"))
                                                    .body(
                                                            "[0].jobDag.envOptions.'job.mode'",
                                                            equalTo("STREAMING"))
                                                    .body(
                                                            "[0].jobDag.envOptions.'checkpoint.interval'",
                                                            equalTo("5000"))
                                                    .body("[0].jobName", equalTo("fake_to_file"))
                                                    .body("[0].jobStatus", equalTo("RUNNING"));
                                        }));
    }

    @Test
    public void testGetJobInfoByJobId() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().get(
                                                        HOST
                                                                + key
                                                                + CONTEXT_PATH
                                                                + RestConstant.REST_URL_JOB_INFO
                                                                + "/"
                                                                + batchJobProxy.getJobId())
                                                .then()
                                                .statusCode(200)
                                                .body(
                                                        "jobDag.jobId",
                                                        equalTo(
                                                                Long.toString(
                                                                        batchJobProxy.getJobId())))
                                                .body("jobDag.pipelineEdges", hasKey("1"))
                                                .body("jobDag.pipelineEdges['1']", hasSize(1))
                                                .body(
                                                        "jobDag.pipelineEdges['1'][0].inputVertexId",
                                                        equalTo("1"))
                                                .body(
                                                        "jobDag.pipelineEdges['1'][0].targetVertexId",
                                                        equalTo("2"))
                                                .body("jobDag.vertexInfoMap", hasSize(2))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].vertexId",
                                                        equalTo(1))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].type",
                                                        equalTo("source"))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].vertexName",
                                                        equalTo(
                                                                "pipeline-1 [Source[0]-FakeSource]"))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].tablePaths[0]",
                                                        equalTo("fake"))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].vertexId",
                                                        equalTo(2))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].type",
                                                        equalTo("sink"))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].vertexName",
                                                        equalTo(
                                                                "pipeline-1 [Sink[0]-console-MultiTableSink]"))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].tablePaths[0]",
                                                        equalTo("fake"))
                                                .body(
                                                        "metrics.TableSourceReceivedCount.'Source[0].fake'",
                                                        equalTo("5"))
                                                .body(
                                                        "metrics.TableSinkWriteCount.'Sink[0].fake'",
                                                        equalTo("5"))
                                                .body("metrics.SinkWriteCount", equalTo("5"))
                                                .body("metrics.SourceReceivedCount", equalTo("5"))
                                                .body(
                                                        "jobDag.envOptions.'job.mode'",
                                                        equalTo("BATCH"))
                                                .body("jobName", equalTo("fake_to_console"))
                                                .body("jobStatus", equalTo("FINISHED"));

                                        given().get(
                                                        HOST
                                                                + value
                                                                + node1Config
                                                                        .getEngineConfig()
                                                                        .getHttpConfig()
                                                                        .getContextPath()
                                                                + RestConstant.REST_URL_JOB_INFO
                                                                + "/"
                                                                + batchJobProxy.getJobId())
                                                .then()
                                                .statusCode(200)
                                                .body(
                                                        "jobDag.jobId",
                                                        equalTo(
                                                                Long.toString(
                                                                        batchJobProxy.getJobId())))
                                                .body("jobDag.pipelineEdges", hasKey("1"))
                                                .body("jobDag.pipelineEdges['1']", hasSize(1))
                                                .body(
                                                        "jobDag.pipelineEdges['1'][0].inputVertexId",
                                                        equalTo("1"))
                                                .body(
                                                        "jobDag.pipelineEdges['1'][0].targetVertexId",
                                                        equalTo("2"))
                                                .body("jobDag.vertexInfoMap", hasSize(2))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].vertexId",
                                                        equalTo(1))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].type",
                                                        equalTo("source"))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].vertexName",
                                                        equalTo(
                                                                "pipeline-1 [Source[0]-FakeSource]"))
                                                .body(
                                                        "jobDag.vertexInfoMap[0].tablePaths[0]",
                                                        equalTo("fake"))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].vertexId",
                                                        equalTo(2))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].type",
                                                        equalTo("sink"))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].vertexName",
                                                        equalTo(
                                                                "pipeline-1 [Sink[0]-console-MultiTableSink]"))
                                                .body(
                                                        "jobDag.vertexInfoMap[1].tablePaths[0]",
                                                        equalTo("fake"))
                                                .body(
                                                        "metrics.TableSourceReceivedCount.'Source[0].fake'",
                                                        equalTo("5"))
                                                .body(
                                                        "metrics.TableSinkWriteCount.'Sink[0].fake'",
                                                        equalTo("5"))
                                                .body("metrics.SinkWriteCount", equalTo("5"))
                                                .body("metrics.SourceReceivedCount", equalTo("5"))
                                                .body("metrics.IntermediateQueueSize", equalTo("0"))
                                                .body(
                                                        "jobDag.envOptions.'job.mode'",
                                                        equalTo("BATCH"))
                                                .body("jobName", equalTo("fake_to_console"))
                                                .body("jobStatus", equalTo("FINISHED"));
                                    });
                        });
    }

    @Test
    public void testOverview() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().get(
                                                        HOST
                                                                + key
                                                                + CONTEXT_PATH
                                                                + RestConstant.REST_URL_OVERVIEW)
                                                .then()
                                                .statusCode(200)
                                                .body("projectVersion", notNullValue())
                                                .body("totalSlot", equalTo("40"))
                                                .body("workers", equalTo("2"))
                                                .body("pendingJobs", notNullValue());
                                        given().get(
                                                        HOST
                                                                + value
                                                                + node1Config
                                                                        .getEngineConfig()
                                                                        .getHttpConfig()
                                                                        .getContextPath()
                                                                + RestConstant.REST_URL_OVERVIEW)
                                                .then()
                                                .statusCode(200)
                                                .body("projectVersion", notNullValue())
                                                .body("totalSlot", equalTo("40"))
                                                .body("workers", equalTo("2"))
                                                .body("pendingJobs", notNullValue());
                                    });
                        });
    }

    @Test
    public void testOverviewFilterByTag() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().get(
                                                        HOST
                                                                + key
                                                                + CONTEXT_PATH
                                                                + RestConstant.REST_URL_OVERVIEW
                                                                + "?node=node1")
                                                .then()
                                                .statusCode(200)
                                                .body("projectVersion", notNullValue())
                                                .body("totalSlot", equalTo("20"))
                                                .body("workers", equalTo("1"));
                                        given().get(
                                                        HOST
                                                                + value
                                                                + node1Config
                                                                        .getEngineConfig()
                                                                        .getHttpConfig()
                                                                        .getContextPath()
                                                                + RestConstant.REST_URL_OVERVIEW
                                                                + "?node=node1")
                                                .then()
                                                .statusCode(200)
                                                .body("projectVersion", notNullValue())
                                                .body("totalSlot", equalTo("20"))
                                                .body("workers", equalTo("1"));
                                    });
                        });
    }

    @Test
    public void testUpdateTagsSuccess() {

        String config = "{\n" + "    \"tag1\": \"dev_1\",\n" + "    \"tag2\": \"dev_2\"\n" + "}";
        given().get(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.REST_URL_OVERVIEW
                                + "?tag1=dev_1")
                .then()
                .statusCode(200)
                .body("projectVersion", notNullValue())
                .body("totalSlot", equalTo("0"))
                .body("workers", equalTo("0"));
        given().body(config)
                .put(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.REST_URL_UPDATE_TAGS)
                .then()
                .statusCode(200)
                .body("message", equalTo("update node tags done."));

        given().get(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.REST_URL_OVERVIEW
                                + "?tag1=dev_1")
                .then()
                .statusCode(200)
                .body("projectVersion", notNullValue())
                .body("totalSlot", equalTo("20"))
                .body("workers", equalTo("1"));
    }

    @Test
    public void testUpdateTagsFail() {

        given().put(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.REST_URL_UPDATE_TAGS)
                .then()
                .statusCode(400)
                .body("message", equalTo("Request body is empty."));
    }

    @Test
    public void testClearTags() {

        String config = "{}";
        given().get(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.REST_URL_OVERVIEW
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
                                + RestConstant.REST_URL_UPDATE_TAGS)
                .then()
                .statusCode(200)
                .body("message", equalTo("update node tags done."));

        given().get(
                        HOST
                                + node1.getCluster().getLocalMember().getAddress().getPort()
                                + CONTEXT_PATH
                                + RestConstant.REST_URL_OVERVIEW
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
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_THREADS)
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
                                                                    + RestConstant
                                                                            .REST_URL_RUNNING_THREADS)
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
                                                                            .REST_URL_SYSTEM_MONITORING_INFORMATION)
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
                                                                            .REST_URL_SYSTEM_MONITORING_INFORMATION)
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
                                                        + "            \"plugin_output\": \"fake\",\n"
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
                                                                + RestConstant
                                                                        .REST_URL_ENCRYPT_CONFIG)
                                                .then()
                                                .statusCode(200)
                                                .body("source[0].plugin_output", equalTo("fake"))
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
                                                                + RestConstant
                                                                        .REST_URL_ENCRYPT_CONFIG)
                                                .then()
                                                .statusCode(200)
                                                .body("source[0].plugin_output", equalTo("fake"))
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
                                                                + RestConstant.REST_URL_THREAD_DUMP)
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
                                                                + RestConstant.REST_URL_THREAD_DUMP)
                                                .then()
                                                .statusCode(200)
                                                .body("[0].threadName", notNullValue())
                                                .body("[0].threadState", notNullValue())
                                                .body("[0].stackTrace", notNullValue())
                                                .body("[0].threadId", notNullValue());
                                    });
                        });
    }

    @Test
    public void verifyHtmlResponseBasic() {
        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().get(
                                                        HOST
                                                                + value
                                                                + node1Config
                                                                        .getEngineConfig()
                                                                        .getHttpConfig()
                                                                        .getContextPath())
                                                .then()
                                                .statusCode(200)
                                                .contentType(containsString("text/html"))
                                                .body(containsString("<html"))
                                                .body(
                                                        containsString(
                                                                "<title>Seatunnel Engine UI</title>"));
                                    });
                        });
    }

    @Test
    public void testSubmitJobWithSqlFormat() {
        String sqlConfig =
                "/* config\n"
                        + "env {\n"
                        + "  parallelism = 1\n"
                        + "  job.mode = \"BATCH\"\n"
                        + "}\n"
                        + "*/\n"
                        + "\n"
                        + "CREATE TABLE test_source (\n"
                        + "    id INT,\n"
                        + "    name STRING,\n"
                        + "    c_time TIMESTAMP\n"
                        + ") WITH (\n"
                        + "    'connector' = 'FakeSource',\n"
                        + "    'schema' = '{ \n"
                        + "      fields { \n"
                        + "        id = \"int\", \n"
                        + "        name = \"string\",\n"
                        + "        c_time = \"timestamp\"\n"
                        + "      } \n"
                        + "    }',\n"
                        + "    'rows' = '[ \n"
                        + "      { fields = [1, \"test\", null], kind = INSERT }\n"
                        + "    ]',\n"
                        + "    'type' = 'source'\n"
                        + ");\n"
                        + "\n"
                        + "CREATE TABLE test_sink (\n"
                        + "    id INT,\n"
                        + "    name STRING,\n"
                        + "    c_time TIMESTAMP\n"
                        + ") WITH (\n"
                        + "    'connector' = 'Console',\n"
                        + "    'type' = 'sink'\n"
                        + ");\n"
                        + "\n"
                        + "INSERT INTO test_sink SELECT * FROM test_source;";

        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().body(sqlConfig)
                                                .queryParam("format", "sql")
                                                .queryParam("jobName", "test-sql-job")
                                                .post(HOST + key + CONTEXT_PATH + "/submit-job")
                                                .then()
                                                .statusCode(200)
                                                .body("jobId", notNullValue())
                                                .body("jobName", equalTo("test-sql-job"));
                                    });
                        });
    }

    @Test
    public void testSubmitJobWithJsonFormat() {
        String jsonConfig =
                "{\n"
                        + "    \"env\": {\n"
                        + "        \"parallelism\": 1,\n"
                        + "        \"job.mode\": \"BATCH\"\n"
                        + "    },\n"
                        + "    \"source\": [\n"
                        + "        {\n"
                        + "            \"plugin_name\": \"FakeSource\",\n"
                        + "            \"plugin_output\": \"fake\",\n"
                        + "            \"row.num\": 2,\n"
                        + "            \"schema\": {\n"
                        + "                \"fields\": {\n"
                        + "                    \"name\": \"string\",\n"
                        + "                    \"age\": \"int\"\n"
                        + "                }\n"
                        + "            }\n"
                        + "        }\n"
                        + "    ],\n"
                        + "    \"sink\": [\n"
                        + "        {\n"
                        + "            \"plugin_name\": \"Console\",\n"
                        + "            \"plugin_input\": [\"fake\"]\n"
                        + "        }\n"
                        + "    ]\n"
                        + "}";

        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().body(jsonConfig)
                                                .queryParam("jobName", "test-json-job")
                                                .post(HOST + key + CONTEXT_PATH + "/submit-job")
                                                .then()
                                                .statusCode(200)
                                                .body("jobId", notNullValue())
                                                .body("jobName", equalTo("test-json-job"));
                                    });
                        });
    }

    @Test
    public void testSubmitJobWithHoconFormat() {
        String hoconConfig =
                "env {\n"
                        + "  parallelism = 1\n"
                        + "  job.mode = \"BATCH\"\n"
                        + "}\n"
                        + "\n"
                        + "source {\n"
                        + "  FakeSource {\n"
                        + "    plugin_output = \"fake\"\n"
                        + "    row.num = 2\n"
                        + "    schema = {\n"
                        + "      fields {\n"
                        + "        name = \"string\"\n"
                        + "        age = \"int\"\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n"
                        + "\n"
                        + "sink {\n"
                        + "  Console {\n"
                        + "    plugin_input = \"fake\"\n"
                        + "  }\n"
                        + "}";

        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().body(hoconConfig)
                                                .queryParam("format", "hocon")
                                                .queryParam("jobName", "test-hocon-job")
                                                .post(HOST + key + CONTEXT_PATH + "/submit-job")
                                                .then()
                                                .statusCode(200)
                                                .body("jobId", notNullValue())
                                                .body("jobName", equalTo("test-hocon-job"));
                                    });
                        });
    }

    @Test
    public void testCheckpointOverviewAndHistoryApi() {
        long jobId = clientJobProxy.getJobId();
        List<Integer> httpPorts = new ArrayList<>(ports.values());

        AtomicReference<Map<String, Object>> overviewRef = new AtomicReference<>();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .until(
                        () -> {
                            Map<String, Object> overview =
                                    getCheckpointOverview(
                                            jobId, buildHttpBaseUrl(httpPorts.get(0)));
                            List<Map<String, Object>> pipelines =
                                    castList(overview.get("pipelines"));
                            if (pipelines.isEmpty()) {
                                return false;
                            }
                            Map<String, Object> pipeline = pipelines.get(0);
                            Map<String, Object> counts = castMap(pipeline.get("counts"));
                            if (counts.isEmpty()) {
                                return false;
                            }
                            if (getLong(counts, "completed") > 0L) {
                                overviewRef.set(overview);
                                return true;
                            }
                            return false;
                        });

        Map<String, Object> latestOverview = overviewRef.get();
        Assertions.assertNotNull(
                latestOverview, "Failed to fetch checkpoint overview with completed counts");
        List<Map<String, Object>> pipelines = castList(latestOverview.get("pipelines"));
        Assertions.assertFalse(
                pipelines.isEmpty(), "Checkpoint overview does not contain any pipelines");
        Map<String, Object> pipeline = pipelines.get(0);
        int pipelineId = ((Number) pipeline.get("pipelineId")).intValue();
        Map<String, Object> counts = castMap(pipeline.get("counts"));
        Assertions.assertFalse(counts.isEmpty(), "Checkpoint overview missing count metrics");
        Assertions.assertTrue(getLong(counts, "triggered") >= 1L);
        Assertions.assertTrue(getLong(counts, "completed") >= 1L);
        Assertions.assertEquals(0L, getLong(counts, "failed"));

        long failedCheckpointId = System.currentTimeMillis();
        checkpointMonitorService.onCheckpointFailed(
                jobId,
                pipelineId,
                failedCheckpointId,
                CheckpointType.CHECKPOINT_TYPE,
                CheckpointCloseReason.CHECKPOINT_EXPIRED,
                new RuntimeException("mock failure"),
                System.currentTimeMillis());
        long inProgressCheckpointId = failedCheckpointId + 1;
        checkpointMonitorService.onCheckpointTriggered(
                jobId,
                pipelineId,
                inProgressCheckpointId,
                CheckpointType.CHECKPOINT_TYPE,
                System.currentTimeMillis(),
                4);
        checkpointMonitorService.onCheckpointAcknowledge(
                jobId, pipelineId, inProgressCheckpointId, 2, 4);
        checkpointMonitorService.onPipelineRestored(jobId, pipelineId);

        httpPorts.stream()
                .map(this::buildHttpBaseUrl)
                .forEach(
                        baseUrl ->
                                Awaitility.await()
                                        .atMost(30, TimeUnit.SECONDS)
                                        .untilAsserted(
                                                () -> {
                                                    Map<String, Object> overview =
                                                            getCheckpointOverview(jobId, baseUrl);
                                                    Map<String, Object> targetPipeline =
                                                            findPipeline(overview, pipelineId);
                                                    Map<String, Object> targetCounts =
                                                            castMap(targetPipeline.get("counts"));
                                                    Assertions.assertTrue(
                                                            getLong(targetCounts, "failed") >= 1L);
                                                    Assertions.assertTrue(
                                                            getLong(targetCounts, "restored")
                                                                    >= 1L);
                                                    List<Map<String, Object>> inProgress =
                                                            castList(
                                                                    targetPipeline.get(
                                                                            "inProgress"));
                                                    Assertions.assertTrue(
                                                            inProgress.stream()
                                                                    .map(this::castMap)
                                                                    .anyMatch(
                                                                            info ->
                                                                                    getLong(
                                                                                                            info,
                                                                                                            "checkpointId")
                                                                                                    == inProgressCheckpointId
                                                                                            && getInt(
                                                                                                            info,
                                                                                                            "acknowledged")
                                                                                                    == 2
                                                                                            && getInt(
                                                                                                            info,
                                                                                                            "total")
                                                                                                    == 4));
                                                    List<Map<String, Object>> history =
                                                            getCheckpointHistory(
                                                                    jobId, baseUrl, "FAILED");
                                                    Assertions.assertTrue(
                                                            history.stream()
                                                                    .map(
                                                                            record ->
                                                                                    castMap(
                                                                                            record
                                                                                                    .get(
                                                                                                            "checkpoint")))
                                                                    .anyMatch(
                                                                            checkpoint ->
                                                                                    getLong(
                                                                                                    checkpoint,
                                                                                                    "checkpointId")
                                                                                            == failedCheckpointId));
                                                }));
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

    @SuppressWarnings("unchecked")
    private <T> List<T> castList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        return (List<T>) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        if (value == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) value;
    }

    private long getLong(Map<String, Object> source, String key) {
        Object value = source.get(key);
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private int getInt(Map<String, Object> source, String key) {
        Object value = source.get(key);
        return value instanceof Number ? ((Number) value).intValue() : 0;
    }

    private Map<String, Object> getCheckpointOverview(long jobId, String baseUrl) {
        return given().get(baseUrl + RestConstant.REST_URL_CHECKPOINT_OVERVIEW + "/" + jobId)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<Map<String, Object>>() {});
    }

    private List<Map<String, Object>> getCheckpointHistory(
            long jobId, String baseUrl, String status) {
        return given().queryParam("status", status)
                .get(baseUrl + RestConstant.REST_URL_CHECKPOINT_HISTORY + "/" + jobId)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<List<Map<String, Object>>>() {});
    }

    private Map<String, Object> findPipeline(Map<String, Object> overview, int pipelineId) {
        return castList(overview.get("pipelines")).stream()
                .map(item -> (Map<String, Object>) item)
                .filter(pipeline -> ((Number) pipeline.get("pipelineId")).intValue() == pipelineId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Pipeline not found"));
    }

    private String buildHttpBaseUrl(int httpPort) {
        return HOST + httpPort + node1Config.getEngineConfig().getHttpConfig().getContextPath();
    }
}
