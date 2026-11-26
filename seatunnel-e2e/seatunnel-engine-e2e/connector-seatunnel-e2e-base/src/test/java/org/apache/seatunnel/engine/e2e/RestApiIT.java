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
import org.apache.seatunnel.engine.server.diagnostic.JobRuntimeDiagnostics;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.engine.server.rest.service.LogService;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.engine.server.rest.RestConstant.CONTEXT_PATH;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
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

    /**
     * The write (1-based, per writer instance) that the InMemory sink fixture fails. The source
     * fixture emits two records every 20 ms, so this lands roughly six seconds after the job
     * starts. A one-second checkpoint interval therefore completes several checkpoints before the
     * failure, which makes the restore a checkpoint restore rather than a restart from scratch.
     */
    private static final long FAIL_AT_WRITE = 600L;

    private static final String DYNAMIC_FAIL_AT_WRITE = "dynamic_fail_at_write";

    private static final String DYNAMIC_FAIL_AT_WRITE_MARKER = "dynamic_fail_at_write_marker";

    /** Part of the message thrown by the InMemory sink fixture; must survive into the cause. */
    private static final String INJECTED_FAILURE_TEXT =
            "InMemory sink injected one-shot write failure at record";

    /**
     * Placeholder {@code SubPlan#retainRestoreCause} stores when no error was captured. Seeing it
     * here would mean the real failure text was lost during the restore.
     */
    private static final String NO_ERROR_RECORDED_PLACEHOLDER = "no error was recorded";

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
        node1Config.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);
        node1Config.getEngineConfig().getHttpConfig().setLogResponseMaxSizeMb(1);
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
        // Both members deliberately share the same configured port and mutable HTTP bean.
        // Node2 must bind another port without changing what either member advertises.
        node2Config.getEngineConfig().setHttpConfig(node1Config.getEngineConfig().getHttpConfig());
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
        ports.put(node1.getCluster().getLocalMember().getAddress().getPort(), httpPort(node1));
        ports.put(node2.getCluster().getLocalMember().getAddress().getPort(), httpPort(node2));
    }

    @Test
    public void testLogResponseLimitIsAppliedToV1AndV2() throws Exception {
        Path directory = Paths.get(new LogService(node1.node.getNodeEngine()).getLogPath());
        Path file = Files.createTempFile(directory, "rest-response-limit-", ".log");
        StringBuilder content = new StringBuilder("FIRST-RECORD-MUST-BE-TRUNCATED\n");
        for (int i = 0; i < 40000; i++) {
            content.append("日志0123456789abcdefghijklmnopqrstuvwxyz\n");
        }
        content.append("END-OF-LOG\n");
        String original = content.toString();
        int limit = 1024 * 1024;
        Files.write(file, original.getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(Files.size(file) > limit);
        String v1 =
                HOST + node1.getCluster().getLocalMember().getAddress().getPort() + CONTEXT_PATH;
        String v2 = buildHttpBaseUrl(httpPort(node1));
        try {
            for (String base : Arrays.asList(v1, v2)) {
                for (String endpoint :
                        Arrays.asList(RestConstant.REST_URL_LOG, RestConstant.REST_URL_LOGS)) {
                    String response =
                            given().get(base + endpoint + "/" + file.getFileName())
                                    .then()
                                    .statusCode(200)
                                    .extract()
                                    .asString();
                    String[] parts = response.split("\n", 2);
                    Assertions.assertTrue(parts[0].startsWith("[SeaTunnel] Log truncated:"));
                    Assertions.assertEquals(2, parts.length);
                    String tail = parts[1];
                    Assertions.assertTrue(tail.getBytes(StandardCharsets.UTF_8).length <= limit);
                    Assertions.assertTrue(original.endsWith(tail));
                    Assertions.assertTrue(tail.endsWith("END-OF-LOG\n"));
                    Assertions.assertTrue(tail.contains("日志"));
                    Assertions.assertFalse(tail.contains("\uFFFD"));
                    Assertions.assertFalse(tail.contains("FIRST-RECORD-MUST-BE-TRUNCATED"));
                }
            }
        } finally {
            Files.deleteIfExists(file);
        }
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

    @Test
    public void testDynamicHttpPortIsResolvableByPeers() throws Exception {
        int node1HttpPort = httpPort(node1);
        int node2HttpPort = httpPort(node2);

        Assertions.assertSame(
                node1Config.getEngineConfig().getHttpConfig(),
                node2Config.getEngineConfig().getHttpConfig());
        Assertions.assertEquals(8080, node2Config.getEngineConfig().getHttpConfig().getPort());
        Assertions.assertNotEquals(
                node1HttpPort,
                node2HttpPort,
                "node2 must expose the dynamically chosen REST port, not the configured one");

        // Embedded members share the Log4j context and job-log directory. Both must have a
        // log to list, otherwise counting distinct nodes could hide a fan-out regression.
        Path node1LogPath =
                Paths.get(new LogService(node1.node.getNodeEngine()).getLogPath()).toRealPath();
        Path node2LogPath =
                Paths.get(new LogService(node2.node.getNodeEngine()).getLogPath()).toRealPath();
        Assertions.assertEquals(node1LogPath, node2LogPath);
        Assertions.assertTrue(
                node1LogPath
                        .resolve("job-" + clientJobProxy.getJobId() + ".log")
                        .toFile()
                        .isFile());

        List<Map<String, Object>> logEntries =
                given().get(
                                buildHttpBaseUrl(node1HttpPort)
                                        + RestConstant.REST_URL_LOGS
                                        + "?format=JSON")
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<List<Map<String, Object>>>() {});

        Set<String> reportedNodes =
                logEntries.stream()
                        .map(entry -> String.valueOf(entry.get("node")))
                        .collect(Collectors.toSet());
        Assertions.assertEquals(
                new HashSet<>(expectedNodeIds()),
                reportedNodes,
                "GET /logs must enumerate both members, got " + reportedNodes);
        Assertions.assertTrue(
                reportedNodes.stream().anyMatch(node -> node.endsWith(":" + node2HttpPort)),
                "GET /logs must report node2 on its dynamic port, got " + reportedNodes);
    }

    @Test
    public void testLoggers() {
        String loggersUrl =
                HOST
                        + httpPort(node1)
                        + node1Config.getEngineConfig().getHttpConfig().getContextPath()
                        + RestConstant.REST_URL_LOGGERS;
        // a logger of this test only, so that changing its level cannot hide job logs
        String logger = "org.apache.seatunnel.engine.e2e.RestApiIT.loggers";

        // the list reports every configured logger, the root logger included
        given().get(loggersUrl)
                .then()
                .statusCode(200)
                .body("node", notNullValue())
                .body("loggers.name", hasItem("root"));

        given().get(loggersUrl + "/root")
                .then()
                .statusCode(200)
                .body("name", equalTo("root"))
                .body("level", notNullValue())
                .body("origin", equalTo("file"));

        // an unknown level is rejected instead of being reported as applied
        given().post(loggersUrl + "/" + logger + "?level=DEBUGG")
                .then()
                .statusCode(400)
                .body("message", containsString("valid levels are"));

        // a request without a level does not silently do nothing either
        given().post(loggersUrl + "/" + logger)
                .then()
                .statusCode(400)
                .body("message", containsString("level is required"));

        given().post(loggersUrl + "?level=DEBUG")
                .then()
                .statusCode(400)
                .body("message", containsString("logger name"));

        given().get(loggersUrl + "?scope=galaxy")
                .then()
                .statusCode(400)
                .body("message", containsString("valid scopes are"));

        // the level is applied and reported as a runtime override, not as the file level
        given().body("{\"level\":\"debug\"}")
                .post(loggersUrl + "/" + logger)
                .then()
                .statusCode(200)
                .body("status", equalTo("SUCCESS"))
                .body("name", equalTo(logger))
                .body("level", equalTo("DEBUG"))
                .body("origin", equalTo("runtime-override"));

        given().get(loggersUrl + "/" + logger)
                .then()
                .statusCode(200)
                .body("level", equalTo("DEBUG"))
                .body("origin", equalTo("runtime-override"));

        // and the override can be reverted, which the legacy log-level endpoint cannot do
        given().delete(loggersUrl + "/" + logger)
                .then()
                .statusCode(200)
                .body("status", equalTo("SUCCESS"))
                .body("previousLevel", equalTo("DEBUG"))
                .body("origin", equalTo("file"));

        given().delete(loggersUrl + "/" + logger)
                .then()
                .statusCode(200)
                .body("status", equalTo("NO_OVERRIDE"));

        // the cluster scope answers for every member
        given().get(loggersUrl + "?scope=cluster")
                .then()
                .statusCode(200)
                .body("scope", equalTo("cluster"))
                .body("status", equalTo("SUCCESS"))
                .body("nodes", hasSize(ports.size()))
                .body("nodes.node", containsInAnyOrder(expectedNodeIds().toArray(new String[0])));

        given().post(loggersUrl + "/" + logger + "?scope=cluster&level=TRACE")
                .then()
                .statusCode(200)
                .body("scope", equalTo("cluster"))
                .body("status", equalTo("SUCCESS"))
                .body("level", equalTo("TRACE"))
                .body("nodes", hasSize(ports.size()))
                .body("nodes.node", containsInAnyOrder(expectedNodeIds().toArray(new String[0])))
                .body("nodes[0].level", equalTo("TRACE"))
                .body("nodes[0].origin", equalTo("runtime-override"));

        given().delete(loggersUrl + "/" + logger + "?scope=cluster")
                .then()
                .statusCode(200)
                .body("scope", equalTo("cluster"))
                .body("status", equalTo("SUCCESS"))
                .body("nodes.node", containsInAnyOrder(expectedNodeIds().toArray(new String[0])))
                .body("nodes[0].origin", equalTo("file"));
    }

    private int httpPort(HazelcastInstanceImpl instance) {
        SeaTunnelServer server =
                instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getHttpPort();
    }

    private List<String> expectedNodeIds() {
        return Arrays.asList(node1, node2).stream()
                .map(
                        instance ->
                                instance.getCluster().getLocalMember().getAddress().getHost()
                                        + ":"
                                        + httpPort(instance))
                .collect(Collectors.toList());
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
    public void testGetJobDiagnosticsOfRunningJob() {
        // the diagnostics of a running job must be identical no matter which member serves the
        // request: the master builds them locally, other members fetch them from the master
        ports.forEach(
                (key, value) ->
                        given().get(
                                        HOST
                                                + value
                                                + node1Config
                                                        .getEngineConfig()
                                                        .getHttpConfig()
                                                        .getContextPath()
                                                + RestConstant.REST_URL_JOB_INFO
                                                + "/"
                                                + clientJobProxy.getJobId())
                                .then()
                                .statusCode(200)
                                .body("jobStatus", equalTo("RUNNING"))
                                .body(
                                        "diagnostics.jobId",
                                        equalTo(Long.toString(clientJobProxy.getJobId())))
                                .body("diagnostics.generatedAt", notNullValue())
                                .body("diagnostics.stateTimestamps.RUNNING", notNullValue())
                                .body("diagnostics.pipelines", hasSize(1))
                                .body("diagnostics.pipelines[0].pipelineId", equalTo(1))
                                .body("diagnostics.pipelines[0].pipelineStatus", equalTo("RUNNING"))
                                .body("diagnostics.pipelines[0].restoreCount", equalTo(0))
                                .body(
                                        "diagnostics.pipelines[0].stateTimestamps.RUNNING",
                                        notNullValue())
                                .body("diagnostics.totalPipelineRestoreCount", equalTo(0)));
    }

    /**
     * Verifies that a pipeline which really fails and restores is explained by the REST job
     * diagnostics, not only counted.
     *
     * <p>{@code SubPlan#restoreCauses} retains why each earlier run of a pipeline ended, because
     * {@code errorByPhysicalVertex} only describes the current attempt and the next attempt's
     * initialization replaces it. The cause therefore has to be captured while the failed run is
     * still visible, and it has to reach {@code diagnostics.pipelines[].restoreCauses} of {@code
     * /job-info} on every member. The retention and the JSON rendering each have unit tests, but no
     * test exercises the whole path: a task fails, the pipeline restores from a checkpoint, the job
     * keeps running, and the REST response explains the restore.
     *
     * <p>The failure is injected deterministically instead of by killing a node: the InMemory sink
     * fails exactly one configured write after periodic checkpoints have had time to complete, and
     * creates a marker file before throwing so the retried attempt cannot fail the same way again.
     * The assertions are the diagnostics contract: one retained cause per restore, oldest first,
     * the first cause is the sink's own failure text rather than the "no error was recorded"
     * placeholder, causes stay unchanged while the restored pipeline completes more checkpoints,
     * and both members report the same list.
     */
    @Test
    public void testJobDiagnosticsExplainPipelineRestore() throws Exception {
        Path markerDirectory = Files.createTempDirectory("seatunnel-restore-cause-");
        Path marker = markerDirectory.resolve("fail-once.marker");
        Path confFile =
                Paths.get(
                        System.getProperty("java.io.tmpdir"),
                        "seatunnel-restore-cause-" + System.nanoTime() + ".conf");
        ClientJobProxy restoredJob = null;
        try {
            Map<String, String> values = new HashMap<>();
            values.put(DYNAMIC_FAIL_AT_WRITE, String.valueOf(FAIL_AT_WRITE));
            values.put(
                    DYNAMIC_FAIL_AT_WRITE_MARKER,
                    marker.toAbsolutePath().toString().replace("\\", "\\\\"));
            TestUtils.createTestConfigFileFromTemplate(
                    "restore-cause/stream_checkpointable_to_inmemory_fail_once_template.conf",
                    values,
                    confFile.toString());

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("restore-cause-diagnostics");
            restoredJob =
                    engineClient
                            .createExecutionContext(confFile.toString(), jobConfig, node1Config)
                            .execute();
            final long jobId = restoredJob.getJobId();

            AtomicReference<Map<String, Object>> diagnostics = new AtomicReference<>();
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Map<String, Object> jobInfo =
                                        getJobInfoJson(jobId, httpPort(node1));
                                Map<String, Object> pipeline = firstPipeline(jobInfo);
                                Assertions.assertTrue(
                                        getInt(pipeline, JobRuntimeDiagnostics.RESTORE_COUNT) >= 1,
                                        "Waiting for the injected sink failure to trigger a "
                                                + "pipeline restore, job info: "
                                                + jobInfo);
                                Assertions.assertEquals(
                                        "RUNNING",
                                        pipeline.get(JobRuntimeDiagnostics.PIPELINE_STATUS),
                                        "Waiting for the restored pipeline to run again, job "
                                                + "info: "
                                                + jobInfo);
                                diagnostics.set(castMap(jobInfo.get(RestConstant.DIAGNOSTICS)));
                            });

            Map<String, Object> restoredDiagnostics = diagnostics.get();
            Map<String, Object> restoredPipeline = firstPipeline(diagnostics.get());
            int restoreCount = getInt(restoredPipeline, JobRuntimeDiagnostics.RESTORE_COUNT);
            Assertions.assertEquals(
                    restoreCount,
                    getInt(restoredDiagnostics, JobRuntimeDiagnostics.TOTAL_PIPELINE_RESTORE_COUNT),
                    "The single pipeline of this job must account for every restore");
            List<String> causes =
                    castStringList(restoredPipeline.get(JobRuntimeDiagnostics.RESTORE_CAUSES));
            Assertions.assertEquals(
                    restoreCount,
                    causes.size(),
                    "Every restore must retain exactly one cause, restored diagnostics: "
                            + restoredDiagnostics);
            for (int i = 0; i < causes.size(); i++) {
                Assertions.assertTrue(
                        causes.get(i).startsWith("restore " + (i + 1) + ": "),
                        "Causes must be retained oldest first with their attempt number, but entry "
                                + i
                                + " was: "
                                + causes.get(i));
            }
            Assertions.assertTrue(
                    causes.get(0).contains(INJECTED_FAILURE_TEXT),
                    "The first cause must be the sink failure that triggered the restore, not a "
                            + "placeholder, but was: "
                            + causes.get(0));
            Assertions.assertFalse(
                    causes.get(0).contains(NO_ERROR_RECORDED_PLACEHOLDER),
                    "The failure text was lost before the restore captured it: " + causes.get(0));

            // The cause list must describe earlier runs only: a successful retry must not clear it,
            // and must not append anything either. Several checkpoint intervals are observed so a
            // regression that clears the list when the new attempt makes progress is caught.
            long observationDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (System.nanoTime() < observationDeadline) {
                Map<String, Object> jobInfo = getJobInfoJson(jobId, httpPort(node1));
                Map<String, Object> pipeline = firstPipeline(jobInfo);
                Assertions.assertEquals(
                        "RUNNING",
                        pipeline.get(JobRuntimeDiagnostics.PIPELINE_STATUS),
                        "The restored job must keep running, job info: " + jobInfo);
                Assertions.assertEquals(
                        restoreCount,
                        getInt(pipeline, JobRuntimeDiagnostics.RESTORE_COUNT),
                        "The restore count must not change after the pipeline recovered, job info: "
                                + jobInfo);
                Assertions.assertEquals(
                        causes,
                        castStringList(pipeline.get(JobRuntimeDiagnostics.RESTORE_CAUSES)),
                        "Restore causes must survive the successful retry, job info: " + jobInfo);
                Thread.sleep(500L);
            }

            // Diagnostics are built by the active master and fetched through it from any member, so
            // both members must report the same restore history.
            Map<String, Object> otherMemberDiagnostics =
                    castMap(getJobInfoJson(jobId, httpPort(node2)).get(RestConstant.DIAGNOSTICS));
            Map<String, Object> otherMemberPipeline = firstPipeline(otherMemberDiagnostics);
            Assertions.assertEquals(
                    restoreCount,
                    getInt(otherMemberPipeline, JobRuntimeDiagnostics.RESTORE_COUNT),
                    "The other member must report the same restore count: "
                            + otherMemberDiagnostics);
            Assertions.assertEquals(
                    causes,
                    castStringList(otherMemberPipeline.get(JobRuntimeDiagnostics.RESTORE_CAUSES)),
                    "The other member must report the same restore causes: "
                            + otherMemberDiagnostics);
        } finally {
            if (restoredJob != null) {
                try {
                    restoredJob.cancelJob();
                } catch (RuntimeException e) {
                    log.warn("Failed to cancel the restore diagnostics test job", e);
                }
            }
            Files.deleteIfExists(confFile);
            Files.deleteIfExists(marker);
            Files.deleteIfExists(markerDirectory);
        }
    }

    private Map<String, Object> getJobInfoJson(long jobId, int httpPort) {
        return given().get(
                        buildHttpBaseUrl(httpPort) + RestConstant.REST_URL_JOB_INFO + "/" + jobId)
                .then()
                .statusCode(200)
                .extract()
                .as(new TypeRef<Map<String, Object>>() {});
    }

    private Map<String, Object> firstPipeline(Map<String, Object> jobInfoOrDiagnostics) {
        Map<String, Object> diagnostics =
                jobInfoOrDiagnostics.containsKey(RestConstant.DIAGNOSTICS)
                        ? castMap(jobInfoOrDiagnostics.get(RestConstant.DIAGNOSTICS))
                        : jobInfoOrDiagnostics;
        List<Map<String, Object>> pipelines = castList(diagnostics.get("pipelines"));
        Assertions.assertFalse(
                pipelines == null || pipelines.isEmpty(),
                "No pipeline diagnostics in: " + jobInfoOrDiagnostics);
        return pipelines.get(0);
    }

    @SuppressWarnings("unchecked")
    private List<String> castStringList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        return (List<String>) value;
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
        // The nested schema field name and the top-level key cannot be parsed as config paths,
        // so both must stay literal instead of failing RestUtil.buildConfig.
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
                        + "                    \"age\": \"int\",\n"
                        + "                    \"^t_nova_.*$\": \"string\"\n"
                        + "                }\n"
                        + "            }\n"
                        + "        }\n"
                        + "    ],\n"
                        + "    \"sink\": [\n"
                        + "        {\n"
                        + "            \"plugin_name\": \"Console\",\n"
                        + "            \"plugin_input\": [\"fake\"]\n"
                        + "        }\n"
                        + "    ],\n"
                        + "    \"^t_nova_.*$\": \"literal\"\n"
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

    /**
     * HOCON bodies are parsed by the HOCON parser before the submit flow decrypts them. A key such
     * as {@code "$systemId"} has to be quoted in the body, because {@code $} is a reserved HOCON
     * character, and it has to stay a literal key when the decrypted config is re-parsed so that
     * the FieldMapper mapping keeps working.
     */
    @Test
    public void testSubmitJobWithHoconFormatAndQuotedLiteralKey() {
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
                        + "        \"$systemId\" = \"string\"\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n"
                        + "\n"
                        + "transform {\n"
                        + "  FieldMapper {\n"
                        + "    plugin_input = \"fake\"\n"
                        + "    plugin_output = \"mapped\"\n"
                        + "    field_mapper = {\n"
                        + "      \"$systemId\" = \"nova_system_id\"\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n"
                        + "\n"
                        + "sink {\n"
                        + "  Console {\n"
                        + "    plugin_input = \"mapped\"\n"
                        + "  }\n"
                        + "}";

        Arrays.asList(node2, node1)
                .forEach(
                        instance -> {
                            ports.forEach(
                                    (key, value) -> {
                                        given().body(hoconConfig)
                                                .queryParam("format", "hocon")
                                                .queryParam("jobName", "test-hocon-literal-key-job")
                                                .post(HOST + key + CONTEXT_PATH + "/submit-job")
                                                .then()
                                                .statusCode(200)
                                                .body("jobId", notNullValue())
                                                .body(
                                                        "jobName",
                                                        equalTo("test-hocon-literal-key-job"));
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
                                    getCheckpointOverview(jobId, buildHttpBaseUrl(httpPort(node1)));
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
