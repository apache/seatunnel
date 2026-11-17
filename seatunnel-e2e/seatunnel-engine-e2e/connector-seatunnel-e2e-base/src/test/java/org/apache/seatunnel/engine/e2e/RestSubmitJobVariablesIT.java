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
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;

/** Submits one templated config several times through the REST API with different variables. */
public class RestSubmitJobVariablesIT {

    private static final String JOB_FILE = "rest_submit_variables_fake_to_assert.conf";

    private static final String JSON_JOB =
            "{\"env\": {\"job.mode\": \"BATCH\", \"parallelism\": 1,"
                    + " \"job.name\": \"rest_variables_${run}\"},"
                    + " \"source\": [{\"plugin_name\": \"FakeSource\", \"plugin_output\": \"fake\","
                    + " \"row.num\": \"${rows}\", \"schema\": {\"table\": \"${database}.${table}\","
                    + " \"fields\": {\"name\": \"string\", \"age\": \"int\"}}}],"
                    + " \"sink\": [{\"plugin_name\": \"Assert\", \"plugin_input\": \"fake\","
                    + " \"rules\": {\"row_rules\": [{\"rule_type\": \"MAX_ROW\", \"rule_value\": \"${rows}\"},"
                    + " {\"rule_type\": \"MIN_ROW\", \"rule_value\": \"${rows}\"}]}}]}";

    @TempDir Path tempDir;

    private HazelcastInstanceImpl node;
    private SeaTunnelClient engineClient;
    private SeaTunnelConfig seaTunnelConfig;
    private String baseUrl;
    private DeployMode previousDeployMode;

    @BeforeEach
    void setUp() {
        previousDeployMode = Common.getDeployMode();
        String testClusterName = TestUtils.getClusterName("RestSubmitJobVariablesIT");
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        seaTunnelConfig
                .getEngineConfig()
                .getCheckpointConfig()
                .getStorage()
                .getStoragePluginConfig()
                .put("namespace", tempDir.resolve("checkpoints").toString());
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(18085);

        node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        SeaTunnelServer server = node.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .ignoreExceptions()
                .atMost(30, TimeUnit.SECONDS)
                .until(
                        () ->
                                given().get(
                                                        "http://localhost:"
                                                                + server.getHttpPort()
                                                                + "/overview")
                                                .statusCode()
                                        == 200);
        baseUrl = "http://localhost:" + server.getHttpPort();

        Common.setDeployMode(DeployMode.CLIENT);
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
    }

    @AfterEach
    void tearDown() {
        try {
            if (engineClient != null) {
                engineClient.close();
            }
        } finally {
            try {
                if (node != null) {
                    node.shutdown();
                }
            } finally {
                Common.setDeployMode(previousDeployMode);
            }
        }
    }

    @Test
    void testOneConfigRunsWithDifferentVariables() throws Exception {
        String config =
                new String(
                        Files.readAllBytes(Paths.get(TestUtils.getResource(JOB_FILE))),
                        StandardCharsets.UTF_8);

        String orders =
                submit(
                        "/submit-job?format=hocon",
                        "run=orders,rows=3,database=shop,table=orders",
                        config);
        String refunds =
                submit(
                        "/submit-job?format=hocon",
                        "run=refunds,rows=7,database=shop,table=refunds",
                        config);

        assertFinished(orders, "rest_variables_orders", 3, "shop.orders");
        assertFinished(refunds, "rest_variables_refunds", 7, "shop.refunds");
    }

    @Test
    void testOptionalVariablesOverrideDefaultAndKeepAbsentDefault() throws Exception {
        String config =
                new String(
                                Files.readAllBytes(Paths.get(TestUtils.getResource(JOB_FILE))),
                                StandardCharsets.UTF_8)
                        .replace("row.num = ${rows}", "row.num = 3\nrow.num = ${?rows}");

        String supplied =
                submit(
                        "/submit-job?format=hocon",
                        "run=optional_supplied,rows=5,database=shop,table=supplied",
                        config);
        assertFinished(supplied, "rest_variables_optional_supplied", 5, "shop.supplied");

        String absent =
                submit(
                        "/submit-job?format=hocon",
                        "run=optional_absent,rows=3,database=shop,table=absent",
                        config.replace("${?rows}", "${?not_supplied}"));
        assertFinished(absent, "rest_variables_optional_absent", 3, "shop.absent");
    }

    @Test
    void testVariablesPreserveLiteralSchemaKeys() {
        String config = JSON_JOB.replace("\"name\": \"string\"", "\"^t_nova_.*$\": \"string\"");
        String jobId =
                submit(
                        "/submit-job?format=json",
                        "run=literal_keys,rows=5,database=shop,table=literal_keys",
                        config);

        assertFinished(jobId, "rest_variables_literal_keys", 5, "shop.literal_keys");
    }

    @Test
    void testSubmitJobsWithVariablesPerJob() {
        String body =
                "["
                        + JSON_JOB.replaceFirst(
                                "\\{",
                                "{\"params\": {\"variables\":"
                                        + " \"run=first,rows=2,database=shop,table=first\"},")
                        + ","
                        + JSON_JOB.replaceFirst(
                                "\\{",
                                "{\"params\": {}, \"variables\": {\"run\": \"second\","
                                        + " \"rows\": 4, \"database\": \"shop\", \"table\": \"second\"},")
                        + "]";

        List<String> jobIds =
                given().body(body)
                        .post(baseUrl + "/submit-jobs")
                        .then()
                        .statusCode(200)
                        .extract()
                        .jsonPath()
                        .getList("jobId");

        assertFinished(jobIds.get(0), "rest_variables_first", 2, "shop.first");
        assertFinished(jobIds.get(1), "rest_variables_second", 4, "shop.second");
    }

    @Test
    void testRestSubmissionMatchesCliSubmission() throws Exception {
        // the fixture of the command line variable e2e; Assert counts rows per table across jobs
        // in one JVM, so this comparison uses a Console sink
        String filePath = TestUtils.getResource("fake_to_console.variables.conf");
        List<String> cliVariables =
                Arrays.asList(
                        "resName=fake_table",
                        "rowNum=10",
                        "strTemplate=[abc,def]",
                        "nameType=string",
                        "nameVal=abc",
                        "pluginInputIdentifier=sql");
        ClientJobProxy cliJob =
                engineClient
                        .createExecutionContext(
                                filePath, cliVariables, new JobConfig(), seaTunnelConfig)
                        .execute();
        String restJobId =
                submit(
                        "/submit-job?format=hocon",
                        String.join(",", cliVariables),
                        new String(
                                Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8));

        JsonPath cli = awaitEnded(String.valueOf(cliJob.getJobId()));
        JsonPath rest = awaitEnded(restJobId);
        Assertions.assertEquals(JobStatus.FINISHED.name(), cli.getString("jobStatus"));
        Assertions.assertEquals(
                JobStatus.FINISHED.name(), rest.getString("jobStatus"), rest.prettify());
        Assertions.assertEquals("20", rest.getString("metrics.SourceReceivedCount"));
        for (String path :
                Arrays.asList(
                        "jobDag.envOptions",
                        "jobDag.pipelineEdges",
                        "jobDag.vertexInfoMap.type",
                        "jobDag.vertexInfoMap.vertexName",
                        "jobDag.vertexInfoMap.tablePaths",
                        "metrics.SourceReceivedCount")) {
            Assertions.assertEquals(
                    String.valueOf(cli.<Object>get(path)),
                    String.valueOf(rest.<Object>get(path)),
                    path);
        }
    }

    private String submit(String path, String variables, String body) {
        Response response =
                given().queryParam("variables", variables).body(body).post(baseUrl + path);
        Assertions.assertEquals(200, response.getStatusCode(), response.asString());
        return response.jsonPath().getString("jobId");
    }

    private JsonPath awaitEnded(String jobId) {
        return Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(
                        () -> given().get(baseUrl + "/job-info/" + jobId).jsonPath(),
                        info -> isEnded(info.getString("jobStatus")));
    }

    private void assertFinished(String jobId, String jobName, int rows, String tablePath) {
        JsonPath jobInfo = awaitEnded(jobId);
        Assertions.assertEquals(
                JobStatus.FINISHED.name(), jobInfo.getString("jobStatus"), jobInfo.prettify());
        Assertions.assertEquals(jobName, jobInfo.getString("jobName"));
        Assertions.assertEquals(
                String.valueOf(rows), jobInfo.getString("metrics.SourceReceivedCount"));
        Assertions.assertTrue(
                jobInfo.getList("jobDag.vertexInfoMap.tablePaths").toString().contains(tablePath),
                jobInfo.prettify());
    }

    private static boolean isEnded(String status) {
        return status != null && JobStatus.valueOf(status).isEndState();
    }
}
