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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.sun.net.httpserver.HttpServer;
import io.restassured.response.Response;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

public class MultiTableMetricsIT {

    private static final String HOST = "http://localhost:";

    private static ClientJobProxy batchJobProxy;

    private static HazelcastInstanceImpl node1;

    private static SeaTunnelClient engineClient;

    private final List<JsonNode> lineageEvents = new CopyOnWriteArrayList<>();
    private HttpServer lineageReceiver;

    @BeforeEach
    void beforeClass() throws Exception {
        lineageReceiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        lineageReceiver.createContext(
                "/events",
                exchange -> {
                    try {
                        lineageEvents.add(new ObjectMapper().readTree(exchange.getRequestBody()));
                        exchange.sendResponseHeaders(200, -1);
                    } finally {
                        exchange.close();
                    }
                });
        lineageReceiver.start();
        String testClusterName = TestUtils.getClusterName("MultiTableMetricsIT");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        Map<String, Object> lineageOptions = new HashMap<>();
        lineageOptions.put(LineageConfig.ENABLED, true);
        lineageOptions.put(LineageConfig.JOB_NAME_PER_OUTPUT, true);
        lineageOptions.put(
                LineageConfig.URL,
                "http://127.0.0.1:" + lineageReceiver.getAddress().getPort() + "/events");
        seaTunnelConfig.getEngineConfig().setLineageOptions(lineageOptions);
        node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);

        String batchFilePath = TestUtils.getResource("batch_fake_multi_table_to_console.conf");
        JobConfig batchConf = new JobConfig();
        batchConf.setName("batch_fake_multi_table_to_console");
        ClientJobExecutionEnvironment batchJobExecutionEnv =
                engineClient.createExecutionContext(batchFilePath, batchConf, seaTunnelConfig);
        LogicalDag dag = batchJobExecutionEnv.getLogicalDag();
        // Console/FakeSource have no production lineage identity. Assign test identities before
        // serialization so the real reporter and HTTP backend exercise their metrics end to end.
        dag.getLogicalVertexMap()
                .values()
                .forEach(
                        vertex -> {
                            if (vertex.getAction() instanceof SinkAction) {
                                SinkAction<?, ?, ?, ?> sink =
                                        (SinkAction<?, ?, ?, ?>) vertex.getAction();
                                String table =
                                        ((MultiTableSink) sink.getSink())
                                                .getSinkTables()
                                                .get(0)
                                                .getFullName();
                                sink.setLineageDatasets(
                                        Collections.singletonList(
                                                LineageDataset.of("test://console", table)));
                            } else if (vertex.getAction() instanceof SourceAction) {
                                SourceAction<?, ?, ?> source =
                                        (SourceAction<?, ?, ?>) vertex.getAction();
                                source.setLineageDatasets(
                                        Collections.singletonList(
                                                LineageDataset.of(
                                                        "test://fake", "input-" + source.getId())));
                            }
                        });
        JobImmutableInformation information =
                new JobImmutableInformation(
                        Long.parseLong(batchConf.getJobContext().getJobId()),
                        batchConf.getName(),
                        false,
                        node1.node.getNodeEngine().getSerializationService(),
                        dag,
                        Collections.emptyList(),
                        Collections.emptyList());
        batchJobProxy = engineClient.getJobClient().createJobProxy(information);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, batchJobProxy.getJobStatus()));
    }

    @Test
    public void multiTableMetrics() {
        Collections.singletonList(node1)
                .forEach(
                        instance -> {
                            Response response =
                                    given().get(
                                                    HOST
                                                            + instance.getCluster()
                                                                    .getLocalMember()
                                                                    .getAddress()
                                                                    .getPort()
                                                            + RestConstant.CONTEXT_PATH
                                                            + RestConstant.REST_URL_JOB_INFO
                                                            + "/"
                                                            + batchJobProxy.getJobId());
                            // In the test example, the data size of a single [3, "C", 100] is 13
                            int dataSize = 13;
                            response.prettyPrint();
                            response.then()
                                    .statusCode(200)
                                    .body("jobName", equalTo("batch_fake_multi_table_to_console"))
                                    .body("jobStatus", equalTo("FINISHED"))
                                    .body("metrics.SourceReceivedCount", equalTo("15"))
                                    .body("metrics.SinkWriteCount", equalTo("15"))
                                    .body(
                                            "metrics.TableSourceReceivedCount.'Source[0].fake.table1'",
                                            equalTo("10"))
                                    .body(
                                            "metrics.TableSourceReceivedCount.'Source[1].fake.public.table2'",
                                            equalTo("5"))
                                    .body(
                                            "metrics.TableSinkWriteCount.'Sink[0].fake.table1'",
                                            equalTo("10"))
                                    .body(
                                            "metrics.TableSinkWriteCount.'Sink[1].fake.public.table2'",
                                            equalTo("5"))
                                    .body(
                                            "metrics.SourceReceivedBytes",
                                            equalTo(String.valueOf(dataSize * 15)))
                                    .body(
                                            "metrics.SinkWriteBytes",
                                            equalTo(String.valueOf(dataSize * 15)))
                                    .body(
                                            "metrics.TableSourceReceivedBytes.'Source[0].fake.table1'",
                                            equalTo(String.valueOf(dataSize * 10)))
                                    .body(
                                            "metrics.TableSourceReceivedBytes.'Source[1].fake.public.table2'",
                                            equalTo(String.valueOf(dataSize * 5)))
                                    .body(
                                            "metrics.TableSinkWriteBytes.'Sink[0].fake.table1'",
                                            equalTo(String.valueOf(dataSize * 10)))
                                    .body(
                                            "metrics.TableSinkWriteBytes.'Sink[1].fake.public.table2'",
                                            equalTo(String.valueOf(dataSize * 5)));
                            Assertions.assertTrue(
                                    Double.parseDouble(response.path("metrics.SourceReceivedQPS"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSourceReceivedQPS.'Source[0].fake.table1'"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSourceReceivedQPS.'Source[1].fake.public.table2'"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path("metrics.SinkWriteQPS"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSinkWriteQPS.'Sink[0].fake.table1'"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSinkWriteQPS.'Sink[1].fake.public.table2'"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.SourceReceivedBytesPerSeconds"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSourceReceivedBytesPerSeconds.'Source[0].fake.table1'"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSourceReceivedBytesPerSeconds.'Source[1].fake.public.table2'"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.SinkWriteBytesPerSeconds"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSinkWriteBytesPerSeconds.'Sink[0].fake.table1'"))
                                                    > 0
                                            && Double.parseDouble(
                                                            response.path(
                                                                    "metrics.TableSinkWriteBytesPerSeconds.'Sink[1].fake.public.table2'"))
                                                    > 0);
                        });
    }

    @Test
    public void shouldEmitCompleteStatisticsForEachSink() {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(2, completeEvents().size()));
        Map<String, JsonNode> outputs = new HashMap<>();
        for (JsonNode event : completeEvents()) {
            Assertions.assertEquals(1, event.path("inputs").size());
            Assertions.assertEquals(1, event.path("outputs").size());
            JsonNode output = event.path("outputs").get(0);
            Assertions.assertNull(outputs.put(output.path("name").asText(), event));
        }
        Assertions.assertEquals(2, outputs.size());
        Assertions.assertTrue(outputs.containsKey("fake.table1"));
        Assertions.assertTrue(outputs.containsKey("fake.public.table2"));
        for (Map.Entry<String, JsonNode> entry : outputs.entrySet()) {
            Assertions.assertEquals(
                    "batch_fake_multi_table_to_console::test://console/" + entry.getKey(),
                    entry.getValue().path("job").path("name").asText());
            long rows = entry.getKey().equals("fake.table1") ? 10 : 5;
            JsonNode statistics =
                    entry.getValue()
                            .path("outputs")
                            .get(0)
                            .path("outputFacets")
                            .path("outputStatistics");
            Assertions.assertEquals(rows, statistics.path("rowCount").asLong());
            Assertions.assertEquals(13 * rows, statistics.path("size").asLong());
            Assertions.assertEquals(
                    "committed",
                    entry.getValue()
                            .path("run")
                            .path("facets")
                            .path(LineageConfig.DEFAULT_RUN_FACET)
                            .path("output_statistics_semantics")
                            .asText());
            String runId = entry.getValue().path("run").path("runId").asText();
            List<String> runStates =
                    lineageEvents.stream()
                            .filter(event -> event.path("run").path("runId").asText().equals(runId))
                            .map(event -> event.path("eventType").asText())
                            .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("START", "COMPLETE"), runStates);
            Assertions.assertEquals(
                    1,
                    lineageEvents.stream()
                            .filter(
                                    event ->
                                            event.path("eventType").asText().equals("START")
                                                    && event.path("run")
                                                            .path("runId")
                                                            .asText()
                                                            .equals(runId))
                            .count());
        }
        Assertions.assertEquals(
                2,
                completeEvents().stream()
                        .map(event -> event.path("run").path("runId").asText())
                        .distinct()
                        .count());
    }

    private List<JsonNode> completeEvents() {
        return lineageEvents.stream()
                .filter(event -> event.path("eventType").asText().equals("COMPLETE"))
                .collect(Collectors.toList());
    }

    @AfterEach
    void afterClass() {
        if (engineClient != null) {
            engineClient.close();
        }

        if (node1 != null) {
            node1.shutdown();
        }
        if (lineageReceiver != null) {
            lineageReceiver.stop(0);
        }
    }
}
