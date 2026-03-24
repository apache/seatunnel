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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.VertexInfo;
import org.apache.seatunnel.engine.core.parse.JobConfigParser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class BaseServiceTableMetricsTest {

    private JobInfoService jobInfoService;
    private Method getJobMetricsMethod;

    @BeforeEach
    void setUp() throws Exception {
        NodeEngineImpl nodeEngine = org.mockito.Mockito.mock(NodeEngineImpl.class);

        jobInfoService = new JobInfoService(nodeEngine);

        getJobMetricsMethod =
                BaseService.class.getDeclaredMethod(
                        "getJobMetrics", String.class, JobDAGInfo.class);
        getJobMetricsMethod.setAccessible(true);
    }

    @Test
    public void testTableQPSMetricsAggregation() throws Exception {
        String jobMetrics =
                "{"
                        + "\"SourceReceivedCount#fake.table1\": [{\"value\": 100}],"
                        + "\"SourceReceivedCount#fake.table2\": [{\"value\": 200}],"
                        + "\"SinkWriteCount#fake.table1\": [{\"value\": 90}],"
                        + "\"SinkWriteCount#fake.table2\": [{\"value\": 180}],"
                        + "\"SinkCommittedCount#fake.table1\": [{\"value\": 80}],"
                        + "\"SinkCommittedCount#fake.table2\": [{\"value\": 160}],"
                        + "\"SourceReceivedBytes#fake.table1\": [{\"value\": 1000}],"
                        + "\"SourceReceivedBytes#fake.table2\": [{\"value\": 2000}],"
                        + "\"SinkWriteBytes#fake.table1\": [{\"value\": 900}],"
                        + "\"SinkWriteBytes#fake.table2\": [{\"value\": 1800}],"
                        + "\"SinkCommittedBytes#fake.table1\": [{\"value\": 800}],"
                        + "\"SinkCommittedBytes#fake.table2\": [{\"value\": 1600}],"
                        + "\"SourceReceivedQPS#fake.table1\": [{\"value\": 10.5}],"
                        + "\"SourceReceivedQPS#fake.table2\": [{\"value\": 20.3}],"
                        + "\"SinkWriteQPS#fake.table1\": [{\"value\": 9.2}],"
                        + "\"SinkWriteQPS#fake.table2\": [{\"value\": 18.7}],"
                        + "\"SinkCommittedQPS#fake.table1\": [{\"value\": 8.1}],"
                        + "\"SinkCommittedQPS#fake.table2\": [{\"value\": 16.4}],"
                        + "\"SourceReceivedBytesPerSeconds#fake.table1\": [{\"value\": 105.5}],"
                        + "\"SourceReceivedBytesPerSeconds#fake.table2\": [{\"value\": 203.2}],"
                        + "\"SinkWriteBytesPerSeconds#fake.table1\": [{\"value\": 92.3}],"
                        + "\"SinkWriteBytesPerSeconds#fake.table2\": [{\"value\": 187.6}],"
                        + "\"SinkCommittedBytesPerSeconds#fake.table1\": [{\"value\": 81.2}],"
                        + "\"SinkCommittedBytesPerSeconds#fake.table2\": [{\"value\": 164.5}],"
                        + "\"SourceReceivedCount\": [{\"value\": 300}],"
                        + "\"SinkWriteCount\": [{\"value\": 270}],"
                        + "\"SinkCommittedCount\": [{\"value\": 240}],"
                        + "\"SourceReceivedBytes\": [{\"value\": 3000}],"
                        + "\"SinkWriteBytes\": [{\"value\": 2700}],"
                        + "\"SinkCommittedBytes\": [{\"value\": 2400}],"
                        + "\"SourceReceivedQPS\": [{\"value\": 30.8}],"
                        + "\"SinkWriteQPS\": [{\"value\": 27.9}],"
                        + "\"SinkCommittedQPS\": [{\"value\": 24.5}],"
                        + "\"SourceReceivedBytesPerSeconds\": [{\"value\": 308.7}],"
                        + "\"SinkWriteBytesPerSeconds\": [{\"value\": 279.9}],"
                        + "\"SinkCommittedBytesPerSeconds\": [{\"value\": 245.7}]"
                        + "}";

        Map<String, Object> result =
                (Map<String, Object>) getJobMetricsMethod.invoke(jobInfoService, jobMetrics, null);

        Map<String, Object> tableSourceQPS =
                (Map<String, Object>) result.get("TableSourceReceivedQPS");
        Assertions.assertNotNull(tableSourceQPS);
        Assertions.assertEquals(10.5, (Double) tableSourceQPS.get("fake.table1"), 0.01);
        Assertions.assertEquals(20.3, (Double) tableSourceQPS.get("fake.table2"), 0.01);

        Map<String, Object> tableSinkQPS = (Map<String, Object>) result.get("TableSinkWriteQPS");
        Assertions.assertNotNull(tableSinkQPS);
        Assertions.assertEquals(9.2, (Double) tableSinkQPS.get("fake.table1"), 0.01);
        Assertions.assertEquals(18.7, (Double) tableSinkQPS.get("fake.table2"), 0.01);

        Map<String, Object> tableSinkCommittedQPS =
                (Map<String, Object>) result.get("TableSinkCommittedQPS");
        Assertions.assertNotNull(tableSinkCommittedQPS);
        Assertions.assertEquals(8.1, (Double) tableSinkCommittedQPS.get("fake.table1"), 0.01);
        Assertions.assertEquals(16.4, (Double) tableSinkCommittedQPS.get("fake.table2"), 0.01);

        Map<String, Object> tableSourceBytesPerSec =
                (Map<String, Object>) result.get("TableSourceReceivedBytesPerSeconds");
        Assertions.assertNotNull(tableSourceBytesPerSec);
        Assertions.assertEquals(105.5, (Double) tableSourceBytesPerSec.get("fake.table1"), 0.01);
        Assertions.assertEquals(203.2, (Double) tableSourceBytesPerSec.get("fake.table2"), 0.01);

        Map<String, Object> tableSinkBytesPerSec =
                (Map<String, Object>) result.get("TableSinkWriteBytesPerSeconds");
        Assertions.assertNotNull(tableSinkBytesPerSec);
        Assertions.assertEquals(92.3, (Double) tableSinkBytesPerSec.get("fake.table1"), 0.01);
        Assertions.assertEquals(187.6, (Double) tableSinkBytesPerSec.get("fake.table2"), 0.01);

        Map<String, Object> tableSinkCommittedBytesPerSec =
                (Map<String, Object>) result.get("TableSinkCommittedBytesPerSeconds");
        Assertions.assertNotNull(tableSinkCommittedBytesPerSec);
        Assertions.assertEquals(
                81.2, (Double) tableSinkCommittedBytesPerSec.get("fake.table1"), 0.01);
        Assertions.assertEquals(
                164.5, (Double) tableSinkCommittedBytesPerSec.get("fake.table2"), 0.01);
    }

    @Test
    public void testTableCountMetricsAggregation() throws Exception {
        String jobMetrics =
                "{"
                        + "\"SourceReceivedCount#fake.table1\": [{\"value\": 100}, {\"value\": 50}],"
                        + "\"SourceReceivedCount#fake.table2\": [{\"value\": 200}, {\"value\": 100}],"
                        + "\"SinkWriteCount#fake.table1\": [{\"value\": 90}, {\"value\": 45}],"
                        + "\"SinkWriteCount#fake.table2\": [{\"value\": 180}, {\"value\": 90}],"
                        + "\"SinkCommittedCount#fake.table1\": [{\"value\": 80}, {\"value\": 40}],"
                        + "\"SinkCommittedCount#fake.table2\": [{\"value\": 160}, {\"value\": 80}],"
                        + "\"SourceReceivedCount\": [{\"value\": 300}],"
                        + "\"SinkWriteCount\": [{\"value\": 270}],"
                        + "\"SinkCommittedCount\": [{\"value\": 240}]"
                        + "}";

        Map<String, Object> result =
                (Map<String, Object>) getJobMetricsMethod.invoke(jobInfoService, jobMetrics, null);

        Map<String, Object> tableSourceCount =
                (Map<String, Object>) result.get("TableSourceReceivedCount");
        Assertions.assertNotNull(tableSourceCount);
        Assertions.assertEquals(150L, tableSourceCount.get("fake.table1"));
        Assertions.assertEquals(300L, tableSourceCount.get("fake.table2"));

        Map<String, Object> tableSinkCount =
                (Map<String, Object>) result.get("TableSinkWriteCount");
        Assertions.assertNotNull(tableSinkCount);
        Assertions.assertEquals(135L, tableSinkCount.get("fake.table1"));
        Assertions.assertEquals(270L, tableSinkCount.get("fake.table2"));

        Map<String, Object> tableSinkCommittedCount =
                (Map<String, Object>) result.get("TableSinkCommittedCount");
        Assertions.assertNotNull(tableSinkCommittedCount);
        Assertions.assertEquals(120L, tableSinkCommittedCount.get("fake.table1"));
        Assertions.assertEquals(240L, tableSinkCommittedCount.get("fake.table2"));
    }

    @Test
    public void testMixedMetricsWithMultipleWorkers() throws Exception {
        String jobMetrics =
                "{"
                        + "\"SourceReceivedQPS#fake.table1\": [{\"value\": 5.5}, {\"value\": 4.5}, {\"value\": 3.2}],"
                        + "\"SourceReceivedQPS#fake.table2\": [{\"value\": 10.2}, {\"value\": 9.8}, {\"value\": 8.5}],"
                        + "\"SinkCommittedQPS#fake.table1\": [{\"value\": 4.1}, {\"value\": 3.9}, {\"value\": 2.8}],"
                        + "\"SinkCommittedQPS#fake.table2\": [{\"value\": 8.2}, {\"value\": 7.8}, {\"value\": 6.5}],"
                        + "\"SourceReceivedQPS\": [{\"value\": 30.8}],"
                        + "\"SinkCommittedQPS\": [{\"value\": 24.5}]"
                        + "}";

        Map<String, Object> result =
                (Map<String, Object>) getJobMetricsMethod.invoke(jobInfoService, jobMetrics, null);

        Map<String, Object> tableSourceQPS =
                (Map<String, Object>) result.get("TableSourceReceivedQPS");
        Assertions.assertNotNull(tableSourceQPS);
        Assertions.assertEquals(13.2, (Double) tableSourceQPS.get("fake.table1"), 0.01);
        Assertions.assertEquals(28.5, (Double) tableSourceQPS.get("fake.table2"), 0.01);

        Map<String, Object> tableSinkCommittedQPS =
                (Map<String, Object>) result.get("TableSinkCommittedQPS");
        Assertions.assertNotNull(tableSinkCommittedQPS);
        Assertions.assertEquals(10.8, (Double) tableSinkCommittedQPS.get("fake.table1"), 0.01);
        Assertions.assertEquals(22.5, (Double) tableSinkCommittedQPS.get("fake.table2"), 0.01);
    }

    @Test
    public void testMultipleSinksWithSameTableName() throws Exception {
        String jobMetrics =
                "{"
                        + "\"SinkWriteCount#fake.user_table\": [{\"value\": 5}, {\"value\": 5}],"
                        + "\"SinkCommittedCount#fake.user_table\": [{\"value\": 5}, {\"value\": 5}],"
                        + "\"SourceReceivedCount#fake.user_table\": [{\"value\": 10}],"
                        + "\"SinkWriteCount\": [{\"value\": 10}],"
                        + "\"SinkCommittedCount\": [{\"value\": 10}],"
                        + "\"SourceReceivedCount\": [{\"value\": 10}]"
                        + "}";

        JobDAGInfo dagInfo = createDAGInfoWithMultipleSinks();

        Map<String, Object> result =
                (Map<String, Object>)
                        getJobMetricsMethod.invoke(jobInfoService, jobMetrics, dagInfo);

        Map<String, Object> tableSinkCount =
                (Map<String, Object>) result.get("TableSinkWriteCount");
        Assertions.assertNotNull(tableSinkCount);

        Assertions.assertTrue(
                tableSinkCount.containsKey("Sink[0].fake.user_table"),
                "Should contain Sink[0].fake.user_table");
        Assertions.assertTrue(
                tableSinkCount.containsKey("Sink[1].fake.user_table"),
                "Should contain Sink[1].fake.user_table");

        Assertions.assertEquals(5L, tableSinkCount.get("Sink[0].fake.user_table"));
        Assertions.assertEquals(5L, tableSinkCount.get("Sink[1].fake.user_table"));

        Assertions.assertFalse(
                tableSinkCount.containsKey("fake.user_table"),
                "Should not contain raw table name key 'fake.user_table'");

        Map<String, Object> tableSinkCommittedCount =
                (Map<String, Object>) result.get("TableSinkCommittedCount");
        Assertions.assertNotNull(tableSinkCommittedCount);
        Assertions.assertEquals(5L, tableSinkCommittedCount.get("Sink[0].fake.user_table"));
        Assertions.assertEquals(5L, tableSinkCommittedCount.get("Sink[1].fake.user_table"));

        Map<String, Object> tableSourceCount =
                (Map<String, Object>) result.get("TableSourceReceivedCount");
        Assertions.assertNotNull(tableSourceCount);

        Assertions.assertTrue(
                tableSourceCount.containsKey("Source[0].fake.user_table"),
                "Should contain Source[0].fake.user_table");
        Assertions.assertEquals(10L, tableSourceCount.get("Source[0].fake.user_table"));
    }

    @Test
    public void testMetricsWithArraySizeMismatch_NoTags_AssignByIndex() throws Exception {
        // 2 sinks configured, but only 1 metric entry provided and no tags to attribute reliably
        String jobMetrics =
                "{"
                        + "\"SinkWriteCount#fake.user_table\": [{\"value\": 100}],"
                        + "\"SinkWriteCount\": [{\"value\": 100}]"
                        + "}";

        JobDAGInfo dagInfo = createDAGInfoWithMultipleSinks();

        Map<String, Object> result =
                (Map<String, Object>)
                        getJobMetricsMethod.invoke(jobInfoService, jobMetrics, dagInfo);

        Map<String, Object> tableSinkCount =
                (Map<String, Object>) result.get("TableSinkWriteCount");
        Assertions.assertNotNull(tableSinkCount);
        Assertions.assertTrue(tableSinkCount.containsKey("Sink[0].fake.user_table"));
        Assertions.assertFalse(tableSinkCount.containsKey("fake.user_table"));
        Assertions.assertFalse(tableSinkCount.containsKey("Sink[1].fake.user_table"));
        Assertions.assertEquals(100L, tableSinkCount.get("Sink[0].fake.user_table"));
    }

    @Test
    public void testMetricsWithArraySizeMismatch_UsesTagsForAttribution() throws Exception {
        // 2 sinks configured, but only Sink[1] reports metrics yet; tags allow correct attribution
        String jobMetrics =
                "{"
                        + "\"SinkWriteCount#fake.user_table\": ["
                        + "{\"value\": 100, \"tags\": {\"taskName\": \"pipeline-1 [Sink[1]-console-MultiTableSink]\"}}"
                        + "],"
                        + "\"SinkWriteCount\": [{\"value\": 100}]"
                        + "}";

        JobDAGInfo dagInfo = createDAGInfoWithMultipleSinks();

        Map<String, Object> result =
                (Map<String, Object>)
                        getJobMetricsMethod.invoke(jobInfoService, jobMetrics, dagInfo);

        Map<String, Object> tableSinkCount =
                (Map<String, Object>) result.get("TableSinkWriteCount");
        Assertions.assertNotNull(tableSinkCount);
        Assertions.assertEquals(1, tableSinkCount.size());
        Assertions.assertTrue(tableSinkCount.containsKey("Sink[1].fake.user_table"));
        Assertions.assertEquals(100L, tableSinkCount.get("Sink[1].fake.user_table"));
    }

    @Test
    public void testMetricsWithArraySizeMismatch_NoTags_AssignAvailableMetricsByIndex()
            throws Exception {
        // 3 sinks configured, but only first 2 metric entries reported
        String jobMetrics =
                "{"
                        + "\"SinkWriteCount#fake.user_table\": [{\"value\": 1}, {\"value\": 2}],"
                        + "\"SinkWriteCount\": [{\"value\": 3}]"
                        + "}";

        JobDAGInfo dagInfo = createDAGInfoWithThreeSinks();

        Map<String, Object> result =
                (Map<String, Object>)
                        getJobMetricsMethod.invoke(jobInfoService, jobMetrics, dagInfo);

        Map<String, Object> tableSinkCount =
                (Map<String, Object>) result.get("TableSinkWriteCount");
        Assertions.assertNotNull(tableSinkCount);
        Assertions.assertEquals(2, tableSinkCount.size());
        Assertions.assertEquals(1L, tableSinkCount.get("Sink[0].fake.user_table"));
        Assertions.assertEquals(2L, tableSinkCount.get("Sink[1].fake.user_table"));
        Assertions.assertFalse(tableSinkCount.containsKey("Sink[2].fake.user_table"));
    }

    @Test
    public void testMetricsWithNullJobDAGInfo_FallbackToTableName() throws Exception {
        String jobMetrics =
                "{"
                        + "\"SinkWriteCount#fake.user_table\": [{\"value\": 100}],"
                        + "\"SinkWriteCount\": [{\"value\": 100}]"
                        + "}";

        Map<String, Object> result =
                (Map<String, Object>) getJobMetricsMethod.invoke(jobInfoService, jobMetrics, null);

        Map<String, Object> tableSinkCount =
                (Map<String, Object>) result.get("TableSinkWriteCount");
        Assertions.assertNotNull(tableSinkCount);
        Assertions.assertTrue(tableSinkCount.containsKey("fake.user_table"));
        Assertions.assertEquals(100L, tableSinkCount.get("fake.user_table"));
    }

    @Test
    public void testMetricsWithMalformedJSON() throws Exception {
        String malformedMetrics = "{\"SinkWriteCount#fake.user_table\": [invalid}";

        Map<String, Object> result =
                (Map<String, Object>)
                        getJobMetricsMethod.invoke(jobInfoService, malformedMetrics, null);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testMultipleSinksOrderIsDeterministicWithoutTags() throws Exception {
        // Ensure identifier list order doesn't depend on vertexInfoMap iteration order
        String jobMetrics =
                "{"
                        + "\"SinkWriteCount#fake.user_table\": [{\"value\": 1}, {\"value\": 2}],"
                        + "\"SinkWriteCount\": [{\"value\": 3}]"
                        + "}";

        JobDAGInfo dagInfo = createDAGInfoWithMultipleSinksInReverseOrder();

        Map<String, Object> result =
                (Map<String, Object>)
                        getJobMetricsMethod.invoke(jobInfoService, jobMetrics, dagInfo);

        Map<String, Object> tableSinkCount =
                (Map<String, Object>) result.get("TableSinkWriteCount");
        Assertions.assertNotNull(tableSinkCount);
        Assertions.assertEquals(1L, tableSinkCount.get("Sink[0].fake.user_table"));
        Assertions.assertEquals(2L, tableSinkCount.get("Sink[1].fake.user_table"));
    }

    private JobDAGInfo createDAGInfoWithMultipleSinks() {
        Map<Long, VertexInfo> vertexInfoMap = new HashMap<>();

        VertexInfo sourceVertex = new VertexInfo();
        sourceVertex.setVertexId(1L);
        sourceVertex.setType(PluginType.SOURCE);
        String sourceName = JobConfigParser.createSourceActionName(0, "FakeSource");
        sourceVertex.setConnectorType("pipeline-1 [" + sourceName + "]");
        sourceVertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
        vertexInfoMap.put(1L, sourceVertex);

        VertexInfo sink0Vertex = new VertexInfo();
        sink0Vertex.setVertexId(2L);
        sink0Vertex.setType(PluginType.SINK);
        String sink0Name = JobConfigParser.createSinkActionName(0, "console", "MultiTableSink");
        sink0Vertex.setConnectorType("pipeline-1 [" + sink0Name + "]");
        sink0Vertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
        vertexInfoMap.put(2L, sink0Vertex);

        VertexInfo sink1Vertex = new VertexInfo();
        sink1Vertex.setVertexId(3L);
        sink1Vertex.setType(PluginType.SINK);
        String sink1Name = JobConfigParser.createSinkActionName(1, "console", "MultiTableSink");
        sink1Vertex.setConnectorType("pipeline-1 [" + sink1Name + "]");
        sink1Vertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
        vertexInfoMap.put(3L, sink1Vertex);

        JobDAGInfo dagInfo = new JobDAGInfo();
        dagInfo.setVertexInfoMap(vertexInfoMap);

        return dagInfo;
    }

    private JobDAGInfo createDAGInfoWithMultipleSinksInReverseOrder() {
        Map<Long, VertexInfo> vertexInfoMap = new LinkedHashMap<>();

        VertexInfo sourceVertex = new VertexInfo();
        sourceVertex.setVertexId(1L);
        sourceVertex.setType(PluginType.SOURCE);
        String sourceName = JobConfigParser.createSourceActionName(0, "FakeSource");
        sourceVertex.setConnectorType("pipeline-1 [" + sourceName + "]");
        sourceVertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
        vertexInfoMap.put(1L, sourceVertex);

        VertexInfo sink1Vertex = new VertexInfo();
        sink1Vertex.setVertexId(3L);
        sink1Vertex.setType(PluginType.SINK);
        String sink1Name = JobConfigParser.createSinkActionName(1, "console", "MultiTableSink");
        sink1Vertex.setConnectorType("pipeline-1 [" + sink1Name + "]");
        sink1Vertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
        vertexInfoMap.put(3L, sink1Vertex);

        VertexInfo sink0Vertex = new VertexInfo();
        sink0Vertex.setVertexId(2L);
        sink0Vertex.setType(PluginType.SINK);
        String sink0Name = JobConfigParser.createSinkActionName(0, "console", "MultiTableSink");
        sink0Vertex.setConnectorType("pipeline-1 [" + sink0Name + "]");
        sink0Vertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
        vertexInfoMap.put(2L, sink0Vertex);

        JobDAGInfo dagInfo = new JobDAGInfo();
        dagInfo.setVertexInfoMap(vertexInfoMap);
        return dagInfo;
    }

    private JobDAGInfo createDAGInfoWithThreeSinks() {
        Map<Long, VertexInfo> vertexInfoMap = new HashMap<>();

        VertexInfo sourceVertex = new VertexInfo();
        sourceVertex.setVertexId(1L);
        sourceVertex.setType(PluginType.SOURCE);
        String sourceName = JobConfigParser.createSourceActionName(0, "FakeSource");
        sourceVertex.setConnectorType("pipeline-1 [" + sourceName + "]");
        sourceVertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
        vertexInfoMap.put(1L, sourceVertex);

        for (int i = 0; i < 3; i++) {
            VertexInfo sinkVertex = new VertexInfo();
            sinkVertex.setVertexId(2L + i);
            sinkVertex.setType(PluginType.SINK);
            String sinkName = JobConfigParser.createSinkActionName(i, "console", "MultiTableSink");
            sinkVertex.setConnectorType("pipeline-1 [" + sinkName + "]");
            sinkVertex.setTablePaths(Arrays.asList(TablePath.of("fake.user_table")));
            vertexInfoMap.put(2L + i, sinkVertex);
        }

        JobDAGInfo dagInfo = new JobDAGInfo();
        dagInfo.setVertexInfoMap(vertexInfoMap);
        return dagInfo;
    }
}
