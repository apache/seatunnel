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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobClientTest {

    private JobClient jobClient;
    private SeaTunnelHazelcastClient hazelcastClient;

    @BeforeEach
    public void setUp() {
        hazelcastClient = mock(SeaTunnelHazelcastClient.class);
        jobClient = new JobClient(hazelcastClient);
    }

    @Test
    public void testNormalCaseWithCommittedMetrics() {
        String metricsJson =
                "{"
                        + "\"SourceReceivedCount\": [{\"value\": 1000, \"name\": \"source1\"}],"
                        + "\"SinkWriteCount\": [{\"value\": 950, \"name\": \"sink1\"}],"
                        + "\"SinkCommittedCount\": [{\"value\": 900, \"name\": \"sink1\"}]"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(1000L, summary.getSourceReadCount());
        Assertions.assertEquals(950L, summary.getSinkWriteCount());
        Assertions.assertEquals(900L, summary.getSinkCommittedCount());
    }

    @Test
    public void testWithoutCommittedMetrics() {
        String metricsJson =
                "{"
                        + "\"SourceReceivedCount\": [{\"value\": 1000, \"name\": \"source1\"}],"
                        + "\"SinkWriteCount\": [{\"value\": 950, \"name\": \"sink1\"}]"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(1000L, summary.getSourceReadCount());
        Assertions.assertEquals(950L, summary.getSinkWriteCount());
        Assertions.assertEquals(0L, summary.getSinkCommittedCount());
    }

    @Test
    public void testEmptyMetrics() {
        String metricsJson = "{}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(0L, summary.getSourceReadCount());
        Assertions.assertEquals(0L, summary.getSinkWriteCount());
        Assertions.assertEquals(0L, summary.getSinkCommittedCount());
    }

    @Test
    public void testEmptyArrays() {
        String metricsJson =
                "{"
                        + "\"SourceReceivedCount\": [],"
                        + "\"SinkWriteCount\": [],"
                        + "\"SinkCommittedCount\": []"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(0L, summary.getSourceReadCount());
        Assertions.assertEquals(0L, summary.getSinkWriteCount());
        Assertions.assertEquals(0L, summary.getSinkCommittedCount());
    }

    @Test
    public void testMultipleSinks() {
        String metricsJson =
                "{"
                        + "\"SourceReceivedCount\": ["
                        + "  {\"value\": 500, \"name\": \"source1\"},"
                        + "  {\"value\": 400, \"name\": \"source2\"},"
                        + "  {\"value\": 300, \"name\": \"source3\"}"
                        + "],"
                        + "\"SinkWriteCount\": ["
                        + "  {\"value\": 500, \"name\": \"sink1\"},"
                        + "  {\"value\": 400, \"name\": \"sink2\"},"
                        + "  {\"value\": 300, \"name\": \"sink3\"}"
                        + "],"
                        + "\"SinkCommittedCount\": ["
                        + "  {\"value\": 450, \"name\": \"sink1\"},"
                        + "  {\"value\": 380, \"name\": \"sink2\"},"
                        + "  {\"value\": 290, \"name\": \"sink3\"}"
                        + "]"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(1200L, summary.getSourceReadCount());
        Assertions.assertEquals(1200L, summary.getSinkWriteCount());
        Assertions.assertEquals(1120L, summary.getSinkCommittedCount());
    }

    @Test
    public void testCommittedLessThanWrite() {
        String metricsJson =
                "{"
                        + "\"SourceReceivedCount\": [{\"value\": 1000, \"name\": \"source1\"}],"
                        + "\"SinkWriteCount\": [{\"value\": 1000, \"name\": \"sink1\"}],"
                        + "\"SinkCommittedCount\": [{\"value\": 800, \"name\": \"sink1\"}]"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(1000L, summary.getSourceReadCount());
        Assertions.assertEquals(1000L, summary.getSinkWriteCount());
        Assertions.assertEquals(800L, summary.getSinkCommittedCount());
    }

    @Test
    public void testCommittedEqualsWrite() {
        String metricsJson =
                "{"
                        + "\"SourceReceivedCount\": [{\"value\": 1000, \"name\": \"source1\"}],"
                        + "\"SinkWriteCount\": [{\"value\": 1000, \"name\": \"sink1\"}],"
                        + "\"SinkCommittedCount\": [{\"value\": 1000, \"name\": \"sink1\"}]"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(1000L, summary.getSourceReadCount());
        Assertions.assertEquals(1000L, summary.getSinkWriteCount());
        Assertions.assertEquals(1000L, summary.getSinkCommittedCount());
    }

    @Test
    public void testInvalidJson() {
        String metricsJson = "invalid json {{}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(0L, summary.getSourceReadCount());
        Assertions.assertEquals(0L, summary.getSinkWriteCount());
        Assertions.assertEquals(0L, summary.getSinkCommittedCount());
    }

    @Test
    public void testNullMetrics() {
        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any())).thenReturn("null");

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(0L, summary.getSourceReadCount());
        Assertions.assertEquals(0L, summary.getSinkWriteCount());
        Assertions.assertEquals(0L, summary.getSinkCommittedCount());
    }

    @Test
    public void testZeroValues() {
        String metricsJson =
                "{"
                        + "\"SourceReceivedCount\": [{\"value\": 0, \"name\": \"source1\"}],"
                        + "\"SinkWriteCount\": [{\"value\": 0, \"name\": \"sink1\"}],"
                        + "\"SinkCommittedCount\": [{\"value\": 0, \"name\": \"sink1\"}]"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(0L, summary.getSourceReadCount());
        Assertions.assertEquals(0L, summary.getSinkWriteCount());
        Assertions.assertEquals(0L, summary.getSinkCommittedCount());
    }
}
