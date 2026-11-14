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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
                        + "  {\"value\": 600, \"name\": \"source2\"}"
                        + "],"
                        + "\"SinkWriteCount\": ["
                        + "  {\"value\": 100, \"name\": \"sink1\"},"
                        + "  {\"value\": 400, \"name\": \"sink2\"},"
                        + "  {\"value\": 300, \"name\": \"sink3\"},"
                        + "  {\"value\": 300, \"name\": \"sink4\"}"
                        + "],"
                        + "\"SinkCommittedCount\": ["
                        + "  {\"value\": 100, \"name\": \"sink1\"},"
                        + "  {\"value\": 380, \"name\": \"sink2\"},"
                        + "  {\"value\": 290, \"name\": \"sink3\"},"
                        + "  {\"value\": 290, \"name\": \"sink4\"}"
                        + "]"
                        + "}";

        when(hazelcastClient.requestOnMasterAndDecodeResponse(any(), any()))
                .thenReturn(metricsJson);

        JobMetricsRunner.JobMetricsSummary summary = jobClient.getJobMetricsSummary(123456L);

        Assertions.assertNotNull(summary);
        Assertions.assertEquals(1100L, summary.getSourceReadCount());
        Assertions.assertEquals(1100L, summary.getSinkWriteCount());
        Assertions.assertEquals(1060L, summary.getSinkCommittedCount());
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

    @Test
    public void testJobMetricsRunnerFirstRunDoesNotDivideByZero() {
        SeaTunnelClient seaTunnelClient = mock(SeaTunnelClient.class);
        JobMetricsRunner jobMetricsRunner = new JobMetricsRunner(seaTunnelClient, 1L);
        when(seaTunnelClient.getJobMetricsSummary(1L))
                .thenReturn(new JobMetricsRunner.JobMetricsSummary(100L, 90L, 80L));

        // The first run starts immediately (initialDelay=0), so the elapsed time is ~0 ms.
        // Overwrite the private monotonic base right before the run so the zero-elapsed path is
        // exercised deterministically instead of depending on how fast the test executes.
        ReflectionUtils.setField(jobMetricsRunner, "lastRunTimeNanos", System.nanoTime());
        // Previously the 0 ms elapsed time divided by zero and the run was swallowed by the
        // catch-all handler, leaving the counters untouched.
        Assertions.assertDoesNotThrow(jobMetricsRunner::run);
        Assertions.assertDoesNotThrow(jobMetricsRunner::run);

        Assertions.assertEquals(
                100L,
                ReflectionUtils.getField(jobMetricsRunner, "lastReadCount")
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "field lastReadCount not found on JobMetricsRunner")));
        Assertions.assertEquals(
                90L,
                ReflectionUtils.getField(jobMetricsRunner, "lastWriteCount")
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "field lastWriteCount not found on JobMetricsRunner")));
        Assertions.assertEquals(
                80L,
                ReflectionUtils.getField(jobMetricsRunner, "lastCommittedCount")
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "field lastCommittedCount not found on JobMetricsRunner")));
    }

    @Test
    public void testJobMetricsRunnerWhenMetricsNotReadyDoesNotThrow() {
        SeaTunnelClient seaTunnelClient = mock(SeaTunnelClient.class);
        JobMetricsRunner jobMetricsRunner = new JobMetricsRunner(seaTunnelClient, 1L);
        when(seaTunnelClient.getJobMetricsSummary(1L))
                .thenThrow(new RuntimeException("job metrics not ready yet"));

        // Metrics may not be available right after job submission; the failure must be swallowed
        // without touching the baseline counters the next run's deltas are computed from.
        Assertions.assertDoesNotThrow(jobMetricsRunner::run);
        Assertions.assertEquals(
                0L,
                ReflectionUtils.getField(jobMetricsRunner, "lastReadCount")
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "field lastReadCount not found on JobMetricsRunner")));
        Assertions.assertEquals(
                0L,
                ReflectionUtils.getField(jobMetricsRunner, "lastWriteCount")
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "field lastWriteCount not found on JobMetricsRunner")));
        Assertions.assertEquals(
                0L,
                ReflectionUtils.getField(jobMetricsRunner, "lastCommittedCount")
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "field lastCommittedCount not found on JobMetricsRunner")));
        verify(seaTunnelClient).getJobMetricsSummary(1L);
    }
}
