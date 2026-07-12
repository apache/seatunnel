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

package org.apache.seatunnel.engine.server.telemetry.metrics;

import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.JobCounter;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ReportMetricsOperationStats;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.ClusterMetricExports;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.JobMetricExports;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.JobThreadPoolStatusExports;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.ReportMetricsOperationExports;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.version.Version;
import io.prometheus.client.Collector;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

public class TelemetryCollectorCoordinatorGuardTest {

    private Node mockNode;
    private SeaTunnelServer mockServer;
    private CoordinatorService mockCoordinatorService;
    private ClusterServiceImpl mockClusterService;
    private ILogger mockLogger;

    @BeforeEach
    void setUp() throws UnknownHostException, NoSuchFieldException, IllegalAccessException {
        mockNode = Mockito.mock(Node.class);
        mockServer = Mockito.mock(SeaTunnelServer.class);
        mockCoordinatorService = Mockito.mock(CoordinatorService.class);
        mockClusterService = Mockito.mock(ClusterServiceImpl.class);
        mockLogger = Mockito.mock(ILogger.class);

        NodeEngineImpl mockNodeEngine = Mockito.mock(NodeEngineImpl.class);
        MemberImpl mockMember = Mockito.mock(MemberImpl.class);
        Config mockConfig = Mockito.mock(Config.class);

        Mockito.when(mockNode.getNodeEngine()).thenReturn(mockNodeEngine);
        Mockito.when(mockNodeEngine.getService(SeaTunnelServer.SERVICE_NAME))
                .thenReturn(mockServer);
        Mockito.when(mockNodeEngine.getLocalMember()).thenReturn(mockMember);
        Mockito.when(mockNode.getClusterService()).thenReturn(mockClusterService);
        Mockito.when(mockNode.getLogger(Mockito.any(Class.class))).thenReturn(mockLogger);
        Mockito.when(mockNode.getConfig()).thenReturn(mockConfig);
        Mockito.when(mockConfig.getClusterName()).thenReturn("test-cluster");

        // AbstractCollector.getLocalMember() reads Node.nodeEngine as a direct public field,
        // not via a getter, so we inject it via reflection.
        Field nodeEngineField = Node.class.getDeclaredField("nodeEngine");
        nodeEngineField.setAccessible(true);
        nodeEngineField.set(mockNode, mockNodeEngine);

        InetAddress inetAddress = InetAddress.getByName("127.0.0.1");
        Mockito.when(mockMember.getInetAddress()).thenReturn(inetAddress);
        Mockito.when(mockMember.getPort()).thenReturn(5801);

        Mockito.when(mockServer.getCoordinatorService()).thenReturn(mockCoordinatorService);

        // Default stubs for ClusterService — always needed because clusterTime() and nodeCount()
        // run unconditionally on every collect() call.
        Version clusterVersion =
                Version.of(
                        Versions.CURRENT_CLUSTER_VERSION.getMajor(),
                        Versions.CURRENT_CLUSTER_VERSION.getMinor());
        Mockito.when(mockClusterService.getClusterVersion()).thenReturn(clusterVersion);
        Mockito.when(mockClusterService.getMemberImpls()).thenReturn(Collections.emptyList());
        Mockito.when(mockClusterService.getClusterTime()).thenReturn(System.currentTimeMillis());
    }

    @Test
    void testJobMetricExportsReturnsEmptyWhenCoordinatorNotReady() {
        Mockito.when(mockNode.isMaster()).thenReturn(true);
        Mockito.when(mockServer.isCoordinatorActive()).thenReturn(false);

        JobMetricExports exports = new JobMetricExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertTrue(
                result.isEmpty(),
                "collect() must return empty when coordinator is not ready"
                        + " to avoid blocking Hazelcast operation threads");
        Mockito.verify(mockServer, Mockito.never()).getCoordinatorService();
    }

    @Test
    void testJobMetricExportsReturnsMetricsWhenCoordinatorReady() {
        Mockito.when(mockNode.isMaster()).thenReturn(true);
        Mockito.when(mockServer.isCoordinatorActive()).thenReturn(true);
        Mockito.when(mockCoordinatorService.getJobCountMetrics())
                .thenReturn(new JobCounter(0, 0, 0, 1, 0, 0, 0, 0, 0));

        JobMetricExports exports = new JobMetricExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertFalse(
                result.isEmpty(), "collect() must return metrics when coordinator is ready");
        Mockito.verify(mockCoordinatorService).getJobCountMetrics();
    }

    @Test
    void testJobMetricExportsReturnsEmptyWhenNotMaster() {
        Mockito.when(mockNode.isMaster()).thenReturn(false);

        JobMetricExports exports = new JobMetricExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertTrue(result.isEmpty(), "collect() must return empty on non-master node");
        Mockito.verify(mockServer, Mockito.never()).isCoordinatorActive();
    }

    @Test
    void testJobThreadPoolStatusExportsReturnsEmptyWhenCoordinatorNotReady() {
        Mockito.when(mockNode.isMaster()).thenReturn(true);
        Mockito.when(mockServer.isCoordinatorActive()).thenReturn(false);

        JobThreadPoolStatusExports exports = new JobThreadPoolStatusExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertTrue(
                result.isEmpty(),
                "collect() must return empty when coordinator is not ready"
                        + " to avoid blocking Hazelcast operation threads");
        Mockito.verify(mockServer, Mockito.never()).getThreadPoolStatusMetrics();
    }

    @Test
    void testJobThreadPoolStatusExportsReturnsMetricsWhenCoordinatorReady() {
        Mockito.when(mockNode.isMaster()).thenReturn(true);
        Mockito.when(mockServer.isCoordinatorActive()).thenReturn(true);
        ThreadPoolStatus status = new ThreadPoolStatus(1, 2, 10, 3, 100L, 110L, 0L, 0L);
        Mockito.when(mockServer.getThreadPoolStatusMetrics()).thenReturn(status);

        JobThreadPoolStatusExports exports = new JobThreadPoolStatusExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertFalse(
                result.isEmpty(), "collect() must return metrics when coordinator is ready");
        Mockito.verify(mockServer).getThreadPoolStatusMetrics();
    }

    @Test
    void testJobThreadPoolStatusExportsReturnsEmptyWhenNotMaster() {
        Mockito.when(mockNode.isMaster()).thenReturn(false);

        JobThreadPoolStatusExports exports = new JobThreadPoolStatusExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertTrue(result.isEmpty(), "collect() must return empty on non-master node");
        Mockito.verify(mockServer, Mockito.never()).isCoordinatorActive();
    }

    @Test
    void testReportMetricsOperationExportsReturnsEmptyWhenTaskExecutionServiceMissing() {
        Mockito.when(mockServer.getTaskExecutionService()).thenReturn(null);

        ReportMetricsOperationExports exports = new ReportMetricsOperationExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertTrue(
                result.isEmpty(),
                "collect() must return empty when task execution service is unavailable");
    }

    @Test
    void testReportMetricsOperationExportsReturnsMetricsWhenTaskExecutionServiceAvailable() {
        TaskExecutionService taskExecutionService = Mockito.mock(TaskExecutionService.class);
        Mockito.when(mockServer.getTaskExecutionService()).thenReturn(taskExecutionService);
        Mockito.when(taskExecutionService.getReportMetricsOperationStats())
                .thenReturn(new ReportMetricsOperationStats(3L, 1L, 1L, 9L, 15L, 27L));

        ReportMetricsOperationExports exports = new ReportMetricsOperationExports(mockNode);
        List<Collector.MetricFamilySamples> result = exports.collect();

        Assertions.assertFalse(
                result.isEmpty(),
                "collect() must return metrics when task execution service exists");
        Collector.MetricFamilySamples totalMetric =
                result.stream()
                        .filter(s -> "report_metrics_operation".equals(s.name))
                        .findFirst()
                        .orElse(null);
        Assertions.assertNotNull(totalMetric);
        Assertions.assertEquals(3, totalMetric.samples.size());
        assertMetricSample(totalMetric, "report_metrics_operation_total", "success", 3D);
        assertMetricSample(totalMetric, "report_metrics_operation_total", "failure", 1D);
        assertMetricSample(totalMetric, "report_metrics_operation_total", "interrupted", 1D);

        Collector.MetricFamilySamples payloadMetric =
                result.stream()
                        .filter(
                                s ->
                                        "report_metrics_operation_last_payload_task_count"
                                                .equals(s.name))
                        .findFirst()
                        .orElse(null);
        Assertions.assertNotNull(payloadMetric);
        assertSingleMetricSample(payloadMetric, 9D);

        Collector.MetricFamilySamples lastLatencyMetric =
                result.stream()
                        .filter(
                                s ->
                                        "report_metrics_operation_last_invocation_latency_ms"
                                                .equals(s.name))
                        .findFirst()
                        .orElse(null);
        Assertions.assertNotNull(lastLatencyMetric);
        assertSingleMetricSample(lastLatencyMetric, 15D);

        Collector.MetricFamilySamples maxLatencyMetric =
                result.stream()
                        .filter(
                                s ->
                                        "report_metrics_operation_max_invocation_latency_ms"
                                                .equals(s.name))
                        .findFirst()
                        .orElse(null);
        Assertions.assertNotNull(maxLatencyMetric);
        assertSingleMetricSample(maxLatencyMetric, 27D);
    }

    // -------------------------------------------------------------------------
    // ClusterMetricExports
    // -------------------------------------------------------------------------

    @Test
    void testClusterMetricExportsSkipsClusterInfoWhenMasterAddressNull() {
        Mockito.when(mockClusterService.getMasterAddress()).thenReturn(null);

        List<Collector.MetricFamilySamples> result = new ClusterMetricExports(mockNode).collect();

        boolean hasClusterInfo = result.stream().anyMatch(s -> "cluster_info".equals(s.name));
        Assertions.assertFalse(
                hasClusterInfo, "cluster_info must be skipped when master address is null");
    }

    @Test
    void testClusterMetricExportsIncludesClusterInfoWhenMasterAddressAvailable()
            throws UnknownHostException {
        Mockito.when(mockClusterService.getMasterAddress())
                .thenReturn(new Address("127.0.0.1", 5801));

        List<Collector.MetricFamilySamples> result = new ClusterMetricExports(mockNode).collect();

        Collector.MetricFamilySamples clusterInfoMetric =
                result.stream().filter(s -> "cluster_info".equals(s.name)).findFirst().orElse(null);
        boolean hasClusterInfo = clusterInfoMetric != null;
        Assertions.assertTrue(
                hasClusterInfo, "cluster_info must be present when master address is available");
        Assertions.assertNotNull(clusterInfoMetric);
        Assertions.assertFalse(clusterInfoMetric.samples.isEmpty());

        Collector.MetricFamilySamples.Sample sample = clusterInfoMetric.samples.get(0);
        int masterLabelIndex = sample.labelNames.indexOf("master");
        Assertions.assertTrue(masterLabelIndex >= 0, "cluster_info must contain 'master' label");
        Assertions.assertEquals("127.0.0.1:5801", sample.labelValues.get(masterLabelIndex));
    }

    @Test
    void testClusterMetricExportsSkipsClusterInfoAndLogsWarningWhenMasterAddressUnresolvable()
            throws UnknownHostException {
        Address mockAddress = Mockito.mock(Address.class);
        Mockito.when(mockAddress.getInetAddress()).thenThrow(new UnknownHostException("mock"));
        Mockito.when(mockAddress.getPort()).thenReturn(5801);
        Mockito.when(mockClusterService.getMasterAddress()).thenReturn(mockAddress);

        List<Collector.MetricFamilySamples> result = new ClusterMetricExports(mockNode).collect();

        boolean hasClusterInfo = result.stream().anyMatch(s -> "cluster_info".equals(s.name));
        Assertions.assertFalse(
                hasClusterInfo,
                "cluster_info must be skipped when master address can't be resolved");
        Mockito.verify(mockLogger)
                .warning(
                        Mockito.eq("Skip cluster_info metric: unable to resolve master address"),
                        Mockito.any(UnknownHostException.class));
    }

    private void assertMetricSample(
            Collector.MetricFamilySamples metricFamilySamples,
            String expectedSampleName,
            String result,
            double expectedValue) {
        Collector.MetricFamilySamples.Sample sample =
                metricFamilySamples.samples.stream()
                        .filter(
                                s -> {
                                    if (!expectedSampleName.equals(s.name)) {
                                        return false;
                                    }
                                    int resultLabelIndex = s.labelNames.indexOf("result");
                                    return resultLabelIndex >= 0
                                            && result.equals(s.labelValues.get(resultLabelIndex));
                                })
                        .findFirst()
                        .orElse(null);
        Assertions.assertNotNull(sample);
        assertAddressLabel(sample);
        Assertions.assertEquals(expectedValue, sample.value);
    }

    private void assertSingleMetricSample(
            Collector.MetricFamilySamples metricFamilySamples, double expectedValue) {
        Assertions.assertEquals(1, metricFamilySamples.samples.size());
        Collector.MetricFamilySamples.Sample sample = metricFamilySamples.samples.get(0);
        assertAddressLabel(sample);
        Assertions.assertEquals(expectedValue, sample.value);
    }

    private void assertAddressLabel(Collector.MetricFamilySamples.Sample sample) {
        int addressLabelIndex = sample.labelNames.indexOf("address");
        Assertions.assertTrue(addressLabelIndex >= 0, "metric sample must contain 'address' label");
        Assertions.assertEquals("127.0.0.1:5801", sample.labelValues.get(addressLabelIndex));
    }
}
