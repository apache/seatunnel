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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.Measurement;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies finished-metrics persistence issues a single durable IMap write for a newly finished
 * job.
 */
class JobHistoryServiceFinishedMetricsTest {

    private static final int FINISHED_JOB_EXPIRE_MINUTES = 1440;

    private IMap<Long, JobMetrics> finishedJobMetricsImap;
    private JobHistoryService jobHistoryService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        IMap<Object, Object> runningJobStateIMap = mock(IMap.class);
        IMap<Long, JobHistoryService.JobState> finishedJobStateImap = mock(IMap.class);
        finishedJobMetricsImap = mock(IMap.class);
        IMap<Long, JobDAGInfo> finishedJobVertexInfoImap = mock(IMap.class);

        when(finishedJobStateImap.addEntryListener(any(MapListener.class), anyBoolean()))
                .thenReturn(UUID.randomUUID());
        when(finishedJobMetricsImap.addEntryListener(any(MapListener.class), anyBoolean()))
                .thenReturn(UUID.randomUUID());
        when(finishedJobVertexInfoImap.addEntryListener(any(MapListener.class), anyBoolean()))
                .thenReturn(UUID.randomUUID());

        jobHistoryService =
                new JobHistoryService(
                        nodeEngine,
                        runningJobStateIMap,
                        mock(ILogger.class),
                        new HashMap<>(),
                        new HashMap<>(),
                        finishedJobStateImap,
                        finishedJobMetricsImap,
                        finishedJobVertexInfoImap,
                        FINISHED_JOB_EXPIRE_MINUTES);
    }

    @Test
    void storeFinishedPipelineMetricsWritesOnceForNewJob() {
        long jobId = 42L;
        JobMetrics metrics = metrics("SourceReceivedCount", 7.0D);
        when(finishedJobMetricsImap.get(jobId)).thenReturn(null);

        jobHistoryService.storeFinishedPipelineMetrics(jobId, metrics);

        verify(finishedJobMetricsImap, never()).computeIfAbsent(eq(jobId), any());
        ArgumentCaptor<JobMetrics> metricsCaptor = ArgumentCaptor.forClass(JobMetrics.class);
        verify(finishedJobMetricsImap, times(1))
                .put(
                        eq(jobId),
                        metricsCaptor.capture(),
                        eq((long) FINISHED_JOB_EXPIRE_MINUTES),
                        eq(TimeUnit.MINUTES));
        assertEquals(7.0D, metricsCaptor.getValue().get("SourceReceivedCount").get(0).value());
    }

    @Test
    void storeFinishedPipelineMetricsMergesExistingFinishedMetrics() {
        long jobId = 43L;
        JobMetrics existing = metrics("SourceReceivedCount", 1.0D);
        JobMetrics incoming = metrics("SinkWriteCount", 2.0D);
        when(finishedJobMetricsImap.get(jobId)).thenReturn(existing);

        jobHistoryService.storeFinishedPipelineMetrics(jobId, incoming);

        ArgumentCaptor<JobMetrics> metricsCaptor = ArgumentCaptor.forClass(JobMetrics.class);
        verify(finishedJobMetricsImap, times(1))
                .put(
                        eq(jobId),
                        metricsCaptor.capture(),
                        eq((long) FINISHED_JOB_EXPIRE_MINUTES),
                        eq(TimeUnit.MINUTES));
        JobMetrics stored = metricsCaptor.getValue();
        assertFalse(stored.get("SourceReceivedCount").isEmpty());
        assertFalse(stored.get("SinkWriteCount").isEmpty());
        assertEquals(1.0D, stored.get("SourceReceivedCount").get(0).value());
        assertEquals(2.0D, stored.get("SinkWriteCount").get(0).value());
        verify(finishedJobMetricsImap, never()).computeIfAbsent(eq(jobId), any());
    }

    private static JobMetrics metrics(String metricName, double value) {
        Measurement measurement =
                Measurement.of(
                        metricName, value, System.currentTimeMillis(), Collections.emptyMap());
        Map<String, List<Measurement>> map = new HashMap<>();
        map.put(metricName, Collections.singletonList(measurement));
        return JobMetrics.of(map);
    }
}
