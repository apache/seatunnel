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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.collection.IQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies that the monitoring sidecar cannot prevent authoritative history persistence. */
class JobHistoryServiceMonitoringFailureTest {

    @Test
    void testPendingMonitoringFailureDoesNotFailFinishedStateStore() throws Exception {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        ExecutionService executionService = mock(ExecutionService.class);
        ILogger logger = mock(ILogger.class);
        IMap<Object, Object> runningStateMap = mock(IMap.class);
        IMap<Long, JobState> finishedStateMap = mock(IMap.class);
        IMap<Long, JobMonitoringRecord> monitoringMap = mock(IMap.class);
        IMap<String, Long> monitoringMetadataMap = mock(IMap.class);
        IQueue<JobMonitoringRecord> pendingMonitoringQueue = mock(IQueue.class);
        IMap<Long, JobMonitoringRecord> overflowMonitoringMap = mock(IMap.class);
        IMap<Long, JobMetrics> metricsMap = mock(IMap.class);
        IMap<Long, JobDAGInfo> dagMap = mock(IMap.class);

        when(nodeEngine.getExecutionService()).thenReturn(executionService);
        when(monitoringMetadataMap.tryLock(
                        eq(Constant.FINISHED_JOB_MONITORING_OVERFLOW_LOCK_KEY),
                        eq(100L),
                        eq(TimeUnit.MILLISECONDS)))
                .thenReturn(true);
        when(pendingMonitoringQueue.offer(
                        any(JobMonitoringRecord.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new IllegalStateException("injected monitoring failure"));

        JobHistoryService service =
                new JobHistoryService(
                        nodeEngine,
                        runningStateMap,
                        logger,
                        new HashMap<Long, PendingJobInfo>(),
                        new HashMap<Long, JobMaster>(),
                        finishedStateMap,
                        monitoringMap,
                        monitoringMetadataMap,
                        pendingMonitoringQueue,
                        overflowMonitoringMap,
                        metricsMap,
                        dagMap,
                        60);
        JobState state =
                new JobState(
                        1L,
                        "failed-job",
                        JobStatus.FAILED,
                        1000L,
                        null,
                        2000L,
                        Collections.emptyMap(),
                        "expected failure");

        Assertions.assertDoesNotThrow(() -> service.storeFinishedJobState(state));
        verify(finishedStateMap).put(eq(1L), eq(state), eq(60L), eq(TimeUnit.MINUTES));
        verify(pendingMonitoringQueue)
                .offer(any(JobMonitoringRecord.class), eq(100L), eq(TimeUnit.MILLISECONDS));
        verify(overflowMonitoringMap).put(eq(1L), any(JobMonitoringRecord.class));
    }

    @Test
    void testDroppedCounterFlushesPendingDeltaAfterMetadataRecovery() throws Exception {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        ExecutionService executionService = mock(ExecutionService.class);
        IMap<String, Long> monitoringMetadataMap = mock(IMap.class);
        AtomicLong persistentTotal = new AtomicLong();

        when(nodeEngine.getExecutionService()).thenReturn(executionService);
        when(monitoringMetadataMap.tryLock(
                        eq(Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY),
                        eq(100L),
                        eq(TimeUnit.MILLISECONDS)))
                .thenReturn(false, true);
        when(monitoringMetadataMap.getOrDefault(
                        eq(Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY), eq(0L)))
                .thenAnswer(invocation -> persistentTotal.get());
        when(monitoringMetadataMap.put(
                        eq(Constant.FINISHED_JOB_MONITORING_DROPPED_RECORDS_KEY), anyLong()))
                .thenAnswer(
                        invocation -> {
                            persistentTotal.set(invocation.getArgument(1));
                            return null;
                        });

        JobHistoryService service =
                new JobHistoryService(
                        nodeEngine,
                        mock(IMap.class),
                        mock(ILogger.class),
                        new HashMap<Long, PendingJobInfo>(),
                        new HashMap<Long, JobMaster>(),
                        mock(IMap.class),
                        mock(IMap.class),
                        monitoringMetadataMap,
                        mock(IQueue.class),
                        mock(IMap.class),
                        mock(IMap.class),
                        mock(IMap.class),
                        60);

        Assertions.assertEquals(1L, service.incrementDroppedJobMonitoringRecords());
        Assertions.assertEquals(1L, service.getDroppedJobMonitoringRecords());
        Assertions.assertEquals(2L, service.incrementDroppedJobMonitoringRecords());
        Assertions.assertEquals(2L, persistentTotal.get());
        Assertions.assertEquals(2L, service.getDroppedJobMonitoringRecords());
    }
}
