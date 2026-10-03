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
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies listener ownership on partial construction and best-effort shutdown failures without
 * starting a Hazelcast member or relying on cluster timing.
 */
@ExtendWith(MockitoExtension.class)
class JobHistoryServiceRegistrationTest {

    // Node dependency; these lifecycle tests do not start a Hazelcast member.
    @Mock private NodeEngine nodeEngine;

    // Captures unexpected cleanup failures without changing the original exception.
    @Mock private ILogger logger;

    // Distributed running-job state, unused by registration cleanup.
    @Mock private IMap<Object, Object> runningState;

    // State listener owner and history-read source.
    @Mock private IMap<Long, JobHistoryService.JobState> finishedState;

    // Metrics listener owner.
    @Mock private IMap<Long, JobMetrics> finishedMetrics;

    // DAG listener owner, registered last.
    @Mock private IMap<Long, JobDAGInfo> finishedDag;

    /**
     * Every failure position releases exactly the registrations acquired before the failure.
     *
     * @param failingIndex zero-based index of the registration that throws
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void releasesRegistrationsWhenConstructionFails(int failingIndex) {
        List<IMap<Long, ?>> maps = Arrays.asList(finishedState, finishedMetrics, finishedDag);
        UUID[] ids = {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        RuntimeException failure = new IllegalStateException("registration failed");
        for (int i = 0; i < failingIndex; i++) {
            when(maps.get(i).addEntryListener(any(), eq(true))).thenReturn(ids[i]);
        }
        when(maps.get(failingIndex).addEntryListener(any(), eq(true))).thenThrow(failure);

        assertSame(failure, assertThrows(RuntimeException.class, this::newService));

        for (int i = 0; i < maps.size(); i++) {
            if (i < failingIndex) {
                verify(maps.get(i)).removeEntryListener(ids[i]);
            } else {
                verify(maps.get(i), never()).removeEntryListener(any());
            }
            if (i > failingIndex) {
                verify(maps.get(i), never()).addEntryListener(any(), eq(true));
            }
        }
    }

    /**
     * Verifies that rollback continues with the next map when the first removal throws and that
     * callers still receive the original registration failure.
     */
    @Test
    void preservesConstructionFailureWhenRollbackAlsoFails() {
        UUID stateId = UUID.randomUUID();
        UUID metricsId = UUID.randomUUID();
        RuntimeException registrationFailure = new IllegalStateException("DAG registration failed");
        RuntimeException removalFailure = new IllegalStateException("state removal failed");
        when(finishedState.addEntryListener(any(), eq(true))).thenReturn(stateId);
        when(finishedMetrics.addEntryListener(any(), eq(true))).thenReturn(metricsId);
        when(finishedDag.addEntryListener(any(), eq(true))).thenThrow(registrationFailure);
        when(finishedState.removeEntryListener(stateId)).thenThrow(removalFailure);

        assertSame(registrationFailure, assertThrows(RuntimeException.class, this::newService));

        verify(finishedMetrics).removeEntryListener(metricsId);
        verify(logger).warning(anyString(), eq(removalFailure));
    }

    /**
     * Both shutdown-time and unexpected removal failures leave other listeners cleanable.
     *
     * @param nodeStopped whether removal failed because the Hazelcast instance stopped
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void continuesCleanupAfterRemovalFailure(boolean nodeStopped) {
        UUID stateId = UUID.randomUUID();
        UUID metricsId = UUID.randomUUID();
        UUID dagId = UUID.randomUUID();
        when(finishedState.addEntryListener(any(), eq(true))).thenReturn(stateId);
        when(finishedMetrics.addEntryListener(any(), eq(true))).thenReturn(metricsId);
        when(finishedDag.addEntryListener(any(), eq(true))).thenReturn(dagId);
        RuntimeException failure =
                nodeStopped
                        ? new HazelcastInstanceNotActiveException()
                        : new IllegalStateException("removal failed");
        when(finishedState.removeEntryListener(stateId)).thenThrow(failure);

        JobHistoryService service = newService();
        assertDoesNotThrow(service::close);

        verify(finishedMetrics).removeEntryListener(metricsId);
        verify(finishedDag).removeEntryListener(dagId);
        if (nodeStopped) {
            verify(logger).fine(anyString(), eq(failure));
            verify(logger, never()).warning(anyString(), any(Throwable.class));
        } else {
            verify(logger).warning(anyString(), eq(failure));
        }
    }

    /**
     * Verifies that repeated listener cleanup does not invalidate history reads already in flight
     * when the coordinator steps down.
     */
    @Test
    void keepsHistoryReadsAvailableAfterClose() {
        when(finishedState.addEntryListener(any(), eq(true))).thenReturn(UUID.randomUUID());
        when(finishedMetrics.addEntryListener(any(), eq(true))).thenReturn(UUID.randomUUID());
        when(finishedDag.addEntryListener(any(), eq(true))).thenReturn(UUID.randomUUID());
        JobMetrics metrics = JobMetrics.empty();
        when(finishedMetrics.getOrDefault(eq(1L), any(JobMetrics.class))).thenReturn(metrics);

        try (JobHistoryService service = newService()) {
            service.close();
            assertSame(metrics, service.getJobMetrics(1L));
        }
    }

    /**
     * Constructs the real service with mocked maps and no running or pending jobs.
     *
     * @return a service whose registrations belong only to this test invocation
     */
    private JobHistoryService newService() {
        return new JobHistoryService(
                nodeEngine,
                runningState,
                logger,
                Collections.emptyMap(),
                Collections.emptyMap(),
                finishedState,
                finishedMetrics,
                finishedDag,
                1);
    }
}
