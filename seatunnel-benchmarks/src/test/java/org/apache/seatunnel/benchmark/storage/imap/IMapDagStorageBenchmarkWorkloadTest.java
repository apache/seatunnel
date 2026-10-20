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

package org.apache.seatunnel.benchmark.storage.imap;

import org.apache.seatunnel.benchmark.dag.JobDagFixtureFactory;
import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.options.TimeValue;

import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IMapDagStorageBenchmarkWorkloadTest {

    private final Map<Long, JobDAGInfo> cached = new HashMap<>();
    private final Map<Long, JobDAGInfo> persisted = new HashMap<>();
    private final List<Long> writtenKeys = new ArrayList<>();
    private IMap<Long, JobDAGInfo> map;
    private IMapDagStorageBenchmarkWorkload workload;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        map = mock(IMap.class);
        SeaTunnelStorageEnvironmentContext environment =
                mock(SeaTunnelStorageEnvironmentContext.class, RETURNS_DEEP_STUBS);
        when(environment
                        .getServer()
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .<Long, JobDAGInfo>getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO))
                .thenReturn(map);
        when(environment.storageConfig().getEngineConfig().getHistoryJobExpireMinutes())
                .thenReturn(60);
        doAnswer(
                        invocation -> {
                            Long key = invocation.getArgument(0);
                            JobDAGInfo value = invocation.getArgument(1);
                            persisted.put(key, value);
                            return cached.put(key, value);
                        })
                .when(map)
                .put(anyLong(), any(JobDAGInfo.class));
        doAnswer(
                        invocation -> {
                            Long key = invocation.getArgument(0);
                            JobDAGInfo value = invocation.getArgument(1);
                            writtenKeys.add(key);
                            persisted.put(key, value);
                            return cached.put(key, value);
                        })
                .when(map)
                .put(anyLong(), any(JobDAGInfo.class), eq(60L), eq(TimeUnit.MINUTES));
        when(map.get(anyLong())).thenAnswer(invocation -> cached.get(invocation.getArgument(0)));
        when(map.evict(anyLong()))
                .thenAnswer(invocation -> cached.remove(invocation.getArgument(0)) != null);
        doAnswer(
                        invocation -> {
                            Iterable<Long> keys = invocation.getArgument(0);
                            for (Long key : keys) {
                                if (persisted.containsKey(key)) {
                                    cached.put(key, persisted.get(key));
                                }
                            }
                            return null;
                        })
                .when(map)
                .loadAll(anySet(), eq(true));
        doAnswer(
                        invocation -> {
                            cached.remove(invocation.getArgument(0));
                            persisted.remove(invocation.getArgument(0));
                            return null;
                        })
                .when(map)
                .delete(anyLong());

        workload = new IMapDagStorageBenchmarkWorkload();
        workload.pipelineCount = 1;
        workload.storedDagCount = 100;
        workload.setUp(environment);
        clearInvocations(map);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 20})
    void boundsIntermediateVerificationWithoutReloadingHistory(int batches) {
        Set<Long> retainedKeys = new HashSet<>(persisted.keySet());
        for (int batch = 0; batch < batches; batch++) {
            storeBatch(IterationType.MEASUREMENT, batches + 1);
            workload.cleanStoreIteration();
            assertEquals(retainedKeys, persisted.keySet());
        }
        assertEquals(batches * 100, new HashSet<>(writtenKeys).size());
        verify(map, times(batches * 3)).get(anyLong());
        verify(map, times(batches * 100)).delete(anyLong());
        verify(map, never()).evict(anyLong());
        verify(map, never()).loadAll(anySet(), anyBoolean());
        verify(map, times(batches * 100))
                .put(anyLong(), any(JobDAGInfo.class), eq(60L), eq(TimeUnit.MINUTES));
    }

    @ParameterizedTest
    @CsvSource({"0,1", "3,1", "3,5"})
    void reloadsOnlyAfterTheFinalMeasurement(int warmups, int measurements) {
        for (int index = 0; index < warmups; index++) {
            storeBatch(IterationType.WARMUP, warmups);
            workload.cleanStoreIteration();
        }
        for (int index = 0; index < measurements - 1; index++) {
            storeBatch(IterationType.MEASUREMENT, measurements);
            workload.cleanStoreIteration();
        }
        verify(map, never()).evict(anyLong());
        verify(map, never()).loadAll(anySet(), anyBoolean());

        long lastKey = storeBatch(IterationType.MEASUREMENT, measurements);
        workload.cleanStoreIteration();
        Set<Long> samples = new HashSet<>();
        samples.add(lastKey + 99);
        samples.add(lastKey + 49);
        samples.add(lastKey);
        verify(map).loadAll(samples, true);
        for (Long key : samples) {
            verify(map).evict(key);
        }
        assertEquals(101, persisted.size());
    }

    @ParameterizedTest
    @CsvSource({"0,false", "50,false", "99,false", "0,true", "50,true", "99,true"})
    void rejectsInvalidCachedSamplesAndStillDeletesTheBatch(int sampleIndex, boolean missing) {
        storeBatch(IterationType.MEASUREMENT, 2);
        Long key = writtenKeys.get(sampleIndex);
        if (missing) {
            cached.remove(key);
        } else {
            cached.put(key, JobDagFixtureFactory.create(2));
        }
        IllegalStateException failure =
                assertThrows(IllegalStateException.class, workload::cleanStoreIteration);
        assertTrue(failure.getMessage().contains("did not match for key " + key));
        assertEquals(101, persisted.size());
        verify(map, times(100)).delete(anyLong());
        verify(map, never()).loadAll(anySet(), anyBoolean());
    }

    @Test
    void finalReadBackDetectsMissingPersistenceEvenWhenTheCachedValueIsCorrect() {
        storeBatch(IterationType.MEASUREMENT, 1);
        Long key = writtenKeys.get(0);
        persisted.remove(key);
        assertTrue(cached.containsKey(key));

        IllegalStateException failure =
                assertThrows(IllegalStateException.class, workload::cleanStoreIteration);
        assertTrue(failure.getMessage().contains("did not match for key " + key));
        verify(map, times(100)).delete(anyLong());
        assertEquals(101, persisted.size());
    }

    @Test
    void cleansAcknowledgedWritesAfterAnIncompleteBatch() {
        doAnswer(
                        invocation -> {
                            if (writtenKeys.size() == 6) {
                                throw new IllegalStateException("write failed");
                            }
                            Long key = invocation.getArgument(0);
                            JobDAGInfo value = invocation.getArgument(1);
                            writtenKeys.add(key);
                            persisted.put(key, value);
                            return cached.put(key, value);
                        })
                .when(map)
                .put(anyLong(), any(JobDAGInfo.class), eq(60L), eq(TimeUnit.MINUTES));
        workload.prepareStoreIteration(iteration(IterationType.MEASUREMENT, 1));
        assertEquals(
                "write failed",
                assertThrows(IllegalStateException.class, workload::storeFinishedJobDagBatch)
                        .getMessage());
        assertTrue(
                assertThrows(IllegalStateException.class, workload::cleanStoreIteration)
                        .getMessage()
                        .contains("expected entry count"));
        verify(map, times(6)).delete(anyLong());
        assertEquals(101, persisted.size());
        verify(map, never()).loadAll(anySet(), anyBoolean());
    }

    @Test
    void loadInvocationsStillReloadAndValidateWithoutDeletingData() {
        workload.prepareStoreIteration(iteration(IterationType.MEASUREMENT, 1));
        for (int index = 0; index < 2; index++) {
            workload.prepareInvocation();
            workload.loadFinishedJobDag();
            workload.cleanInvocation();
        }
        workload.cleanStoreIteration();
        verify(map, times(2)).loadAll(anySet(), eq(true));
        verify(map, times(4)).evict(anyLong());
        verify(map, times(2)).get(anyLong());
        verify(map, never()).delete(anyLong());
    }

    private long storeBatch(IterationType type, int count) {
        workload.prepareStoreIteration(iteration(type, count));
        return workload.storeFinishedJobDagBatch();
    }

    private static IterationParams iteration(IterationType type, int count) {
        return new IterationParams(type, count, TimeValue.seconds(1), 1);
    }
}
