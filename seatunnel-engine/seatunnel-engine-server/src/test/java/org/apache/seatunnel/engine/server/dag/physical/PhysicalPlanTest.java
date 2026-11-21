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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PhysicalPlanTest {

    private static final int PIPELINES = 8;
    private static final int TASK_GROUPS_PER_PIPELINE = 2_000;

    @Test
    void concurrentPipelineMergesKeepEveryEntry() throws Exception {
        PhysicalPlan physicalPlan = newPhysicalPlan();
        // Each pipeline merges its own futures, as JobMaster#preApplyResources(SubPlan) does.
        List<Map<TaskGroupLocation, CompletableFuture<SlotProfile>>> perPipeline =
                new ArrayList<>();
        for (int pipeline = 1; pipeline <= PIPELINES; pipeline++) {
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures = new HashMap<>();
            for (long taskGroup = 0; taskGroup < TASK_GROUPS_PER_PIPELINE; taskGroup++) {
                futures.put(
                        new TaskGroupLocation(1L, pipeline, taskGroup),
                        CompletableFuture.completedFuture(mock(SlotProfile.class)));
            }
            perPipeline.add(futures);
        }

        ExecutorService executor = Executors.newFixedThreadPool(PIPELINES);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures : perPipeline) {
                results.add(
                        executor.submit(
                                () -> {
                                    start.await();
                                    physicalPlan.getPreApplyResourceFutures().putAll(futures);
                                    return null;
                                }));
            }
            start.countDown();
            for (Future<?> result : results) {
                result.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> merged =
                physicalPlan.getPreApplyResourceFutures();
        assertEquals(PIPELINES * TASK_GROUPS_PER_PIPELINE, merged.size());
        for (Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures : perPipeline) {
            futures.forEach(
                    (location, future) -> assertNotNull(merged.get(location), location.toString()));
        }
    }

    @Test
    void setterKeepsTheMapConcurrent() {
        PhysicalPlan physicalPlan = newPhysicalPlan();
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures = new HashMap<>();
        TaskGroupLocation location = new TaskGroupLocation(1L, 1, 1L);
        futures.put(location, CompletableFuture.completedFuture(mock(SlotProfile.class)));

        physicalPlan.setPreApplyResourceFutures(futures);

        assertInstanceOf(ConcurrentHashMap.class, physicalPlan.getPreApplyResourceFutures());
        assertTrue(physicalPlan.getPreApplyResourceFutures().containsKey(location));
    }

    @SuppressWarnings("unchecked")
    private static PhysicalPlan newPhysicalPlan() {
        JobImmutableInformation jobInfo =
                mock(JobImmutableInformation.class, Answers.RETURNS_DEEP_STUBS);
        when(jobInfo.getJobId()).thenReturn(1L);
        return new PhysicalPlan(
                Collections.singletonList(mock(SubPlan.class)),
                mock(ExecutorService.class),
                jobInfo,
                System.currentTimeMillis(),
                mock(IMap.class),
                mock(IMap.class));
    }
}
