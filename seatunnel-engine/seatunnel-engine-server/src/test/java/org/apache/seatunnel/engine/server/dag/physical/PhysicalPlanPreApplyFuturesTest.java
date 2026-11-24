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

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The pre-applied slot futures are merged by every pipeline that restores.
 *
 * <p>{@code SubPlan.stateProcess()} is synchronized per pipeline, but each pipeline is a different
 * {@code SubPlan} instance, so two pipelines of the same job can merge into this one map from their
 * own threads while {@code ResourceUtils} reads it for a third. The field was a plain {@code
 * HashMap}, which can lose an entry under that access pattern; a lost entry leaves a task group
 * with no pre-applied future, so the pipeline fails to allocate and the slot already acquired for
 * it is neither consumed nor released.
 */
class PhysicalPlanPreApplyFuturesTest {

    private static PhysicalPlan newPhysicalPlan() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("pre-apply-test");

        JobImmutableInformation information = mock(JobImmutableInformation.class);
        when(information.getJobId()).thenReturn(7L);
        when(information.getJobConfig()).thenReturn(jobConfig);

        @SuppressWarnings("unchecked")
        IMap<Object, Object> runningJobStateIMap = mock(IMap.class);
        @SuppressWarnings("unchecked")
        IMap<Object, Object> runningJobStateTimestampsIMap = mock(IMap.class);

        return new PhysicalPlan(
                Collections.singletonList(mock(SubPlan.class)),
                mock(ExecutorService.class),
                information,
                System.currentTimeMillis(),
                runningJobStateIMap,
                runningJobStateTimestampsIMap);
    }

    private static TaskGroupLocation location(int taskGroupId) {
        return new TaskGroupLocation(7L, 1, taskGroupId);
    }

    private static Map<TaskGroupLocation, CompletableFuture<SlotProfile>> entries(
            int from, int count) {
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> map = new HashMap<>();
        for (int i = from; i < from + count; i++) {
            map.put(location(i), CompletableFuture.completedFuture(new SlotProfile()));
        }
        return map;
    }

    @Test
    void theMapIsSafeForConcurrentAccessFromTheStart() {
        Assertions.assertInstanceOf(
                ConcurrentMap.class,
                newPhysicalPlan().getPreApplyResourceFutures(),
                "pipelines merge into this map from their own threads");
    }

    @Test
    void replacingTheMapDoesNotPutANonConcurrentOneBack() {
        // The whole-job path hands over a locally built HashMap. Storing it as
        // given would silently undo the contract for every pipeline that
        // restores afterwards and merges into it.
        PhysicalPlan plan = newPhysicalPlan();
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> handedOver = entries(0, 3);

        plan.setPreApplyResourceFutures(handedOver);

        Assertions.assertInstanceOf(
                ConcurrentMap.class,
                plan.getPreApplyResourceFutures(),
                "a plain map must not be installed");
        Assertions.assertEquals(
                handedOver.keySet(),
                plan.getPreApplyResourceFutures().keySet(),
                "every handed-over entry must survive the copy");
    }

    @Test
    void replacingTheMapWithNothingLeavesAUsableMap() {
        PhysicalPlan plan = newPhysicalPlan();

        plan.setPreApplyResourceFutures(null);

        Assertions.assertTrue(plan.getPreApplyResourceFutures().isEmpty());
        // A later restore merges into whatever is stored; that must not NPE.
        plan.getPreApplyResourceFutures().putAll(entries(0, 1));
        Assertions.assertEquals(1, plan.getPreApplyResourceFutures().size());
    }

    @Test
    void everyPipelineMergeSurvivesWhenRestoresOverlap() throws Exception {
        // Four pipelines restoring at once, which is the shape that produced
        // the lost entries: each merges its own task groups into the shared
        // map while the others do the same.
        PhysicalPlan plan = newPhysicalPlan();
        int pipelines = 4;
        int perPipeline = 250;
        List<Map<TaskGroupLocation, CompletableFuture<SlotProfile>>> batches = new ArrayList<>();
        for (int p = 0; p < pipelines; p++) {
            batches.add(entries(p * perPipeline, perPipeline));
        }

        ExecutorService threads = Executors.newFixedThreadPool(pipelines);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(pipelines);
        try {
            for (Map<TaskGroupLocation, CompletableFuture<SlotProfile>> batch : batches) {
                threads.submit(
                        () -> {
                            try {
                                start.await();
                                plan.getPreApplyResourceFutures().putAll(batch);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }
            start.countDown();
            Assertions.assertTrue(
                    done.await(30, TimeUnit.SECONDS), "pipeline merges did not finish");
        } finally {
            threads.shutdownNow();
        }

        Assertions.assertEquals(
                pipelines * perPipeline,
                plan.getPreApplyResourceFutures().size(),
                "no task group may lose its pre-applied future");
        for (Map<TaskGroupLocation, CompletableFuture<SlotProfile>> batch : batches) {
            for (TaskGroupLocation key : batch.keySet()) {
                Assertions.assertNotNull(
                        plan.getPreApplyResourceFutures().get(key),
                        "missing pre-applied future for " + key);
            }
        }
    }
}
