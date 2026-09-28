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
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.map.IMap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

class SubPlanResourceRestoreTest {
    @Test
    void insufficientResourcesMustNotStartRestore() throws Exception {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan plan = failedPipeline(master);
        Mockito.when(master.preApplyResources(plan)).thenReturn(false);
        InvocationTargetException error =
                Assertions.assertThrows(InvocationTargetException.class, () -> processState(plan));
        Assertions.assertInstanceOf(NoEnoughResourceException.class, error.getCause());
        Mockito.verify(plan, Mockito.never()).restorePipeline();
        Mockito.verify(master).releasePipelineResource(plan);
    }

    @Test
    void allocatedResourcesAllowRestore() throws Exception {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan plan = failedPipeline(master);
        Mockito.when(master.preApplyResources(plan)).thenReturn(true);
        processState(plan);
        Mockito.verify(plan).restorePipeline();
    }

    @Test
    void exhaustedRetryBudgetDoesNotAllocateOrRestore() throws Exception {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan plan = failedPipeline(master);
        plan.setPipelineRestoreNum(new AtomicInteger(plan.getPipelineMaxRestoreNum()));
        processState(plan);
        Mockito.verify(master, Mockito.never()).preApplyResources(plan);
        Mockito.verify(plan, Mockito.never()).restorePipeline();
        Assertions.assertTrue(plan.getPipelineFuture().isDone());
        Assertions.assertEquals(PipelineStatus.FAILED, plan.getPipelineState());
    }

    @SuppressWarnings("unchecked")
    private SubPlan failedPipeline(JobMaster master) {
        JobConfig config = new JobConfig();
        config.setName("resource-restore");
        config.getEnvOptions().put("job.retry.interval.seconds", 0);
        JobImmutableInformation info = Mockito.mock(JobImmutableInformation.class);
        Mockito.when(info.getJobConfig()).thenReturn(config);
        Mockito.when(info.getJobId()).thenReturn(1L);
        IMap<Object, Object> states = Mockito.mock(IMap.class);
        Mockito.when(states.get(Mockito.any())).thenReturn(PipelineStatus.FAILED);
        IMap<Object, Long[]> timestamps = Mockito.mock(IMap.class);
        Mockito.when(timestamps.get(Mockito.any()))
                .thenReturn(new Long[PipelineStatus.values().length]);
        SubPlan plan =
                Mockito.spy(
                        new SubPlan(
                                1,
                                1,
                                0L,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                info,
                                Mockito.mock(ExecutorService.class),
                                states,
                                timestamps,
                                Collections.emptyMap()));
        plan.setJobMaster(master);
        plan.isRunning = true;
        Mockito.when(master.isNeedRestore()).thenReturn(true);
        Mockito.doNothing().when(plan).restorePipeline();
        return plan;
    }

    private void processState(SubPlan plan) throws Exception {
        Method method = SubPlan.class.getDeclaredMethod("stateProcess");
        method.setAccessible(true);
        method.invoke(plan);
    }
}
