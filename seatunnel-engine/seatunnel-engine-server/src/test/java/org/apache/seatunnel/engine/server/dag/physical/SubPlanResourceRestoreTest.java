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
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.map.IMap;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class SubPlanResourceRestoreTest {
    @Test
    void insufficientResourcesMustNotStartRestore() throws Exception {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        PhysicalVertex vertex = Mockito.mock(PhysicalVertex.class);
        Mockito.when(vertex.getExecutionState()).thenReturn(ExecutionState.CREATED);
        SubPlan plan =
                failedPipeline(master, PipelineStatus.FAILED, Collections.singletonList(vertex));
        Mockito.when(master.preApplyResources(plan)).thenReturn(false);
        Assertions.assertDoesNotThrow(plan::startSubPlanStateProcess);
        Assertions.assertEquals(PipelineStatus.FAILING, plan.getPipelineState());
        Assertions.assertEquals(1, plan.getPipelineRestoreNum());
        Mockito.verify(plan, Mockito.never()).restorePipeline();
        Mockito.verify(master).releasePipelineResource(plan);
        Mockito.verify(vertex, Mockito.atLeastOnce()).cancel();
        Assertions.assertTrue(
                plan.getErrorByPhysicalVertex().get().contains("required task-group slots: 1"));
        Assertions.assertTrue(
                plan.getErrorByPhysicalVertex().get().contains(plan.getPipelineFullName()));
    }

    @Test
    void masterFailoverAllocationFailureMustNotAbortRemainingPipelines() {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan failed = failedPipeline(master);
        SubPlan canceled = failedPipeline(master, PipelineStatus.CANCELED);
        Mockito.when(master.preApplyResources(failed)).thenReturn(false, true);
        Mockito.when(master.preApplyResources(canceled)).thenReturn(false);

        Assertions.assertDoesNotThrow(
                () -> {
                    failed.restorePipelineState();
                    canceled.restorePipelineState();
                });
        Assertions.assertEquals(PipelineStatus.FAILING, failed.getPipelineState());
        Assertions.assertEquals(PipelineStatus.FAILING, canceled.getPipelineState());
        Assertions.assertEquals(1, failed.getPipelineRestoreNum());
        Assertions.assertEquals(1, canceled.getPipelineRestoreNum());
        Mockito.verify(failed, Mockito.never()).restorePipeline();
        Mockito.verify(canceled, Mockito.never()).restorePipeline();
        Mockito.verify(master).preApplyResources(canceled);

        // Retry after cancellation completes and replacement capacity becomes available.
        failed.updatePipelineState(PipelineStatus.FAILED);
        Assertions.assertEquals(2, failed.getPipelineRestoreNum());
        Mockito.verify(failed).restorePipeline();
    }

    @Test
    void cancellationDuringResourceAllocationMustNotBecomeFailure() {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan plan = failedPipeline(master);
        Mockito.when(master.preApplyResources(plan))
                .thenAnswer(
                        invocation -> {
                            Mockito.when(master.isNeedRestore()).thenReturn(false);
                            return false;
                        });

        Assertions.assertDoesNotThrow(plan::startSubPlanStateProcess);
        Assertions.assertEquals(PipelineStatus.CANCELING, plan.getPipelineState());
        Assertions.assertNull(plan.getErrorByPhysicalVertex().get());
        Mockito.verify(plan, Mockito.never()).restorePipeline();
    }

    @Test
    void neverStartedPipelineMustUseResourcesReservedByFailoverScheduler() {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        PhysicalVertex vertex = Mockito.mock(PhysicalVertex.class);
        TaskGroupLocation location = new TaskGroupLocation(1L, 1, 1L);
        SlotProfile slot = Mockito.mock(SlotProfile.class);
        Mockito.when(vertex.getExecutionState()).thenReturn(ExecutionState.CREATED);
        Mockito.when(vertex.getTaskGroupLocation()).thenReturn(location);
        Mockito.when(master.getPhysicalPlan().getPreApplyResourceFutures())
                .thenReturn(
                        Collections.singletonMap(
                                location, CompletableFuture.completedFuture(slot)));
        SubPlan plan =
                failedPipeline(master, PipelineStatus.CREATED, Collections.singletonList(vertex));

        Assertions.assertDoesNotThrow(plan::restorePipelineState);
        Assertions.assertEquals(PipelineStatus.RUNNING, plan.getPipelineState());
        Assertions.assertEquals(0, plan.getPipelineRestoreNum());
        Mockito.verify(master)
                .setOwnedSlotProfiles(
                        plan.getPipelineLocation(), Collections.singletonMap(location, slot));
        Mockito.verify(vertex).makeTaskGroupDeploy();
        Mockito.verify(master, Mockito.never()).releasePipelineResource(plan);
        Mockito.verify(master, Mockito.never()).preApplyResources(plan);
        Mockito.verify(plan, Mockito.never()).restorePipeline();
    }

    @Test
    void cancellationDuringRestorePreparationMustNotRequestNewResources() {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan plan = failedPipeline(master);
        CheckpointManager checkpointManager = master.getCheckpointManager();
        Mockito.doAnswer(
                        invocation -> {
                            Mockito.when(master.isNeedRestore()).thenReturn(false);
                            return null;
                        })
                .when(checkpointManager)
                .reportedPipelineRunning(1, false);

        Assertions.assertDoesNotThrow(plan::startSubPlanStateProcess);
        Assertions.assertEquals(PipelineStatus.CANCELING, plan.getPipelineState());
        Assertions.assertNull(plan.getErrorByPhysicalVertex().get());
        Mockito.verify(master, Mockito.never()).preApplyResources(plan);
        Mockito.verify(plan, Mockito.never()).restorePipeline();
    }

    @Test
    void partiallyStartedPipelineMustStillCancelBeforeRedeployment() {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        PhysicalVertex vertex = Mockito.mock(PhysicalVertex.class);
        Mockito.when(vertex.getExecutionState()).thenReturn(ExecutionState.RUNNING);
        SubPlan plan =
                failedPipeline(master, PipelineStatus.CREATED, Collections.singletonList(vertex));

        Assertions.assertDoesNotThrow(plan::restorePipelineState);
        Assertions.assertEquals(PipelineStatus.CANCELING, plan.getPipelineState());
        Mockito.verify(vertex, Mockito.atLeastOnce()).cancel();
        Mockito.verify(vertex, Mockito.never()).makeTaskGroupDeploy();
    }

    @Test
    void allocatedResourcesAllowRestore() throws Exception {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan plan = failedPipeline(master);
        Mockito.when(master.preApplyResources(plan)).thenReturn(true);
        plan.startSubPlanStateProcess();
        Mockito.verify(plan).restorePipeline();
    }

    @Test
    void exhaustedRetryBudgetDoesNotAllocateOrRestore() throws Exception {
        JobMaster master = Mockito.mock(JobMaster.class, Mockito.RETURNS_DEEP_STUBS);
        SubPlan plan = failedPipeline(master);
        plan.setPipelineRestoreNum(new AtomicInteger(plan.getPipelineMaxRestoreNum()));
        plan.startSubPlanStateProcess();
        Mockito.verify(master, Mockito.never()).preApplyResources(plan);
        Mockito.verify(plan, Mockito.never()).restorePipeline();
        Assertions.assertTrue(plan.getPipelineFuture().isDone());
        Assertions.assertEquals(PipelineStatus.FAILED, plan.getPipelineState());
    }

    private SubPlan failedPipeline(JobMaster master) {
        return failedPipeline(master, PipelineStatus.FAILED);
    }

    private SubPlan failedPipeline(JobMaster master, PipelineStatus initialState) {
        return failedPipeline(master, initialState, Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    private SubPlan failedPipeline(
            JobMaster master, PipelineStatus initialState, List<PhysicalVertex> vertices) {
        JobConfig config = new JobConfig();
        config.setName("resource-restore");
        config.getEnvOptions().put("job.retry.interval.seconds", 0);
        JobImmutableInformation info = Mockito.mock(JobImmutableInformation.class);
        Mockito.when(info.getJobConfig()).thenReturn(config);
        Mockito.when(info.getJobId()).thenReturn(1L);
        IMap<Object, Object> states = Mockito.mock(IMap.class);
        AtomicReference<PipelineStatus> currentState = new AtomicReference<>(initialState);
        Mockito.when(states.get(Mockito.any())).thenAnswer(invocation -> currentState.get());
        Mockito.doAnswer(
                        invocation -> {
                            currentState.set(invocation.getArgument(1));
                            return null;
                        })
                .when(states)
                .set(Mockito.any(), Mockito.any());
        IMap<Object, Long[]> timestamps = Mockito.mock(IMap.class);
        Mockito.when(timestamps.get(Mockito.any()))
                .thenReturn(new Long[PipelineStatus.values().length]);
        SubPlan plan =
                Mockito.spy(
                        new SubPlan(
                                1,
                                1,
                                0L,
                                vertices,
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
}
