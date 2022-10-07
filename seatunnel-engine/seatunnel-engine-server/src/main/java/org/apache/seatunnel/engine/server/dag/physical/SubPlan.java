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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineState;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import lombok.NonNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SubPlan {
    private static final ILogger LOGGER = Logger.getLogger(SubPlan.class);

    private final List<PhysicalVertex> physicalVertexList;

    private final List<PhysicalVertex> coordinatorVertexList;

    private final int pipelineId;

    private final int totalPipelineNum;

    private final JobImmutableInformation jobImmutableInformation;

    private AtomicInteger finishedTaskNum = new AtomicInteger(0);

    private AtomicInteger canceledTaskNum = new AtomicInteger(0);

    private AtomicInteger failedTaskNum = new AtomicInteger(0);

    private final String pipelineFullName;

    private final IMap<Object, Object> runningJobStateIMap;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * pipeline transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * Complete this future when this sub plan complete. When this future completed, the waitForCompleteBySubPlan in {@link PhysicalPlan }
     * whenComplete method will be called.
     */
    private CompletableFuture<PipelineState> pipelineFuture;

    private final PipelineLocation pipelineLocation;

    private final ExecutorService executorService;

    private JobMaster jobMaster;

    private PassiveCompletableFuture<Void> reSchedulerPipelineFuture;

    private Integer pipelineRestoreNum;

    public SubPlan(int pipelineId,
                   int totalPipelineNum,
                   long initializationTimestamp,
                   @NonNull List<PhysicalVertex> physicalVertexList,
                   @NonNull List<PhysicalVertex> coordinatorVertexList,
                   @NonNull JobImmutableInformation jobImmutableInformation,
                   @NonNull ExecutorService executorService,
                   @NonNull IMap runningJobStateIMap,
                   @NonNull IMap runningJobStateTimestampsIMap) {
        this.pipelineId = pipelineId;
        this.pipelineLocation = new PipelineLocation(jobImmutableInformation.getJobId(), pipelineId);
        this.pipelineFuture = new CompletableFuture<>();
        this.totalPipelineNum = totalPipelineNum;
        this.physicalVertexList = physicalVertexList;
        this.coordinatorVertexList = coordinatorVertexList;
        pipelineRestoreNum = 0;

        Long[] stateTimestamps = new Long[PipelineState.values().length];
        if (runningJobStateTimestampsIMap.get(pipelineLocation) == null) {
            stateTimestamps[PipelineState.INITIALIZING.ordinal()] = initializationTimestamp;
            runningJobStateTimestampsIMap.put(pipelineLocation, stateTimestamps);

        }

        if (runningJobStateIMap.get(pipelineLocation) == null) {
            // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
            stateTimestamps[PipelineState.CREATED.ordinal()] = System.currentTimeMillis();
            runningJobStateTimestampsIMap.put(pipelineLocation, stateTimestamps);

            runningJobStateIMap.put(pipelineLocation, PipelineState.CREATED);
        }

        this.jobImmutableInformation = jobImmutableInformation;
        this.pipelineFullName = String.format(
            "Job %s (%s), Pipeline: [(%d/%d)]",
            jobImmutableInformation.getJobConfig().getName(),
            jobImmutableInformation.getJobId(),
            pipelineId,
            totalPipelineNum);

        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.executorService = executorService;
    }

    public PassiveCompletableFuture<PipelineState> initStateFuture() {
        physicalVertexList.stream().forEach(physicalVertex -> {
            addPhysicalVertexCallBack(physicalVertex.initStateFuture());
        });

        coordinatorVertexList.stream().forEach(coordinator -> {
            addPhysicalVertexCallBack(coordinator.initStateFuture());
        });

        this.pipelineFuture = new CompletableFuture<>();
        return new PassiveCompletableFuture<>(pipelineFuture);
    }

    private void addPhysicalVertexCallBack(PassiveCompletableFuture<TaskExecutionState> future) {
        future.thenAcceptAsync(executionState -> {
            // We need not handle t, Because we will not return t from PhysicalVertex
            if (ExecutionState.CANCELED.equals(executionState.getExecutionState())) {
                canceledTaskNum.incrementAndGet();
            } else if (ExecutionState.FAILED.equals(executionState.getExecutionState())) {
                LOGGER.severe(String.format("Task Failed in %s, Begin to cancel other tasks in this pipeline.",
                    this.getPipelineFullName()));
                failedTaskNum.incrementAndGet();
                cancelPipeline();
            }

            if (finishedTaskNum.incrementAndGet() == (physicalVertexList.size() + coordinatorVertexList.size())) {
                if (failedTaskNum.get() > 0) {
                    turnToEndState(PipelineState.FAILED);
                    LOGGER.info(String.format("%s end with state FAILED", this.pipelineFullName));
                } else if (canceledTaskNum.get() > 0) {
                    turnToEndState(PipelineState.CANCELED);
                    LOGGER.info(String.format("%s end with state CANCELED", this.pipelineFullName));
                } else {
                    turnToEndState(PipelineState.FINISHED);
                    LOGGER.info(String.format("%s end with state FINISHED", this.pipelineFullName));
                }
                pipelineFuture.complete((PipelineState) runningJobStateIMap.get(pipelineLocation));
            }
        });
    }

    private void turnToEndState(@NonNull PipelineState endState) {
        synchronized (this) {
            // consistency check
            PipelineState current = (PipelineState) runningJobStateIMap.get(pipelineLocation);
            if (current.isEndState()) {
                String message = "Pipeline is trying to leave terminal state " + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (!endState.isEndState()) {
                String message = "Need a end state, not " + endState;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
            updateStateTimestamps(endState);

            runningJobStateIMap.set(pipelineLocation, endState);
        }
    }

    public boolean updatePipelineState(@NonNull PipelineState current, @NonNull PipelineState targetState) {
        synchronized (this) {
            // consistency check
            if (current.isEndState()) {
                String message = "Pipeline is trying to leave terminal state " + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (PipelineState.SCHEDULED.equals(targetState) && !PipelineState.CREATED.equals(current)) {
                String message = "Only [CREATED] pipeline can turn to [SCHEDULED]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (PipelineState.DEPLOYING.equals(targetState) && !PipelineState.SCHEDULED.equals(current)) {
                String message = "Only [SCHEDULED] pipeline can turn to [DEPLOYING]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (PipelineState.RUNNING.equals(targetState) && !PipelineState.DEPLOYING.equals(current)) {
                String message = "Only [DEPLOYING] pipeline can turn to [RUNNING]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            // now do the actual state transition
            if (current.equals(runningJobStateIMap.get(pipelineLocation))) {
                LOGGER.info(String.format("%s turn from state %s to %s.",
                    pipelineFullName,
                    current,
                    targetState));

                // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
                updateStateTimestamps(targetState);
                runningJobStateIMap.set(pipelineLocation, targetState);
                return true;
            } else {
                return false;
            }
        }
    }

    public void cancelPipeline() {
        if (getPipelineState().isEndState()) {
            LOGGER.warning(
                String.format("%s is in end state %s, can not be cancel", pipelineFullName, getPipelineState()));
            return;
        }
        // If an active Master Node done and another Master Node active, we can not know whether canceled pipeline
        // complete. So we need cancel running pipeline again.
        if (!PipelineState.CANCELING.equals((PipelineState) runningJobStateIMap.get(pipelineLocation))) {
            updatePipelineState(getPipelineState(), PipelineState.CANCELING);
        }
        cancelPipelineTasks();
    }

    private void cancelPipelineTasks() {
        List<CompletableFuture<Void>> coordinatorCancelList =
            coordinatorVertexList.stream().map(coordinator -> cancelTask(coordinator)).filter(x -> x != null)
                .collect(Collectors.toList());

        List<CompletableFuture<Void>> taskCancelList =
            physicalVertexList.stream().map(task -> cancelTask(task)).filter(x -> x != null)
                .collect(Collectors.toList());

        try {
            coordinatorCancelList.addAll(taskCancelList);
            CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                coordinatorCancelList.toArray(new CompletableFuture[coordinatorCancelList.size()]));
            voidCompletableFuture.get();
        } catch (Exception e) {
            LOGGER.severe(
                String.format("%s cancel error with exception: %s", pipelineFullName, ExceptionUtils.getMessage(e)));
        }
    }

    private CompletableFuture<Void> cancelTask(@NonNull PhysicalVertex task) {
        if (!task.getExecutionState().isEndState() &&
            !ExecutionState.CANCELING.equals(task.getExecutionState())) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                task.cancel();
                return null;
            }, executorService);
            return future;
        }
        return null;
    }

    /**
     * Before restore a pipeline, the pipeline must do reset
     */
    private void reset() {
        resetPipelineState();
        finishedTaskNum.set(0);
        canceledTaskNum.set(0);
        failedTaskNum.set(0);

        coordinatorVertexList.forEach(coordinate -> {
            coordinate.reset();
        });

        physicalVertexList.forEach(task -> {
            task.reset();
        });
    }

    private void updateStateTimestamps(@NonNull PipelineState targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update runningJobStateIMap
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(pipelineLocation);
        stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsIMap.set(pipelineLocation, stateTimestamps);

    }

    private void resetPipelineState() {
        PipelineState pipelineState = getPipelineState();
        if (!pipelineState.isEndState()) {
            String message = String.format("%s reset state failed, only end state can be reset, current is %s",
                getPipelineFullName(), pipelineState);
            LOGGER.severe(message);
            throw new IllegalStateException(message);
        }

        updateStateTimestamps(PipelineState.CREATED);
        runningJobStateIMap.set(pipelineLocation, PipelineState.CREATED);
    }

    /**
     * restore the pipeline when pipeline failed or canceled by error.
     */
    public void restorePipeline() {
        synchronized (pipelineRestoreNum) {
            try {
                pipelineRestoreNum++;
                LOGGER.info(String.format("Restore pipeline %s", pipelineFullName));
                // We must ensure the scheduler complete and then can handle pipeline state change.
                jobMaster.getScheduleFuture().join();

                if (reSchedulerPipelineFuture != null) {
                    reSchedulerPipelineFuture.join();
                }
                reset();
                jobMaster.getPhysicalPlan().addPipelineEndCallback(this);
                if (jobMaster.getCheckpointManager().isCompletedPipeline(pipelineId)) {
                    forcePipelineFinish();
                    return;
                }
                reSchedulerPipelineFuture = jobMaster.reSchedulerPipeline(this);
                if (reSchedulerPipelineFuture != null) {
                    reSchedulerPipelineFuture.join();
                }
            } catch (Throwable e) {
                LOGGER.severe(
                    String.format("Restore pipeline %s error with exception: %s", pipelineFullName,
                        ExceptionUtils.getMessage(e)));
                cancelPipeline();
            }
        }
    }

    /**
     * If the job state in CheckpointManager is complete, we need force this pipeline finish
     */
    private void forcePipelineFinish() {
        coordinatorVertexList.forEach(coordinator -> coordinator.updateTaskExecutionState(
            new TaskExecutionState(coordinator.getTaskGroupLocation(), ExecutionState.FINISHED, null)));
        physicalVertexList.forEach(task -> task.updateTaskExecutionState(
            new TaskExecutionState(task.getTaskGroupLocation(), ExecutionState.FINISHED, null)));
    }

    /**
     * restore the pipeline state after new Master Node active
     */
    public void restorePipelineState() {
        // only need restore from RUNNING or CANCELING state
        if (getPipelineState().ordinal() < PipelineState.RUNNING.ordinal()) {
            restorePipeline();
        } else if (PipelineState.CANCELING.equals(getPipelineState())) {
            cancelPipelineTasks();
        }
    }

    public List<PhysicalVertex> getPhysicalVertexList() {
        return physicalVertexList;
    }

    public List<PhysicalVertex> getCoordinatorVertexList() {
        return coordinatorVertexList;
    }

    public String getPipelineFullName() {
        return pipelineFullName;
    }

    public PipelineState getPipelineState() {
        return (PipelineState) runningJobStateIMap.get(pipelineLocation);
    }

    public PipelineLocation getPipelineLocation() {
        return pipelineLocation;
    }

    public void setJobMaster(JobMaster jobMaster) {
        this.jobMaster = jobMaster;
        coordinatorVertexList.forEach(coordinator -> coordinator.setJobMaster(jobMaster));
        physicalVertexList.forEach(task -> task.setJobMaster(jobMaster));
    }

    public int getPipelineRestoreNum() {
        return pipelineRestoreNum;
    }
}
