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

import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineExecutionState;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorStatus;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.map.IMap;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Data
@Slf4j
public class SubPlan {

    /** The max num pipeline can restore. */
    private final int pipelineMaxRestoreNum;

    private final int pipelineRestoreIntervalSeconds;

    private final List<PhysicalVertex> physicalVertexList;

    private final List<PhysicalVertex> coordinatorVertexList;

    private final int pipelineId;

    private final AtomicInteger finishedTaskNum = new AtomicInteger(0);

    private final AtomicInteger canceledTaskNum = new AtomicInteger(0);

    private final AtomicInteger failedTaskNum = new AtomicInteger(0);

    private final String pipelineFullName;

    private final IMap<Object, Object> runningJobStateIMap;
    private final Map<String, String> tags;

    /**
     * Timestamps (in milliseconds) as returned by {@code System.currentTimeMillis()} when the
     * pipeline transitioned into a certain state. The index into this array is the ordinal of the
     * enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * Complete this future when this sub plan complete. When this future completed, the
     * waitForCompleteBySubPlan in {@link PhysicalPlan } whenComplete method will be called.
     */
    private CompletableFuture<PipelineExecutionState> pipelineFuture;

    private final PipelineLocation pipelineLocation;

    /** The error throw by physicalVertex, should be set when physicalVertex throw error. */
    private AtomicReference<String> errorByPhysicalVertex = new AtomicReference<>();

    private final ExecutorService executorService;

    private JobMaster jobMaster;

    private PassiveCompletableFuture<Void> reSchedulerPipelineFuture;

    private Integer pipelineRestoreNum;

    private final Object restoreLock = new Object();

    private volatile PipelineStatus currPipelineStatus;

    public volatile boolean isRunning = false;

    private Map<TaskGroupLocation, SlotProfile> slotProfiles;

    public SubPlan(
            int pipelineId,
            int totalPipelineNum,
            long initializationTimestamp,
            @NonNull List<PhysicalVertex> physicalVertexList,
            @NonNull List<PhysicalVertex> coordinatorVertexList,
            @NonNull JobImmutableInformation jobImmutableInformation,
            @NonNull ExecutorService executorService,
            @NonNull IMap runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap,
            Map<String, String> tags) {
        this.pipelineId = pipelineId;
        this.pipelineLocation =
                new PipelineLocation(jobImmutableInformation.getJobId(), pipelineId);
        this.pipelineFuture = new CompletableFuture<>();
        this.physicalVertexList = physicalVertexList;
        this.coordinatorVertexList = coordinatorVertexList;
        pipelineRestoreNum = 0;
        pipelineMaxRestoreNum =
                Integer.parseInt(
                        jobImmutableInformation
                                .getJobConfig()
                                .getEnvOptions()
                                .computeIfAbsent(
                                        EnvCommonOptions.JOB_RETRY_TIMES.key(),
                                        key -> EnvCommonOptions.JOB_RETRY_TIMES.defaultValue())
                                .toString());
        pipelineRestoreIntervalSeconds =
                Integer.parseInt(
                        jobImmutableInformation
                                .getJobConfig()
                                .getEnvOptions()
                                .computeIfAbsent(
                                        EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(),
                                        key ->
                                                EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS
                                                        .defaultValue())
                                .toString());
        Long[] stateTimestamps = new Long[PipelineStatus.values().length];
        if (runningJobStateTimestampsIMap.get(pipelineLocation) == null) {
            stateTimestamps[PipelineStatus.INITIALIZING.ordinal()] = initializationTimestamp;
            runningJobStateTimestampsIMap.put(pipelineLocation, stateTimestamps);
        }

        if (runningJobStateIMap.get(pipelineLocation) == null) {
            // we must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap
            stateTimestamps[PipelineStatus.CREATED.ordinal()] = System.currentTimeMillis();
            runningJobStateTimestampsIMap.put(pipelineLocation, stateTimestamps);

            runningJobStateIMap.put(pipelineLocation, PipelineStatus.CREATED);
        }

        this.currPipelineStatus = (PipelineStatus) runningJobStateIMap.get(pipelineLocation);

        this.pipelineFullName =
                String.format(
                        "Job %s (%s), Pipeline: [(%d/%d)]",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId(),
                        pipelineId,
                        totalPipelineNum);
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.executorService = executorService;
        this.tags = tags;
    }

    public synchronized PassiveCompletableFuture<PipelineExecutionState> initStateFuture() {
        // reset errorByPhysicalVertex when restore pipeline
        errorByPhysicalVertex = new AtomicReference<>();
        physicalVertexList.forEach(
                physicalVertex -> {
                    addPhysicalVertexCallBack(physicalVertex.initStateFuture(), physicalVertex);
                });

        coordinatorVertexList.forEach(
                coordinator -> {
                    addPhysicalVertexCallBack(coordinator.initStateFuture(), coordinator);
                });

        this.pipelineFuture = new CompletableFuture<>();
        return new PassiveCompletableFuture<>(pipelineFuture);
    }

    private void addPhysicalVertexCallBack(
            PassiveCompletableFuture<TaskExecutionState> future, PhysicalVertex task) {
        future.thenAcceptAsync(
                executionState -> {
                    try {
                        log.info(
                                "{} future complete with state {}",
                                task.getTaskFullName(),
                                executionState.getExecutionState());
                        // We need not handle t, Because we will not return t from PhysicalVertex
                        if (ExecutionState.CANCELED.equals(executionState.getExecutionState())) {
                            canceledTaskNum.incrementAndGet();
                        } else if (ExecutionState.FAILED.equals(
                                executionState.getExecutionState())) {
                            log.error(
                                    String.format(
                                            "Task %s Failed in %s, Begin to cancel other tasks in this pipeline.",
                                            executionState.getTaskGroupLocation(),
                                            this.getPipelineFullName()));
                            failedTaskNum.incrementAndGet();
                            errorByPhysicalVertex.compareAndSet(
                                    null, executionState.getThrowableMsg());
                            updatePipelineState(PipelineStatus.FAILING);
                        }

                        if (finishedTaskNum.incrementAndGet()
                                == (physicalVertexList.size() + coordinatorVertexList.size())) {
                            PipelineStatus pipelineEndState = getPipelineEndState();
                            log.info(
                                    String.format(
                                            "%s will end with state %s",
                                            this.pipelineFullName, pipelineEndState));
                            updatePipelineState(pipelineEndState);
                        }
                    } catch (Throwable e) {
                        log.error(
                                String.format(
                                        "Never come here. handle %s %s error",
                                        executionState.getTaskGroupLocation(),
                                        executionState.getExecutionState()),
                                e);
                    }
                },
                executorService);
    }

    private PipelineStatus getPipelineEndState() {
        PipelineStatus pipelineStatus = null;
        if (failedTaskNum.get() > 0) {
            pipelineStatus = PipelineStatus.FAILED;
            // we don't care the checkpoint error reason when the task is
            // failed.
            jobMaster.getCheckpointManager().cancelCheckpoint(getPipelineId()).join();
        } else if (canceledTaskNum.get() > 0) {
            pipelineStatus = PipelineStatus.CANCELED;
            CheckpointCoordinatorState checkpointCoordinatorState =
                    jobMaster.getCheckpointManager().cancelCheckpoint(getPipelineId()).join();
            if (CheckpointCoordinatorStatus.FAILED.equals(
                    checkpointCoordinatorState.getCheckpointCoordinatorStatus())) {
                pipelineStatus = PipelineStatus.FAILED;
                errorByPhysicalVertex.compareAndSet(
                        null, checkpointCoordinatorState.getThrowableMsg());
            }

            // Because the pipeline state must update by tasks, If the pipeline can not get enough
            // slot, the pipeline state will turn to Failing and then cancel all tasks in this
            // pipeline.
            // Because the tasks never run, so the tasks will complete with CANCELED. But the actual
            // status of the pipeline should be FAILED
            if (getPipelineState().equals(PipelineStatus.FAILING)) {
                pipelineStatus = PipelineStatus.FAILED;
            }
        } else {
            pipelineStatus = PipelineStatus.FINISHED;
            CheckpointCoordinatorState checkpointCoordinatorState =
                    jobMaster
                            .getCheckpointManager()
                            .waitCheckpointCoordinatorComplete(getPipelineId())
                            .join();

            if (CheckpointCoordinatorStatus.FAILED.equals(
                    checkpointCoordinatorState.getCheckpointCoordinatorStatus())) {
                pipelineStatus = PipelineStatus.FAILED;
                errorByPhysicalVertex.compareAndSet(
                        null, checkpointCoordinatorState.getThrowableMsg());
            } else if (CheckpointCoordinatorStatus.CANCELED.equals(
                    checkpointCoordinatorState.getCheckpointCoordinatorStatus())) {
                pipelineStatus = PipelineStatus.CANCELED;
                errorByPhysicalVertex.compareAndSet(
                        null, checkpointCoordinatorState.getThrowableMsg());
            }
        }
        return pipelineStatus;
    }

    private boolean checkNeedRestore(PipelineStatus pipelineStatus) {
        return canRestorePipeline() && !PipelineStatus.FINISHED.equals(pipelineStatus);
    }

    /** only call when the pipeline will never restart */
    private void notifyCheckpointManagerPipelineEnd(PipelineStatus pipelineStatus) {
        if (jobMaster.getCheckpointManager() == null) {
            return;
        }
        jobMaster
                .getCheckpointManager()
                .listenPipeline(getPipelineLocation().getPipelineId(), pipelineStatus)
                .join();
    }

    private void subPlanDone(PipelineStatus pipelineStatus) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        jobMaster.savePipelineMetricsToHistory(getPipelineLocation());
                        try {
                            jobMaster.removeMetricsContext(getPipelineLocation(), pipelineStatus);
                        } catch (Throwable e) {
                            log.error(
                                    "Remove metrics context for pipeline {} failed, with exception: {}",
                                    pipelineFullName,
                                    ExceptionUtils.getMessage(e));
                        }
                        notifyCheckpointManagerPipelineEnd(pipelineStatus);
                        jobMaster.releasePipelineResource(this);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            log.warn(
                    "The cleaning operation before pipeline {} completion is not completed, with exception: {} ",
                    pipelineFullName,
                    ExceptionUtils.getMessage(e));
        }
    }

    public boolean canRestorePipeline() {
        return jobMaster.isNeedRestore() && getPipelineRestoreNum() < pipelineMaxRestoreNum;
    }

    public synchronized void updatePipelineState(@NonNull PipelineStatus targetState) {
        try {
            PipelineStatus current = (PipelineStatus) runningJobStateIMap.get(pipelineLocation);
            log.debug(
                    String.format(
                            "Try to update the %s state from %s to %s",
                            pipelineFullName, current, targetState));

            if (current.equals(targetState)) {
                log.info(
                        "{} current state equals target state: {}, skip",
                        pipelineFullName,
                        targetState);
                return;
            }

            // consistency check
            if (current.isEndState()) {
                String message = "Pipeline is trying to leave terminal state " + current;
                log.info(message);
                return;
            }

            // now do the actual state transition
            // we must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap
            PipelineStatus finalTargetState = targetState;
            RetryUtils.retryWithException(
                    () -> {
                        updateStateTimestamps(finalTargetState);
                        runningJobStateIMap.set(pipelineLocation, finalTargetState);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                            Constant.OPERATION_RETRY_SLEEP));
            this.currPipelineStatus = targetState;
            log.info(
                    String.format(
                            "%s turned from state %s to %s.",
                            pipelineFullName, current, targetState));
            stateProcess();
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
            if (!targetState.equals(PipelineStatus.FAILING)) {
                makePipelineFailing(e);
            }
        }
    }

    public synchronized void cancelPipeline() {
        cancelCheckpointCoordinator();
        if (!getPipelineState().isEndState()) {
            updatePipelineState(PipelineStatus.CANCELING);
        }
    }

    private void cancelCheckpointCoordinator() {
        if (jobMaster.getCheckpointManager() != null) {
            jobMaster.getCheckpointManager().cancelCheckpoint(pipelineId).join();
        }
    }

    /** Before restore a pipeline, the pipeline must do reset */
    private synchronized void reset() throws Exception {
        resetPipelineState();
        finishedTaskNum.set(0);
        canceledTaskNum.set(0);
        failedTaskNum.set(0);

        coordinatorVertexList.forEach(PhysicalVertex::reset);

        physicalVertexList.forEach(PhysicalVertex::reset);
    }

    private void updateStateTimestamps(@NonNull PipelineStatus targetState) {
        // we must update runningJobStateTimestampsIMap first and then can update
        // runningJobStateIMap
        Long[] stateTimestamps = runningJobStateTimestampsIMap.get(pipelineLocation);
        stateTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        runningJobStateTimestampsIMap.set(pipelineLocation, stateTimestamps);
    }

    private void resetPipelineState() throws Exception {
        RetryUtils.retryWithException(
                () -> {
                    PipelineStatus pipelineState = getPipelineState();
                    if (!pipelineState.isEndState()) {
                        String message =
                                String.format(
                                        "%s reset state failed, only end state can be reset, current is %s",
                                        getPipelineFullName(), pipelineState);
                        log.error(message);
                        throw new IllegalStateException(message);
                    }
                    log.info(
                            String.format(
                                    "Reset pipeline %s state to %s",
                                    getPipelineFullName(), PipelineStatus.CREATED));
                    updateStateTimestamps(PipelineStatus.CREATED);
                    runningJobStateIMap.set(pipelineLocation, PipelineStatus.CREATED);
                    this.currPipelineStatus = PipelineStatus.CREATED;
                    log.info(
                            String.format(
                                    "Reset pipeline %s state to %s complete",
                                    getPipelineFullName(), PipelineStatus.CREATED));
                    return null;
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                        Constant.OPERATION_RETRY_SLEEP));
    }

    /**
     * reset the pipeline and task state and init state future again
     *
     * @return
     */
    private boolean prepareRestorePipeline() {
        synchronized (restoreLock) {
            try {
                pipelineRestoreNum++;
                log.info(
                        String.format(
                                "Restore time %s, pipeline %s",
                                pipelineRestoreNum + "", pipelineFullName));
                reset();
                jobMaster.getCheckpointManager().reportedPipelineRunning(pipelineId, false);
                jobMaster.getPhysicalPlan().addPipelineEndCallback(this);
                log.info(
                        "Wait {}s and then restore the pipeline {}",
                        pipelineRestoreIntervalSeconds,
                        getPipelineFullName());
                Thread.sleep(pipelineRestoreIntervalSeconds * 1000);
                return true;
            } catch (Throwable e) {
                if (this.currPipelineStatus.isEndState()) {
                    // restore failed
                    return false;
                }
                jobMaster.getPhysicalPlan().addPipelineEndCallback(this);
                return true;
            }
        }
    }

    /** restore the pipeline when pipeline failed or canceled by error. */
    public void restorePipeline() {
        try {
            if (jobMaster.getCheckpointManager().isCompletedPipeline(pipelineId)) {
                forcePipelineFinish();
            }
            startSubPlanStateProcess();
        } catch (Throwable e) {
            log.error(
                    String.format("Restore pipeline %s error with exception: ", pipelineFullName),
                    e);
            makePipelineFailing(e);
            startSubPlanStateProcess();
        }
    }

    /** If the job state in CheckpointManager is complete, we need force this pipeline finish */
    private void forcePipelineFinish() {
        coordinatorVertexList.forEach(
                coordinator ->
                        coordinator.updateStateByExecutionService(
                                new TaskExecutionState(
                                        coordinator.getTaskGroupLocation(),
                                        ExecutionState.FINISHED)));
        physicalVertexList.forEach(
                task ->
                        task.updateStateByExecutionService(
                                new TaskExecutionState(
                                        task.getTaskGroupLocation(), ExecutionState.FINISHED)));
    }

    /** restore the pipeline state after new Master Node active */
    public synchronized void restorePipelineState() {
        // if PipelineStatus is less than RUNNING, we need cancel it and reschedule.
        getPhysicalVertexList()
                .forEach(
                        task -> {
                            task.restoreExecutionState();
                        });

        getCoordinatorVertexList()
                .forEach(
                        task -> {
                            task.restoreExecutionState();
                        });

        if (getPipelineState().ordinal() < PipelineStatus.RUNNING.ordinal()) {
            updatePipelineState(PipelineStatus.CANCELING);
        } else if (PipelineStatus.RUNNING.equals(getPipelineState())) {
            AtomicBoolean allTaskRunning = new AtomicBoolean(true);
            getCoordinatorVertexList()
                    .forEach(
                            task -> {
                                if (!task.getExecutionState().equals(ExecutionState.RUNNING)) {
                                    allTaskRunning.set(false);
                                    return;
                                }
                            });

            getPhysicalVertexList()
                    .forEach(
                            task -> {
                                if (!task.getExecutionState().equals(ExecutionState.RUNNING)) {
                                    allTaskRunning.set(false);
                                    return;
                                }
                            });

            jobMaster
                    .getCheckpointManager()
                    .reportedPipelineRunning(
                            this.getPipelineLocation().getPipelineId(), allTaskRunning.get());
        }
        startSubPlanStateProcess();
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

    public PipelineStatus getPipelineState() {
        return this.currPipelineStatus;
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

    public void handleCheckpointError() {
        log.warn(
                String.format(
                        "%s checkpoint have error, cancel the pipeline", getPipelineFullName()));
        if (!getPipelineState().isEndState()) {
            updatePipelineState(PipelineStatus.CANCELING);
        }
    }

    public void startSubPlanStateProcess() {
        isRunning = true;
        log.info("{} state process is start", getPipelineFullName());
        stateProcess();
    }

    public void stopSubPlanStateProcess() {
        isRunning = false;
        log.info("{} state process is stop", getPipelineFullName());
    }

    private synchronized void stateProcess() {
        if (!isRunning) {
            log.warn(String.format("%s state process not start", pipelineFullName));
            return;
        }
        PipelineStatus state = getCurrPipelineStatus();
        switch (state) {
            case CREATED:
                updatePipelineState(PipelineStatus.SCHEDULED);
                break;
            case SCHEDULED:
                try {
                    ResourceUtils.applyResourceForPipeline(jobMaster, this);
                    log.debug(
                            "slotProfiles: {}, PipelineLocation: {}",
                            slotProfiles,
                            this.getPipelineLocation());
                    updatePipelineState(PipelineStatus.DEPLOYING);
                } catch (Exception e) {
                    makePipelineFailing(e);
                }
                break;
            case DEPLOYING:
                coordinatorVertexList.forEach(
                        task -> {
                            if (task.getExecutionState().equals(ExecutionState.CREATED)) {
                                task.startPhysicalVertex();
                                task.makeTaskGroupDeploy();
                            }
                        });

                physicalVertexList.forEach(
                        task -> {
                            if (task.getExecutionState().equals(ExecutionState.CREATED)) {
                                task.startPhysicalVertex();
                                task.makeTaskGroupDeploy();
                            }
                        });
                updatePipelineState(PipelineStatus.RUNNING);
                break;
            case RUNNING:
                break;
            case FAILING:
            case CANCELING:
                coordinatorVertexList.forEach(
                        task -> {
                            task.startPhysicalVertex();
                            task.cancel();
                        });

                physicalVertexList.forEach(
                        task -> {
                            task.startPhysicalVertex();
                            task.cancel();
                        });
                break;
            case FAILED:
            case CANCELED:
                if (checkNeedRestore(state) && prepareRestorePipeline()) {
                    jobMaster.releasePipelineResource(this);
                    jobMaster.preApplyResources(this);
                    restorePipeline();
                    return;
                }
                subPlanDone(state);
                stopSubPlanStateProcess();
                pipelineFuture.complete(
                        new PipelineExecutionState(pipelineId, state, errorByPhysicalVertex.get()));
                return;
            case FINISHED:
                subPlanDone(state);
                stopSubPlanStateProcess();
                pipelineFuture.complete(
                        new PipelineExecutionState(
                                pipelineId, getPipelineState(), errorByPhysicalVertex.get()));
                return;
            default:
                throw new IllegalArgumentException("Unknown Pipeline State: " + getPipelineState());
        }
    }

    public void makePipelineFailing(Throwable e) {
        errorByPhysicalVertex.compareAndSet(null, ExceptionUtils.getMessage(e));
        updatePipelineState(PipelineStatus.FAILING);
    }
}
