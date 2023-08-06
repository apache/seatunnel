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
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineExecutionState;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorStatus;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import lombok.Data;
import lombok.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Data
public class SubPlan {
    private static final ILogger LOGGER = Logger.getLogger(SubPlan.class);

    /** The max num pipeline can restore. */
    public static final int PIPELINE_MAX_RESTORE_NUM = 3; // TODO should set by config

    private final List<PhysicalVertex> physicalVertexList;

    private final List<PhysicalVertex> coordinatorVertexList;

    private final int pipelineId;

    private final AtomicInteger finishedTaskNum = new AtomicInteger(0);

    private final AtomicInteger canceledTaskNum = new AtomicInteger(0);

    private final AtomicInteger failedTaskNum = new AtomicInteger(0);

    private final String pipelineFullName;

    private final IMap<Object, Object> runningJobStateIMap;

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

    private volatile PipelineStatus currPipelineStatus = PipelineStatus.INITIALIZING;

    public SubPlan(
            int pipelineId,
            int totalPipelineNum,
            long initializationTimestamp,
            @NonNull List<PhysicalVertex> physicalVertexList,
            @NonNull List<PhysicalVertex> coordinatorVertexList,
            @NonNull JobImmutableInformation jobImmutableInformation,
            @NonNull ExecutorService executorService,
            @NonNull IMap runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap) {
        this.pipelineId = pipelineId;
        this.pipelineLocation =
                new PipelineLocation(jobImmutableInformation.getJobId(), pipelineId);
        this.pipelineFuture = new CompletableFuture<>();
        this.physicalVertexList = physicalVertexList;
        this.coordinatorVertexList = coordinatorVertexList;
        pipelineRestoreNum = 0;

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
    }

    public synchronized PassiveCompletableFuture<PipelineExecutionState> initStateFuture() {
        // reset errorByPhysicalVertex when restore pipeline
        errorByPhysicalVertex = new AtomicReference<>();
        physicalVertexList.forEach(
                physicalVertex -> {
                    addPhysicalVertexCallBack(physicalVertex.initStateFuture());
                });

        coordinatorVertexList.forEach(
                coordinator -> {
                    addPhysicalVertexCallBack(coordinator.initStateFuture());
                });

        this.pipelineFuture = new CompletableFuture<>();
        return new PassiveCompletableFuture<>(pipelineFuture);
    }

    private void addPhysicalVertexCallBack(PassiveCompletableFuture<TaskExecutionState> future) {
        future.thenAcceptAsync(
                executionState -> {
                    try {
                        // We need not handle t, Because we will not return t from PhysicalVertex
                        if (ExecutionState.CANCELED.equals(executionState.getExecutionState())) {
                            canceledTaskNum.incrementAndGet();
                        } else if (ExecutionState.FAILED.equals(
                                executionState.getExecutionState())) {
                            LOGGER.severe(
                                    String.format(
                                            "Task %s Failed in %s, Begin to cancel other tasks in this pipeline.",
                                            executionState.getTaskGroupLocation(),
                                            this.getPipelineFullName()));
                            failedTaskNum.incrementAndGet();
                            errorByPhysicalVertex.compareAndSet(
                                    null, executionState.getThrowableMsg());
                            cancelPipeline();
                        }

                        if (finishedTaskNum.incrementAndGet()
                                == (physicalVertexList.size() + coordinatorVertexList.size())) {
                            PipelineStatus pipelineEndState = getPipelineEndState();
                            LOGGER.info(
                                    String.format(
                                            "%s end with state %s",
                                            this.pipelineFullName, pipelineEndState));

                            if (!checkNeedRestore(pipelineEndState)) {
                                subPlanDone(pipelineEndState);
                                turnToEndState(pipelineEndState);
                                pipelineFuture.complete(
                                        new PipelineExecutionState(
                                                pipelineId,
                                                pipelineEndState,
                                                errorByPhysicalVertex.get()));
                            } else {
                                turnToEndState(pipelineEndState);
                                if (prepareRestorePipeline()) {
                                    restorePipeline();
                                } else {
                                    pipelineFuture.complete(
                                            new PipelineExecutionState(
                                                    pipelineId,
                                                    pipelineEndState,
                                                    errorByPhysicalVertex.get()));
                                }
                            }
                        }
                    } catch (Throwable e) {
                        LOGGER.severe(
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

    private void subPlanDone(PipelineStatus pipelineStatus) throws Exception {
        RetryUtils.retryWithException(
                () -> {
                    jobMaster.savePipelineMetricsToHistory(getPipelineLocation());
                    jobMaster.removeMetricsContext(getPipelineLocation(), pipelineStatus);
                    jobMaster.releasePipelineResource(this);
                    notifyCheckpointManagerPipelineEnd(pipelineStatus);
                    return null;
                },
                new RetryUtils.RetryMaterial(
                        Constant.OPERATION_RETRY_TIME,
                        true,
                        exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                        Constant.OPERATION_RETRY_SLEEP));
    }

    public boolean canRestorePipeline() {
        return jobMaster.isNeedRestore() && getPipelineRestoreNum() < PIPELINE_MAX_RESTORE_NUM;
    }

    private void turnToEndState(@NonNull PipelineStatus endState) throws Exception {
        synchronized (this) {
            // consistency check
            if (this.currPipelineStatus.isEndState() && !endState.isEndState()) {
                String message =
                        "Pipeline is trying to leave terminal state " + this.currPipelineStatus;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (!endState.isEndState()) {
                String message = "Need a end state, not " + endState;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            // we must update runningJobStateTimestampsIMap first and then can update
            // runningJobStateIMap
            RetryUtils.retryWithException(
                    () -> {
                        updateStateTimestamps(endState);
                        runningJobStateIMap.set(pipelineLocation, endState);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                            Constant.OPERATION_RETRY_SLEEP));
            this.currPipelineStatus = endState;
            LOGGER.info(
                    String.format(
                            "%s turn to end state %s.", pipelineFullName, currPipelineStatus));
        }
    }

    public boolean updatePipelineState(
            @NonNull PipelineStatus current, @NonNull PipelineStatus targetState) throws Exception {
        synchronized (this) {
            // consistency check
            if (current.isEndState()) {
                String message = "Pipeline is trying to leave terminal state " + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (PipelineStatus.SCHEDULED.equals(targetState)
                    && !PipelineStatus.CREATED.equals(current)) {
                String message = "Only [CREATED] pipeline can turn to [SCHEDULED]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (PipelineStatus.DEPLOYING.equals(targetState)
                    && !PipelineStatus.SCHEDULED.equals(current)) {
                String message = "Only [SCHEDULED] pipeline can turn to [DEPLOYING]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (PipelineStatus.RUNNING.equals(targetState)
                    && !PipelineStatus.DEPLOYING.equals(current)) {
                String message = "Only [DEPLOYING] pipeline can turn to [RUNNING]" + current;
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            // now do the actual state transition
            if (current.equals(runningJobStateIMap.get(pipelineLocation))) {
                LOGGER.info(
                        String.format(
                                "%s turn from state %s to %s.",
                                pipelineFullName, current, targetState));

                // we must update runningJobStateTimestampsIMap first and then can update
                // runningJobStateIMap
                RetryUtils.retryWithException(
                        () -> {
                            updateStateTimestamps(targetState);
                            runningJobStateIMap.set(pipelineLocation, targetState);
                            return null;
                        },
                        new RetryUtils.RetryMaterial(
                                Constant.OPERATION_RETRY_TIME,
                                true,
                                exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                                Constant.OPERATION_RETRY_SLEEP));
                this.currPipelineStatus = targetState;
                return true;
            } else {
                return false;
            }
        }
    }

    public synchronized void cancelPipeline() {
        for (int i = 0; i < 10; i++) {
            try {
                LOGGER.warning("start cancel job " + pipelineFullName + " count = " + i);
                if (getPipelineState().isEndState()) {
                    LOGGER.warning(
                            String.format(
                                    "%s is in end state %s, can not be cancel",
                                    pipelineFullName, getPipelineState()));
                }
                // If an active Master Node done and another Master Node active, we can
                // not know whether canceled pipeline complete.
                // So we need cancel running pipeline again.
                if (!PipelineStatus.CANCELING.equals(runningJobStateIMap.get(pipelineLocation))) {
                    updatePipelineState(getPipelineState(), PipelineStatus.CANCELING);
                }
                cancelCheckpointCoordinator();
                Optional<Exception> optionalException = cancelPipelineTasks();
                if (optionalException.isPresent()) {
                    throw optionalException.get();
                }
                break;
            } catch (OperationTimeoutException
                    | HazelcastInstanceNotActiveException
                    | InterruptedException e) {
                try {
                    Thread.sleep(2000);
                } catch (Exception ignored) {
                }
                LOGGER.warning(String.format("%s cancel error will retry", pipelineFullName), e);
            } catch (Throwable e) {
                LOGGER.warning(String.format("%s cancel error", pipelineFullName), e);
                break;
            }
        }
    }

    private void cancelCheckpointCoordinator() {
        if (jobMaster.getCheckpointManager() != null) {
            jobMaster.getCheckpointManager().cancelCheckpoint(pipelineId).join();
        }
    }

    private Optional<Exception> cancelPipelineTasks() {
        List<CompletableFuture<Void>> coordinatorCancelList =
                coordinatorVertexList.stream()
                        .map(this::cancelTask)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        List<CompletableFuture<Void>> taskCancelList =
                physicalVertexList.stream()
                        .map(this::cancelTask)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

        try {
            coordinatorCancelList.addAll(taskCancelList);
            CompletableFuture<Void> voidCompletableFuture =
                    CompletableFuture.allOf(
                            coordinatorCancelList.toArray(new CompletableFuture[0]));
            voidCompletableFuture.get();
            return Optional.empty();
        } catch (Exception e) {
            LOGGER.severe(
                    String.format(
                            "%s cancel error with exception: %s",
                            pipelineFullName, ExceptionUtils.getMessage(e)));
            return Optional.of(e);
        }
    }

    private CompletableFuture<Void> cancelTask(@NonNull PhysicalVertex task) {
        if (!task.getExecutionState().isEndState()
                && !ExecutionState.CANCELING.equals(task.getExecutionState())) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        task.cancel();
                        return null;
                    },
                    executorService);
        }
        return null;
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
                        LOGGER.severe(message);
                        throw new IllegalStateException(message);
                    }
                    LOGGER.info(
                            String.format(
                                    "Reset pipeline %s state to %s",
                                    getPipelineFullName(), PipelineStatus.CREATED));
                    updateStateTimestamps(PipelineStatus.CREATED);
                    runningJobStateIMap.set(pipelineLocation, PipelineStatus.CREATED);
                    this.currPipelineStatus = PipelineStatus.CREATED;
                    LOGGER.info(
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
                LOGGER.info(
                        String.format(
                                "Restore time %s, pipeline %s",
                                pipelineRestoreNum + "", pipelineFullName));
                // We must ensure the scheduler complete and then can handle pipeline state change.
                if (jobMaster.getScheduleFuture() != null) {
                    jobMaster.getScheduleFuture().join();
                }

                if (reSchedulerPipelineFuture != null) {
                    reSchedulerPipelineFuture.join();
                }
                reset();
                jobMaster.getPhysicalPlan().addPipelineEndCallback(this);
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
        synchronized (restoreLock) {
            try {
                if (jobMaster.getCheckpointManager().isCompletedPipeline(pipelineId)) {
                    forcePipelineFinish();
                }
                jobMaster.getCheckpointManager().reportedPipelineRunning(pipelineId, false);
                reSchedulerPipelineFuture = jobMaster.reSchedulerPipeline(this);
                if (reSchedulerPipelineFuture != null) {
                    reSchedulerPipelineFuture.join();
                }
            } catch (Throwable e) {
                LOGGER.severe(
                        String.format(
                                "Restore pipeline %s error with exception: ", pipelineFullName),
                        e);
                cancelPipeline();
            }
        }
    }

    /** If the job state in CheckpointManager is complete, we need force this pipeline finish */
    private void forcePipelineFinish() {
        coordinatorVertexList.forEach(
                coordinator ->
                        coordinator.updateTaskExecutionState(
                                new TaskExecutionState(
                                        coordinator.getTaskGroupLocation(),
                                        ExecutionState.FINISHED)));
        physicalVertexList.forEach(
                task ->
                        task.updateTaskExecutionState(
                                new TaskExecutionState(
                                        task.getTaskGroupLocation(), ExecutionState.FINISHED)));
    }

    /** restore the pipeline state after new Master Node active */
    public synchronized void restorePipelineState() {
        // if PipelineStatus is less than RUNNING or equals CANCELING, may some task is in state
        // CREATED, we can not schedule this tasks because have no PipelineBaseScheduler instance.
        // So, we need cancel the pipeline and restore it.
        if (getPipelineState().ordinal() < PipelineStatus.RUNNING.ordinal()) {
            cancelPipelineTasks();
        } else if (PipelineStatus.CANCELING.equals(getPipelineState())) {
            cancelPipelineTasks();
        } else if (PipelineStatus.RUNNING.equals(getPipelineState())) {
            jobMaster
                    .getCheckpointManager()
                    .reportedPipelineRunning(this.getPipelineLocation().getPipelineId(), true);
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
        LOGGER.warning(
                String.format(
                        "%s checkpoint have error, cancel the pipeline", getPipelineFullName()));
        this.cancelPipeline();
    }
}
