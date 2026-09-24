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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.checkpoint.operation.CheckpointBarrierTriggerOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskReportStatusOperation;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A real {@link CheckpointManager} whose tasks are fakes answered at the network boundary.
 *
 * <p>Everything on the coordinator side is production code: the coordinators, their scheduling,
 * checkpoint id allocation, the IMap state and the checkpoint storage. Only the messages that would
 * travel to a task are intercepted, and the fake answers exactly two of them:
 *
 * <ul>
 *   <li>{@link #startTask}: the task reports {@code READY_START}, which is what makes the
 *       coordinator arm its periodic trigger.
 *   <li>{@link #sendOperationToMemberNode} with a {@link CheckpointBarrierTriggerOperation}: every
 *       task of the pipeline acknowledges the barrier at once, with no state.
 * </ul>
 *
 * <p>Every other operation is answered with an empty success. If the task protocol changes, for
 * example a new message the coordinator waits on before arming the trigger or before completing a
 * checkpoint, this class is what needs updating, and the fixture then fails at setup rather than
 * reporting numbers from a coordinator that never triggers.
 */
final class FakeTaskCheckpointManager extends CheckpointManager {

    /**
     * {@code CheckpointBarrierTriggerOperation} has no getter for its barrier, and the barrier is
     * needed to acknowledge it. Read-only; if the field is renamed or removed, loading this class
     * fails with a message naming it instead of the benchmark silently never completing a
     * checkpoint.
     */
    private static final Field BARRIER_FIELD =
            BenchmarkReflection.requireField(CheckpointBarrierTriggerOperation.class, "barrier");

    private final long jobId;
    private final NodeEngine nodeEngine;
    private final CheckpointPlan plan;
    private final AtomicReference<Throwable> failure;

    FakeTaskCheckpointManager(
            long jobId,
            NodeEngine nodeEngine,
            CheckpointPlan plan,
            CheckpointConfig checkpointConfig,
            CheckpointStorage checkpointStorage,
            ExecutorService executorService,
            IMap<Object, Object> runningJobStateIMap,
            SeaTunnelEngineContext engineContext,
            CheckpointMonitorService checkpointMonitorService,
            AtomicReference<Throwable> failure) {
        super(
                jobId,
                false,
                null,
                null,
                nodeEngine,
                null,
                Collections.singletonMap(plan.getPipelineId(), plan),
                checkpointConfig,
                checkpointStorage,
                executorService,
                runningJobStateIMap,
                engineContext,
                checkpointMonitorService);
        this.jobId = jobId;
        this.nodeEngine = nodeEngine;
        this.plan = plan;
        this.failure = failure;
    }

    /**
     * Starts the pipeline the way a deployed job does: the pipeline is reported running, then every
     * task reports {@code READY_START}. The coordinator arms its periodic trigger one checkpoint
     * interval after the last report.
     */
    void startTask() {
        reportedPipelineRunning(plan.getPipelineId(), false);
        for (TaskLocation task : plan.getPipelineSubtasks()) {
            reportedTask(new TaskReportStatusOperation(task, SeaTunnelTaskState.READY_START));
        }
    }

    @Override
    protected InvocationFuture<?> sendOperationToMemberNode(TaskOperation operation) {
        if (operation instanceof CheckpointBarrierTriggerOperation) {
            acknowledgeBarrier(readBarrier((CheckpointBarrierTriggerOperation) operation));
        }
        return NodeEngineUtil.sendOperationToMemberNode(
                nodeEngine, new NoOpOperation(), nodeEngine.getThisAddress());
    }

    /** Records the failure instead of reaching the absent {@code JobMaster}. */
    @Override
    protected void handleCheckpointError(int pipelineId, boolean neverRestore) {
        failure.compareAndSet(
                null,
                new IllegalStateException(
                        "Checkpoint coordinator of job " + jobId + " reported an error"));
    }

    private void acknowledgeBarrier(CheckpointBarrier barrier) {
        for (TaskLocation task : plan.getPipelineSubtasks()) {
            acknowledgeTask(new TaskAcknowledgeOperation(task, barrier, Collections.emptyList()));
        }
    }

    private static CheckpointBarrier readBarrier(CheckpointBarrierTriggerOperation operation) {
        try {
            return (CheckpointBarrier) BARRIER_FIELD.get(operation);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read the checkpoint barrier", e);
        }
    }

    /** Completes on the local member without doing anything, standing in for a task's reply. */
    private static final class NoOpOperation extends Operation {
        @Override
        public void run() {}
    }
}
