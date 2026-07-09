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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.common.metrics.MetricTags;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.tracing.MDCTracer;
import org.apache.seatunnel.common.utils.function.ConsumerWithException;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.InternalCheckpointListener;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.ActionSubtaskState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointBarrier;
import org.apache.seatunnel.engine.server.checkpoint.operation.TaskAcknowledgeOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeAfterCheckpointOperation;
import org.apache.seatunnel.engine.server.checkpoint.operation.TriggerSchemaChangeBeforeCheckpointOperation;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SinkConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.UnknownFlowException;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.observability.ObservabilityConfig;
import org.apache.seatunnel.engine.server.task.flow.ActionFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.OneInputFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SinkFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.SourceFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.flow.TransformFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.group.AbstractTaskGroupWithIntermediateQueue;
import org.apache.seatunnel.engine.server.task.record.Barrier;
import org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneaky;
import static org.apache.seatunnel.engine.common.utils.ExceptionUtil.sneakyThrow;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CANCELED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.CLOSED;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.PREPARE_CLOSE;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.READY_START;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.RUNNING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.STARTING;
import static org.apache.seatunnel.engine.server.task.statemachine.SeaTunnelTaskState.WAITING_RESTORE;

/**
 * Abstract base class for all Zeta engine task executions.
 *
 * <p>A {@code SeaTunnelTask} drives the lifecycle of a single pipeline subtask. It holds the
 * execution DAG as a {@link Flow} graph, converts that graph into a chain of {@link FlowLifeCycle}
 * objects during {@link #init()}, and then repeatedly calls {@link #stateProcess()} to advance
 * through the task state machine:
 *
 * <pre>
 *   CREATED → INIT → WAITING_RESTORE → READY_START → STARTING → RUNNING → PREPARE_CLOSE → CLOSED
 * </pre>
 *
 * <p>Checkpoint coordination is handled by accumulating per-cycle ACKs via {@link #ack(Barrier)}
 * and buffering per-action state snapshots via {@link #addState(Barrier, ActionStateKey, List)}
 * before sending a single {@link TaskAcknowledgeOperation} to the {@code CheckpointCoordinator}.
 *
 * <p>Subclasses must implement {@link #collect()} (the main data-reading loop) and {@link
 * #createSourceFlowLifeCycle} (factory for the source-specific lifecycle).
 */
@Slf4j
public abstract class SeaTunnelTask extends AbstractTask {
    private static final long serialVersionUID = 2604309561613784425L;

    protected volatile SeaTunnelTaskState currState;
    private final Flow executionFlow;

    protected FlowLifeCycle startFlowLifeCycle;

    protected List<FlowLifeCycle> allCycles;

    protected List<OneInputFlowLifeCycle<Record<?>>> outputs;

    protected List<CompletableFuture<Void>> flowFutures;

    protected final Map<Long, List<ActionSubtaskState>> checkpointStates =
            new ConcurrentHashMap<>();

    private final Map<Long, Integer> cycleAcks = new ConcurrentHashMap<>();

    protected int indexID;

    private TaskGroup taskBelongGroup;

    private SeaTunnelMetricsContext metricsContext;

    private transient boolean observabilityEnabled;

    public SeaTunnelTask(long jobID, TaskLocation taskID, int indexID, Flow executionFlow) {
        super(jobID, taskID);
        this.indexID = indexID;
        this.executionFlow = executionFlow;
        this.currState = SeaTunnelTaskState.CREATED;
    }

    /**
     * Initializes the task by converting the execution {@link Flow} DAG into a chain of {@link
     * FlowLifeCycle} objects.
     *
     * <p>Specifically this method:
     *
     * <ol>
     *   <li>Creates a {@link SeaTunnelMetricsContext} for this task's metrics reporting.
     *   <li>Recursively traverses the {@code executionFlow} graph via {@link
     *       #convertFlowToActionLifeCycle(Flow)}, producing one {@link FlowLifeCycle} per node and
     *       wiring their output lists together.
     *   <li>Calls {@link FlowLifeCycle#init()} on every lifecycle in the chain.
     *   <li>Registers a composite future over all {@code flowFutures} so that {@code closeCalled}
     *       is set to {@code true} when every flow in the chain has completed.
     * </ol>
     *
     * @throws Exception if flow conversion or any lifecycle init fails
     */
    @Override
    public void init() throws Exception {
        super.init();
        metricsContext = getExecutionContext().getOrCreateMetricsContext(taskLocation);
        observabilityEnabled = resolveObservabilityEnabled();
        this.currState = SeaTunnelTaskState.INIT;
        flowFutures = new ArrayList<>();
        allCycles = new ArrayList<>();
        startFlowLifeCycle = convertFlowToActionLifeCycle(executionFlow);
        for (FlowLifeCycle cycle : allCycles) {
            cycle.init();
        }
        CompletableFuture.allOf(flowFutures.toArray(new CompletableFuture[0]))
                .whenComplete((s, e) -> closeCalled = true);
    }

    public boolean isObservabilityEnabled() {
        return observabilityEnabled;
    }

    private boolean resolveObservabilityEnabled() {
        try {
            if (executionContext == null) {
                return false;
            }
            if (executionContext.getTaskExecutionService() == null) {
                return false;
            }
            NodeEngineImpl nodeEngine = executionContext.getTaskExecutionService().getNodeEngine();
            if (nodeEngine == null) {
                return false;
            }
            IMap<Long, JobInfo> jobInfoMap =
                    nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
            JobInfo jobInfo = jobInfoMap.get(jobID);
            if (jobInfo == null || jobInfo.getJobImmutableInformation() == null) {
                return false;
            }
            JobImmutableInformation immutable =
                    nodeEngine
                            .getSerializationService()
                            .toObject(jobInfo.getJobImmutableInformation());
            if (immutable == null || immutable.getJobConfig() == null) {
                return false;
            }
            Map<String, Object> envOptions = immutable.getJobConfig().getEnvOptions();
            return ObservabilityConfig.resolveEnabled(envOptions);
        } catch (Throwable t) {
            log.debug(
                    "Resolve observability enabled failed, jobId={}, taskLocation={}, err={}",
                    jobID,
                    taskLocation,
                    t.getMessage());
            return false;
        }
    }

    /**
     * Advances the task through its state machine. Called repeatedly by the task execution loop.
     *
     * <p>State transitions:
     *
     * <ul>
     *   <li><b>INIT → WAITING_RESTORE</b>: Reports status and waits for {@code restoreComplete}.
     *   <li><b>WAITING_RESTORE → READY_START</b>: Once restore is done, opens all {@link
     *       FlowLifeCycle} instances and waits for the external start signal.
     *   <li><b>READY_START → STARTING → RUNNING</b>: Triggered when {@code startCalled} is set.
     *   <li><b>RUNNING</b>: Calls {@link #collect()} to read/process data. Transitions to {@code
     *       PREPARE_CLOSE} when {@code prepareCloseStatus} is set by a barrier.
     *   <li><b>PREPARE_CLOSE → CLOSED</b>: Waits for all flows to complete ({@code closeCalled}),
     *       then calls {@link #close()} and marks the task progress as done.
     *   <li><b>CANCELLING → CANCELED</b>: External cancellation path; closes and marks done.
     * </ul>
     *
     * @throws Exception if any state transition or the {@link #collect()} call fails
     */
    protected void stateProcess() throws Exception {
        switch (currState) {
            case INIT:
                currState = WAITING_RESTORE;
                reportTaskStatus(WAITING_RESTORE);
                break;
            case WAITING_RESTORE:
                if (restoreComplete.isDone()) {
                    for (FlowLifeCycle cycle : allCycles) {
                        cycle.open();
                    }
                    currState = READY_START;
                    reportTaskStatus(READY_START);
                } else {
                    Thread.sleep(100);
                }
                break;
            case READY_START:
                if (startCalled) {
                    currState = STARTING;
                } else {
                    Thread.sleep(100);
                }
                break;
            case STARTING:
                currState = RUNNING;
                break;
            case RUNNING:
                collect();
                if (prepareCloseStatus) {
                    currState = PREPARE_CLOSE;
                }
                break;
            case PREPARE_CLOSE:
                if (closeCalled) {
                    currState = CLOSED;
                } else {
                    Thread.sleep(100);
                }
                break;
            case CLOSED:
                this.close();
                progress.done();
                return;
                // TODO support cancel by outside
            case CANCELLING:
                this.close();
                currState = CANCELED;
                progress.done();
                return;
            default:
                throw new IllegalArgumentException("Unknown Enumerator State: " + currState);
        }
    }

    public void setTaskGroup(TaskGroup group) {
        this.taskBelongGroup = group;
    }

    /**
     * Recursively converts a {@link Flow} DAG into a chain of {@link FlowLifeCycle} objects.
     *
     * <p>For each node in the graph this method:
     *
     * <ol>
     *   <li>Recurses into {@code flow.getNext()} to build downstream lifecycles first.
     *   <li>Creates a {@link CompletableFuture} and registers it in {@code flowFutures} for
     *       close-detection.
     *   <li>Instantiates the appropriate lifecycle based on the flow/action type:
     *       <ul>
     *         <li>{@link SourceAction} → {@link SourceFlowLifeCycle} (via subclass factory)
     *         <li>{@link SinkAction} → {@link SinkFlowLifeCycle}
     *         <li>{@link TransformChainAction} → {@link TransformFlowLifeCycle}
     *         <li>{@link IntermediateExecutionFlow} → {@link IntermediateQueueFlowLifeCycle}
     *       </ul>
     *   <li>Wires the downstream lifecycles as the outputs of the newly created lifecycle.
     * </ol>
     *
     * @param flow the root (or sub-root) of the DAG to convert
     * @return the lifecycle corresponding to {@code flow}
     * @throws Exception if action type is unknown or lifecycle creation fails
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private FlowLifeCycle convertFlowToActionLifeCycle(@NonNull Flow flow) throws Exception {

        FlowLifeCycle lifeCycle;
        List<OneInputFlowLifeCycle<Record<?>>> flowLifeCycles = new ArrayList<>();
        if (!flow.getNext().isEmpty()) {
            for (Flow f : flow.getNext()) {
                flowLifeCycles.add(
                        (OneInputFlowLifeCycle<Record<?>>) convertFlowToActionLifeCycle(f));
            }
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        flowFutures.add(completableFuture);
        if (flow instanceof PhysicalExecutionFlow) {
            PhysicalExecutionFlow f = (PhysicalExecutionFlow) flow;
            if (f.getAction() instanceof SourceAction) {
                lifeCycle =
                        createSourceFlowLifeCycle(
                                (SourceAction<?, ?, ?>) f.getAction(),
                                (SourceConfig) f.getConfig(),
                                completableFuture,
                                this.getMetricsContext());
                outputs = flowLifeCycles;
            } else if (f.getAction() instanceof SinkAction) {
                lifeCycle =
                        new SinkFlowLifeCycle<>(
                                (SinkAction) f.getAction(),
                                taskLocation,
                                indexID,
                                this,
                                ((SinkConfig) f.getConfig()).getCommitterTask(),
                                ((SinkConfig) f.getConfig()).isContainCommitter(),
                                completableFuture,
                                this.getMetricsContext());
            } else if (f.getAction() instanceof TransformChainAction) {
                lifeCycle =
                        new TransformFlowLifeCycle<SeaTunnelRow>(
                                (TransformChainAction) f.getAction(),
                                this,
                                new SeaTunnelTransformCollector(flowLifeCycles),
                                completableFuture);
            } else {
                throw new UnknownActionException(f.getAction());
            }
        } else if (flow instanceof IntermediateExecutionFlow) {
            IntermediateQueueConfig config =
                    ((IntermediateExecutionFlow<IntermediateQueueConfig>) flow).getConfig();
            lifeCycle =
                    new IntermediateQueueFlowLifeCycle(
                            this,
                            completableFuture,
                            ((AbstractTaskGroupWithIntermediateQueue) taskBelongGroup)
                                    .getQueueCache(
                                            config.getQueueID(),
                                            config.getCapacity(),
                                            this.getMetricsContext()));
            outputs = flowLifeCycles;
        } else {
            throw new UnknownFlowException(flow);
        }
        allCycles.add(lifeCycle);
        return lifeCycle;
    }

    protected abstract SourceFlowLifeCycle<?, ?> createSourceFlowLifeCycle(
            SourceAction<?, ?, ?> sourceAction,
            SourceConfig config,
            CompletableFuture<Void> completableFuture,
            MetricsContext metricsContext);

    protected abstract void collect() throws Exception;

    @Override
    public Set<URL> getJarsUrl() {
        return getFlowInfo((action, set) -> set.addAll(action.getJarUrls()));
    }

    @Override
    public Set<ConnectorJarIdentifier> getConnectorPluginJars() {
        return getFlowInfo((action, set) -> set.addAll(action.getConnectorJarIdentifiers()));
    }

    public Set<ActionStateKey> getActionStateKeys() {
        return getFlowInfo((action, set) -> set.add(ActionStateKey.of(action)));
    }

    private <T> Set<T> getFlowInfo(BiConsumer<Action, Set<T>> function) {
        List<Flow> now = new ArrayList<>();
        now.add(executionFlow);
        Set<T> result = new HashSet<>();
        while (!now.isEmpty()) {
            final List<Flow> next = new ArrayList<>();
            now.forEach(
                    n -> {
                        if (n instanceof PhysicalExecutionFlow) {
                            function.accept(((PhysicalExecutionFlow) n).getAction(), result);
                        }
                        next.addAll(n.getNext());
                    });
            now.clear();
            now.addAll(next);
        }
        return result;
    }

    /**
     * Performs an ordered teardown of all {@link FlowLifeCycle} objects in this task.
     *
     * <p>Each lifecycle's {@link FlowLifeCycle#close()} is called in iteration order. If any
     * lifecycle throws an {@link IOException}, the error is logged but does not prevent the
     * remaining lifecycles from being closed (first-exception-wins logging).
     *
     * @throws IOException if the parent {@link AbstractTask#close()} fails
     */
    @Override
    public void close() throws IOException {
        super.close();
        MDCTracer.tracing(allCycles.stream())
                .forEach(
                        flowLifeCycle -> {
                            try {
                                flowLifeCycle.close();
                            } catch (IOException e) {
                                log.error("Close FlowLifeCycle error.", e);
                            }
                        });
    }

    /**
     * Accumulates a per-cycle checkpoint ACK for the given barrier.
     *
     * <p>Each {@link FlowLifeCycle} in the chain calls this method when it has finished processing
     * a barrier. Once every cycle has ACKed (i.e. {@code ackSize == allCycles.size()}):
     *
     * <ol>
     *   <li>If the barrier carries a {@code prepareClose} signal for this task, {@code
     *       prepareCloseStatus} is set to {@code true} to trigger the {@code RUNNING →
     *       PREPARE_CLOSE} transition.
     *   <li>If the barrier is a snapshot barrier, a {@link TaskAcknowledgeOperation} containing all
     *       buffered {@link ActionSubtaskState}s is sent to the {@code CheckpointCoordinator} on
     *       the master node.
     * </ol>
     *
     * @param barrier the checkpoint or prepare-close barrier being acknowledged
     */
    public void ack(Barrier barrier) {
        log.debug("seatunnel task ack barrier[{}]", this.taskLocation);
        Integer ackSize =
                cycleAcks.compute(barrier.getId(), (id, count) -> count == null ? 1 : ++count);
        if (ackSize == allCycles.size()) {
            cycleAcks.remove(barrier.getId());
            if (barrier.prepareClose(this.taskLocation)) {
                this.prepareCloseStatus = true;
                this.prepareCloseBarrierId.set(barrier.getId());
            }
            if (barrier.snapshot()) {
                this.getExecutionContext()
                        .sendToMaster(
                                new TaskAcknowledgeOperation(
                                        this.taskLocation,
                                        (CheckpointBarrier) barrier,
                                        checkpointStates.remove(barrier.getId())))
                        .join();
            }
        }
    }

    /**
     * Sends a {@link TriggerSchemaChangeBeforeCheckpointOperation} to the master node.
     *
     * <p>This propagates a DDL-before-checkpoint barrier to the upstream enumerator, signalling
     * that a schema change must be applied before the next checkpoint can proceed.
     *
     * @return a future that completes when the master acknowledges the operation
     */
    public InvocationFuture<Object> triggerSchemaChangeBeforeCheckpoint() {
        log.info(
                "trigger schema-change-before checkpoint. jobID[{}], taskLocation[{}]",
                jobID,
                taskLocation);
        return this.getExecutionContext()
                .sendToMaster(new TriggerSchemaChangeBeforeCheckpointOperation(taskLocation));
    }

    /**
     * Sends a {@link TriggerSchemaChangeAfterCheckpointOperation} to the master node.
     *
     * <p>This propagates a DDL-after-checkpoint barrier signalling that the schema change has been
     * committed and downstream tasks can proceed with the new schema.
     *
     * @return a future that completes when the master acknowledges the operation
     */
    public InvocationFuture<Object> triggerSchemaChangeAfterCheckpoint() {
        log.info(
                "trigger schema-change-after checkpoint. jobID[{}], taskLocation[{}]",
                jobID,
                taskLocation);
        return this.getExecutionContext()
                .sendToMaster(new TriggerSchemaChangeAfterCheckpointOperation(taskLocation));
    }

    /**
     * Buffers a per-action checkpoint state snapshot for the given barrier.
     *
     * <p>Each action in the task chain serializes its state as a list of byte arrays and registers
     * it here. The accumulated states are later sent to the {@code CheckpointCoordinator} when all
     * cycles have ACKed via {@link #ack(Barrier)}.
     *
     * @param barrier the checkpoint barrier this state belongs to
     * @param stateKey identifies the action that produced the state
     * @param state the serialized action state as a list of byte arrays
     */
    public void addState(Barrier barrier, ActionStateKey stateKey, List<byte[]> state) {
        List<ActionSubtaskState> states =
                checkpointStates.computeIfAbsent(barrier.getId(), id -> new ArrayList<>());
        states.add(new ActionSubtaskState(stateKey, indexID, state));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        notifyAllAction(listener -> listener.notifyCheckpointComplete(checkpointId));
        tryClose(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        notifyAllAction(listener -> listener.notifyCheckpointAborted(checkpointId));
        tryClose(checkpointId);
    }

    @Override
    public void notifyCheckpointEnd(long checkpointId) throws Exception {
        notifyAllAction(listener -> listener.notifyCheckpointEnd(checkpointId));
        tryClose(checkpointId);
    }

    public void notifyAllAction(ConsumerWithException<InternalCheckpointListener> consumer) {
        allCycles.stream()
                .filter(cycle -> cycle instanceof InternalCheckpointListener)
                .map(cycle -> (InternalCheckpointListener) cycle)
                .forEach(listener -> sneaky(consumer, listener));
    }

    @Override
    public void restoreState(List<ActionSubtaskState> actionStateList) throws Exception {
        log.debug("restoreState for SeaTunnelTask[{}]", actionStateList);
        if (null == actionStateList) {
            log.debug("restoreState is null, do nothing!");
            return;
        }
        Map<ActionStateKey, List<ActionSubtaskState>> stateMap =
                actionStateList.stream()
                        .collect(
                                Collectors.groupingBy(
                                        ActionSubtaskState::getStateKey, Collectors.toList()));
        allCycles.stream()
                .filter(cycle -> cycle instanceof ActionFlowLifeCycle)
                .map(cycle -> (ActionFlowLifeCycle) cycle)
                .forEach(
                        actionFlowLifeCycle -> {
                            try {
                                actionFlowLifeCycle.restoreState(
                                        stateMap.getOrDefault(
                                                ActionStateKey.of(actionFlowLifeCycle.getAction()),
                                                Collections.emptyList()));
                            } catch (Exception e) {
                                sneakyThrow(e);
                            }
                        });
        restoreComplete.complete(null);
        log.debug("restoreState for SeaTunnelTask finished, actionStateList: {}", actionStateList);
    }

    @Override
    public SeaTunnelMetricsContext getMetricsContext() {
        return metricsContext;
    }

    @Override
    public void provideDynamicMetrics(
            MetricDescriptor descriptor, MetricsCollectionContext context) {
        if (null != metricsContext) {
            metricsContext.provideDynamicMetrics(
                    descriptor
                            .copy()
                            .withTag(MetricTags.TASK_NAME, this.getClass().getSimpleName()),
                    context);
        }
    }
}
