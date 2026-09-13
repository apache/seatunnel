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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.engine.common.config.server.QueueType;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupCoordinatorAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainConfig;
import org.apache.seatunnel.engine.core.dag.internal.IntermediateQueue;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.checkpoint.CoordinatorStateKey;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionEdge;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
import org.apache.seatunnel.engine.server.dag.execution.PortAwareExecutionEdge;
import org.apache.seatunnel.engine.server.dag.physical.config.DynamicLookupConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.FlowConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.IntermediateQueueConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SinkConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.UnknownFlowException;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.observability.ObservabilityConfig;
import org.apache.seatunnel.engine.server.task.DynamicLookupCoordinatorTask;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.TransformSeaTunnelTask;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateBlockingQueue;
import org.apache.seatunnel.engine.server.task.group.TaskGroupWithIntermediateDisruptor;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.engine.common.config.server.QueueType.BLOCKINGQUEUE;

public class PhysicalPlanGenerator {

    private final List<Pipeline> pipelines;

    private final IdGenerator taskGroupIdGenerator = new IdGenerator();

    private final JobImmutableInformation jobImmutableInformation;

    private final long initializationTimestamp;

    private final ExecutorService executorService;

    private final ClassLoaderService classLoaderService;

    private final NodeEngine nodeEngine;

    private final FlakeIdGenerator flakeIdGenerator;

    /** Save the enumerator task ID corresponding to source */
    private final Map<SourceAction<?, ?, ?>, TaskLocation> enumeratorTaskIDMap = new HashMap<>();
    /** Save the committer task ID corresponding to sink */
    private final Map<SinkAction<?, ?, ?, ?>, TaskLocation> committerTaskIDMap = new HashMap<>();

    /** All task locations of the pipeline. */
    private final Set<TaskLocation> pipelineTasks;

    /** All starting task ids of a pipeline. */
    private final Set<TaskLocation> startingTasks;

    /**
     * <br>
     * key: the subtask locations; <br>
     * value: all actions in this subtask; f0: action state key, f1: action index;
     */
    private final Map<TaskLocation, Set<Tuple2<ActionStateKey, Integer>>> subtaskActions;

    /** Reader task locations indexed by source action ID and source subtask. */
    private final Map<Long, Map<Integer, TaskLocation>> sourceTaskLocations;

    /** Coordinator tasks that receive checkpoint triggers without becoming source roots. */
    private final Set<TaskLocation> coordinatorCheckpointRoots;

    /** Operator-scoped coordinator checkpoint identities. */
    private final Map<CoordinatorStateKey, TaskLocation> dynamicLookupCoordinatorTasks;

    /** Port topology retained in the checkpoint plan for each multi-input task. */
    private final Map<TaskLocation, List<InputPortDescriptor>> checkpointInputPorts;

    /** Coordinator actions that are not ordinary execution vertices. */
    private final Map<ActionStateKey, Integer> coordinatorPipelineActions;

    private final IMap<Object, Object> runningJobStateIMap;

    private final IMap<Object, Object> runningJobStateTimestampsIMap;

    private final QueueType queueType;

    private final ObservabilityConfig observabilityConfig;

    public PhysicalPlanGenerator(
            @NonNull ExecutionPlan executionPlan,
            @NonNull NodeEngine nodeEngine,
            @NonNull JobImmutableInformation jobImmutableInformation,
            long initializationTimestamp,
            @NonNull ExecutorService executorService,
            @NonNull ClassLoaderService classLoaderService,
            @NonNull FlakeIdGenerator flakeIdGenerator,
            @NonNull IMap runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap,
            @NonNull QueueType queueType) {
        this.pipelines = executionPlan.getPipelines();
        this.nodeEngine = nodeEngine;
        this.jobImmutableInformation = jobImmutableInformation;
        this.initializationTimestamp = initializationTimestamp;
        this.executorService = executorService;
        this.classLoaderService = classLoaderService;
        this.flakeIdGenerator = flakeIdGenerator;
        // the checkpoint of a pipeline
        this.pipelineTasks = new HashSet<>();
        this.startingTasks = new HashSet<>();
        this.subtaskActions = new HashMap<>();
        this.sourceTaskLocations = new HashMap<>();
        this.coordinatorCheckpointRoots = new HashSet<>();
        this.dynamicLookupCoordinatorTasks = new HashMap<>();
        this.checkpointInputPorts = new HashMap<>();
        this.coordinatorPipelineActions = new HashMap<>();
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.queueType = queueType;
        this.observabilityConfig =
                ObservabilityConfig.fromEnvOptions(
                        jobImmutableInformation.getJobConfig().getEnvOptions());
    }

    public Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> generate() {
        Map<String, String> tagFilter =
                (Map<String, String>)
                        jobImmutableInformation
                                .getJobConfig()
                                .getEnvOptions()
                                .get(EnvCommonOptions.NODE_TAG_FILTER.key());
        CopyOnWriteArrayList<PassiveCompletableFuture<PipelineStatus>>
                waitForCompleteBySubPlanList = new CopyOnWriteArrayList<>();

        List<Pipeline> unclosedPipelines = new ArrayList<>();
        for (Pipeline pipeline : this.pipelines) {
            PipelineLocation pipelineLocation =
                    new PipelineLocation(jobImmutableInformation.getJobId(), pipeline.getId());
            PipelineStatus pipelineStatus =
                    (PipelineStatus) runningJobStateIMap.get(pipelineLocation);
            if (jobImmutableInformation.isRestoreJob()
                    || !PipelineStatus.FINISHED.equals(pipelineStatus)) {
                unclosedPipelines.add(pipeline);
            }
        }

        Map<Integer, CheckpointPlan> checkpointPlans = new HashMap<>();
        final int totalPipelineNum = unclosedPipelines.size();
        Stream<SubPlan> subPlanStream =
                unclosedPipelines.stream()
                        .map(
                                pipeline -> {
                                    this.pipelineTasks.clear();
                                    this.startingTasks.clear();
                                    this.subtaskActions.clear();
                                    this.sourceTaskLocations.clear();
                                    this.coordinatorCheckpointRoots.clear();
                                    this.dynamicLookupCoordinatorTasks.clear();
                                    this.checkpointInputPorts.clear();
                                    this.coordinatorPipelineActions.clear();
                                    final int pipelineId = pipeline.getId();
                                    final List<ExecutionEdge> edges = pipeline.getEdges();
                                    validateDynamicLookupPipeline(pipeline);

                                    List<SourceAction<?, ?, ?>> sources = findSourceAction(edges);

                                    List<PhysicalVertex> coordinatorVertexList =
                                            getEnumeratorTask(
                                                    sources, pipelineId, totalPipelineNum);
                                    coordinatorVertexList.addAll(
                                            getCommitterTask(edges, pipelineId, totalPipelineNum));
                                    coordinatorVertexList.addAll(
                                            getDynamicLookupCoordinatorTask(
                                                    pipeline, pipelineId, totalPipelineNum));

                                    List<PhysicalVertex> physicalVertexList =
                                            containsDynamicLookup(pipeline)
                                                    ? getDynamicLookupRuntimeTask(
                                                            pipeline, pipelineId, totalPipelineNum)
                                                    : getSourceTask(
                                                            edges,
                                                            sources,
                                                            pipelineId,
                                                            totalPipelineNum);

                                    CompletableFuture<PipelineStatus> pipelineFuture =
                                            new CompletableFuture<>();
                                    waitForCompleteBySubPlanList.add(
                                            new PassiveCompletableFuture<>(pipelineFuture));

                                    Map<ActionStateKey, Integer> pipelineActions =
                                            new HashMap<>(pipeline.getActions());
                                    pipelineActions.putAll(coordinatorPipelineActions);
                                    checkpointPlans.put(
                                            pipelineId,
                                            CheckpointPlan.builder()
                                                    .pipelineId(pipelineId)
                                                    .pipelineSubtasks(pipelineTasks)
                                                    .startingSubtasks(startingTasks)
                                                    .coordinatorCheckpointRoots(
                                                            coordinatorCheckpointRoots)
                                                    .pipelineActions(pipelineActions)
                                                    .subtaskActions(subtaskActions)
                                                    .coordinatorTasks(dynamicLookupCoordinatorTasks)
                                                    .inputPortsByTask(checkpointInputPorts)
                                                    .build());
                                    return new SubPlan(
                                            pipelineId,
                                            totalPipelineNum,
                                            initializationTimestamp,
                                            physicalVertexList,
                                            coordinatorVertexList,
                                            jobImmutableInformation,
                                            executorService,
                                            runningJobStateIMap,
                                            runningJobStateTimestampsIMap,
                                            tagFilter);
                                });

        PhysicalPlan physicalPlan =
                new PhysicalPlan(
                        subPlanStream.collect(Collectors.toList()),
                        executorService,
                        jobImmutableInformation,
                        initializationTimestamp,
                        runningJobStateIMap,
                        runningJobStateTimestampsIMap);
        return Tuple2.tuple2(physicalPlan, checkpointPlans);
    }

    private List<SourceAction<?, ?, ?>> findSourceAction(List<ExecutionEdge> edges) {
        return edges.stream()
                .filter(s -> s.getLeftVertex().getAction() instanceof SourceAction)
                .map(s -> (SourceAction<?, ?, ?>) s.getLeftVertex().getAction())
                .distinct()
                .collect(Collectors.toList());
    }

    private static boolean containsDynamicLookup(Pipeline pipeline) {
        return pipeline.getVertexes().values().stream()
                .map(ExecutionVertex::getAction)
                .anyMatch(DynamicLookupAction.class::isInstance);
    }

    private List<PhysicalVertex> getCommitterTask(
            List<ExecutionEdge> edges, int pipelineIndex, int totalPipelineNum) {
        AtomicInteger atomicInteger = new AtomicInteger(-1);
        List<ExecutionEdge> collect =
                edges.stream()
                        .filter(s -> s.getRightVertex().getAction() instanceof SinkAction)
                        .collect(Collectors.toList());

        return collect.stream()
                .map(s -> (SinkAction<?, ?, ?, ?>) s.getRightVertex().getAction())
                .map(
                        sinkAction -> {
                            Optional<? extends SinkAggregatedCommitter<?, ?>>
                                    sinkAggregatedCommitter;
                            ClassLoader appClassLoader =
                                    Thread.currentThread().getContextClassLoader();
                            try {
                                ClassLoader classLoader =
                                        classLoaderService.getClassLoader(
                                                jobImmutableInformation.getJobId(),
                                                sinkAction.getJarUrls());
                                Thread.currentThread().setContextClassLoader(classLoader);
                                sinkAggregatedCommitter =
                                        sinkAction.getSink().createAggregatedCommitter();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } finally {
                                Thread.currentThread().setContextClassLoader(appClassLoader);
                                classLoaderService.releaseClassLoader(
                                        jobImmutableInformation.getJobId(),
                                        sinkAction.getJarUrls());
                            }
                            // if sinkAggregatedCommitter is empty, don't create task.
                            if (sinkAggregatedCommitter.isPresent()) {
                                long taskGroupID = taskGroupIdGenerator.getNextId();
                                TaskGroupLocation taskGroupLocation =
                                        new TaskGroupLocation(
                                                jobImmutableInformation.getJobId(),
                                                pipelineIndex,
                                                taskGroupID);
                                TaskLocation taskLocation =
                                        new TaskLocation(taskGroupLocation, 0, 0);
                                SinkAggregatedCommitterTask<?, ?> t =
                                        new SinkAggregatedCommitterTask(
                                                jobImmutableInformation.getJobId(),
                                                taskLocation,
                                                sinkAction,
                                                sinkAggregatedCommitter.get());
                                committerTaskIDMap.put(sinkAction, taskLocation);

                                // checkpoint
                                pipelineTasks.add(taskLocation);
                                subtaskActions.put(
                                        taskLocation,
                                        Collections.singleton(
                                                Tuple2.tuple2(ActionStateKey.of(sinkAction), -1)));

                                return new PhysicalVertex(
                                        atomicInteger.incrementAndGet(),
                                        collect.size(),
                                        new TaskGroupDefaultImpl(
                                                taskGroupLocation,
                                                sinkAction.getName() + "-AggregatedCommitterTask",
                                                Lists.newArrayList(t)),
                                        flakeIdGenerator,
                                        pipelineIndex,
                                        totalPipelineNum,
                                        Collections.singletonList(sinkAction.getJarUrls()),
                                        Collections.singletonList(
                                                sinkAction.getConnectorJarIdentifiers()),
                                        jobImmutableInformation,
                                        initializationTimestamp,
                                        nodeEngine,
                                        runningJobStateIMap,
                                        runningJobStateTimestampsIMap);
                            } else {
                                return null;
                            }
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<PhysicalVertex> getEnumeratorTask(
            List<SourceAction<?, ?, ?>> sources, int pipelineIndex, int totalPipelineNum) {
        AtomicInteger atomicInteger = new AtomicInteger(-1);

        return sources.stream()
                .map(
                        sourceAction -> {
                            long taskGroupID = taskGroupIdGenerator.getNextId();
                            TaskGroupLocation taskGroupLocation =
                                    new TaskGroupLocation(
                                            jobImmutableInformation.getJobId(),
                                            pipelineIndex,
                                            taskGroupID);
                            TaskLocation taskLocation = new TaskLocation(taskGroupLocation, 0, 0);
                            SourceSplitEnumeratorTask<?> t =
                                    new SourceSplitEnumeratorTask<>(
                                            jobImmutableInformation.getJobId(),
                                            taskLocation,
                                            sourceAction);
                            // checkpoint
                            pipelineTasks.add(taskLocation);
                            startingTasks.add(taskLocation);
                            subtaskActions.put(
                                    taskLocation,
                                    Collections.singleton(
                                            Tuple2.tuple2(ActionStateKey.of(sourceAction), -1)));
                            enumeratorTaskIDMap.put(sourceAction, taskLocation);

                            return new PhysicalVertex(
                                    atomicInteger.incrementAndGet(),
                                    sources.size(),
                                    new TaskGroupDefaultImpl(
                                            taskGroupLocation,
                                            sourceAction.getName() + "-SplitEnumerator",
                                            Lists.newArrayList(t)),
                                    flakeIdGenerator,
                                    pipelineIndex,
                                    totalPipelineNum,
                                    Collections.singletonList(t.getJarsUrl()),
                                    Collections.singletonList(t.getConnectorPluginJars()),
                                    jobImmutableInformation,
                                    initializationTimestamp,
                                    nodeEngine,
                                    runningJobStateIMap,
                                    runningJobStateTimestampsIMap);
                        })
                .collect(Collectors.toList());
    }

    /**
     * Creates exactly one operator-scoped coordinator for each dynamic lookup execution vertex.
     *
     * <p>The coordinator is a control-plane task, not a business-row vertex. It is therefore
     * appended to the existing coordinator vertex list next to source enumerators and sink
     * committers.
     */
    private List<PhysicalVertex> getDynamicLookupCoordinatorTask(
            Pipeline pipeline, int pipelineIndex, int totalPipelineNum) {
        List<DynamicLookupAction> lookupActions =
                pipeline.getVertexes().values().stream()
                        .map(ExecutionVertex::getAction)
                        .filter(DynamicLookupAction.class::isInstance)
                        .map(DynamicLookupAction.class::cast)
                        .sorted(java.util.Comparator.comparingLong(Action::getId))
                        .collect(Collectors.toList());
        AtomicInteger coordinatorIndex = new AtomicInteger();
        return lookupActions.stream()
                .map(
                        lookupAction -> {
                            DynamicLookupCoordinatorAction coordinatorAction =
                                    DynamicLookupCoordinatorAction.from(lookupAction);
                            CoordinatorStateKey coordinatorStateKey =
                                    new CoordinatorStateKey(lookupAction.getOperatorUid());
                            long taskGroupId = taskGroupIdGenerator.getNextId();
                            TaskGroupLocation taskGroupLocation =
                                    new TaskGroupLocation(
                                            jobImmutableInformation.getJobId(),
                                            pipelineIndex,
                                            taskGroupId);
                            TaskLocation taskLocation = new TaskLocation(taskGroupLocation, 0, 0);
                            DynamicLookupCoordinatorTask task =
                                    new DynamicLookupCoordinatorTask(
                                            jobImmutableInformation.getJobId(),
                                            taskLocation,
                                            coordinatorAction,
                                            coordinatorStateKey);

                            pipelineTasks.add(taskLocation);
                            coordinatorCheckpointRoots.add(taskLocation);
                            TaskLocation existingCoordinator =
                                    dynamicLookupCoordinatorTasks.putIfAbsent(
                                            coordinatorStateKey, taskLocation);
                            if (existingCoordinator != null) {
                                throw new IllegalArgumentException(
                                        "DYNAMIC_LOOKUP_OPERATOR_UID_COLLISION: operatorUid="
                                                + lookupAction.getOperatorUid());
                            }
                            coordinatorPipelineActions.put(coordinatorStateKey, 0);
                            subtaskActions.put(
                                    taskLocation,
                                    Collections.singleton(
                                            Tuple2.tuple2(
                                                    coordinatorStateKey,
                                                    CheckpointPlan.COORDINATOR_INDEX)));

                            return new PhysicalVertex(
                                    coordinatorIndex.getAndIncrement(),
                                    lookupActions.size(),
                                    new TaskGroupDefaultImpl(
                                            taskGroupLocation,
                                            coordinatorAction.getName(),
                                            Collections.singletonList(task)),
                                    flakeIdGenerator,
                                    pipelineIndex,
                                    totalPipelineNum,
                                    Collections.singletonList(task.getJarsUrl()),
                                    Collections.singletonList(task.getConnectorPluginJars()),
                                    jobImmutableInformation,
                                    initializationTimestamp,
                                    nodeEngine,
                                    runningJobStateIMap,
                                    runningJobStateTimestampsIMap);
                        })
                .collect(Collectors.toList());
    }

    private List<PhysicalVertex> getSourceTask(
            List<ExecutionEdge> edges,
            List<SourceAction<?, ?, ?>> sources,
            int pipelineIndex,
            int totalPipelineNum) {
        boolean containsPortAwareTarget =
                edges.stream().anyMatch(PortAwareExecutionEdge.class::isInstance);
        return sources.stream()
                .map(
                        source ->
                                new PhysicalExecutionFlow(
                                        source,
                                        containsPortAwareTarget
                                                ? getNextWrapperBeforePortAwareTarget(edges, source)
                                                : getNextWrapper(edges, source)))
                .flatMap(
                        flow -> {
                            List<PhysicalVertex> t = new ArrayList<>();
                            List<Flow> flows = new ArrayList<>(Collections.singletonList(flow));
                            if (observabilityConfig.isEnabled()
                                    && !observabilityConfig.getAsyncBoundaries().isEmpty()) {
                                // Split async boundaries across the whole flow graph. Note that
                                // splitAsyncBoundaryFromFlow() uses an "intermediateFlow +
                                // intermediateFlowQuote" pattern (producer/consumer side), so
                                // newly created quote flows are not reachable from the original
                                // root by recursion. We must iterate over the growing flow list
                                // to apply boundaries in deeper segments.
                                int idx = 0;
                                while (idx < flows.size()) {
                                    flows.addAll(splitAsyncBoundaryFromFlow(flows.get(idx++)));
                                }
                            }
                            // Keep the legacy sink split regardless of realtime observability.
                            // Do not gate the legacy split on `split_sink_io`; otherwise enabling
                            // observability without that flag would move Source and Sink back into
                            // one thread.
                            List<Flow> sinkSplitRoots = new ArrayList<>(flows);
                            for (Flow root : sinkSplitRoots) {
                                flows.addAll(splitSinkFromFlow(root));
                            }
                            for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                                long taskGroupId = taskGroupIdGenerator.getNextId();
                                int finalParallelismIndex = i;
                                TaskGroupLocation taskGroupLocation =
                                        new TaskGroupLocation(
                                                jobImmutableInformation.getJobId(),
                                                pipelineIndex,
                                                taskGroupId);
                                AtomicInteger taskInTaskGroupIndex = new AtomicInteger(0);
                                List<SeaTunnelTask> taskList =
                                        flows.stream()
                                                .map(
                                                        f -> {
                                                            setFlowConfig(f);
                                                            final TaskLocation taskLocation =
                                                                    new TaskLocation(
                                                                            taskGroupLocation,
                                                                            taskInTaskGroupIndex
                                                                                    .getAndIncrement(),
                                                                            finalParallelismIndex);
                                                            if (f
                                                                    instanceof
                                                                    PhysicalExecutionFlow) {
                                                                return new SourceSeaTunnelTask<>(
                                                                        jobImmutableInformation
                                                                                .getJobId(),
                                                                        taskLocation,
                                                                        finalParallelismIndex,
                                                                        (PhysicalExecutionFlow<
                                                                                        SourceAction,
                                                                                        SourceConfig>)
                                                                                f,
                                                                        jobImmutableInformation
                                                                                .getJobConfig()
                                                                                .getEnvOptions());
                                                            } else {
                                                                return new TransformSeaTunnelTask(
                                                                        jobImmutableInformation
                                                                                .getJobId(),
                                                                        taskLocation,
                                                                        finalParallelismIndex,
                                                                        f,
                                                                        jobImmutableInformation
                                                                                .getJobConfig()
                                                                                .getEnvOptions());
                                                            }
                                                        })
                                                .peek(this::fillCheckpointPlan)
                                                .collect(Collectors.toList());
                                TaskLocation sourceTaskLocation =
                                        taskList.stream()
                                                .filter(SourceSeaTunnelTask.class::isInstance)
                                                .findFirst()
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalStateException(
                                                                        "Missing source task for "
                                                                                + flow.getAction()
                                                                                        .getName()))
                                                .getTaskLocation();
                                sourceTaskLocations
                                        .computeIfAbsent(
                                                flow.getAction().getId(),
                                                ignored -> new HashMap<>())
                                        .put(finalParallelismIndex, sourceTaskLocation);
                                List<Set<URL>> jars =
                                        taskList.stream()
                                                .map(SeaTunnelTask::getJarsUrl)
                                                .collect(Collectors.toList());

                                List<Set<ConnectorJarIdentifier>> jarIdentifiers =
                                        taskList.stream()
                                                .map(SeaTunnelTask::getConnectorPluginJars)
                                                .collect(Collectors.toList());

                                if (taskList.stream()
                                        .anyMatch(TransformSeaTunnelTask.class::isInstance)) {
                                    // contains IntermediateExecutionFlow in task group
                                    TaskGroupDefaultImpl taskGroup;
                                    if (queueType.equals(BLOCKINGQUEUE)) {
                                        taskGroup =
                                                new TaskGroupWithIntermediateBlockingQueue(
                                                        taskGroupLocation,
                                                        flow.getAction().getName() + "-SourceTask",
                                                        taskList.stream()
                                                                .map(task -> (Task) task)
                                                                .collect(Collectors.toList()));
                                    } else {
                                        taskGroup =
                                                new TaskGroupWithIntermediateDisruptor(
                                                        taskGroupLocation,
                                                        flow.getAction().getName() + "-SourceTask",
                                                        taskList.stream()
                                                                .map(task -> (Task) task)
                                                                .collect(Collectors.toList()));
                                    }
                                    t.add(
                                            new PhysicalVertex(
                                                    i,
                                                    flow.getAction().getParallelism(),
                                                    taskGroup,
                                                    flakeIdGenerator,
                                                    pipelineIndex,
                                                    totalPipelineNum,
                                                    jars,
                                                    jarIdentifiers,
                                                    jobImmutableInformation,
                                                    initializationTimestamp,
                                                    nodeEngine,
                                                    runningJobStateIMap,
                                                    runningJobStateTimestampsIMap));
                                } else {
                                    t.add(
                                            new PhysicalVertex(
                                                    i,
                                                    flow.getAction().getParallelism(),
                                                    new TaskGroupDefaultImpl(
                                                            taskGroupLocation,
                                                            flow.getAction().getName()
                                                                    + "-SourceTask",
                                                            taskList.stream()
                                                                    .map(task -> (Task) task)
                                                                    .collect(Collectors.toList())),
                                                    flakeIdGenerator,
                                                    pipelineIndex,
                                                    totalPipelineNum,
                                                    jars,
                                                    jarIdentifiers,
                                                    jobImmutableInformation,
                                                    initializationTimestamp,
                                                    nodeEngine,
                                                    runningJobStateIMap,
                                                    runningJobStateTimestampsIMap));
                                }
                            }
                            return t.stream();
                        })
                .collect(Collectors.toList());
    }

    private List<PhysicalVertex> getDynamicLookupRuntimeTask(
            Pipeline pipeline, int pipelineIndex, int totalPipelineNum) {
        if (!queueType.equals(BLOCKINGQUEUE)) {
            throw new IllegalArgumentException(
                    "Dynamic lookup M0 requires engine.queue.type=BLOCKINGQUEUE");
        }
        List<ExecutionVertex> lookupVertices =
                pipeline.getVertexes().values().stream()
                        .filter(vertex -> vertex.getAction() instanceof DynamicLookupAction)
                        .sorted(java.util.Comparator.comparingLong(ExecutionVertex::getVertexId))
                        .collect(Collectors.toList());
        List<PhysicalVertex> physicalVertices = new ArrayList<>();
        for (ExecutionVertex lookupVertex : lookupVertices) {
            DynamicLookupAction lookupAction = (DynamicLookupAction) lookupVertex.getAction();
            List<PortAwareExecutionEdge> inputEdges =
                    pipeline.getEdges().stream()
                            .filter(PortAwareExecutionEdge.class::isInstance)
                            .map(PortAwareExecutionEdge.class::cast)
                            .filter(
                                    edge ->
                                            edge.getRightVertexId()
                                                    .equals(lookupVertex.getVertexId()))
                            .sorted(
                                    java.util.Comparator.comparingInt(
                                                    PortAwareExecutionEdge::getTargetInputPort)
                                            .thenComparingLong(PortAwareExecutionEdge::getEdgeId))
                            .collect(Collectors.toList());
            validateDynamicLookupInputEdges(lookupAction, inputEdges);
            for (int subtaskIndex = 0;
                    subtaskIndex < lookupVertex.getParallelism();
                    subtaskIndex++) {
                physicalVertices.add(
                        createDynamicLookupRuntimeVertex(
                                pipeline,
                                lookupVertex,
                                inputEdges,
                                subtaskIndex,
                                pipelineIndex,
                                totalPipelineNum));
            }
        }
        return physicalVertices;
    }

    private PhysicalVertex createDynamicLookupRuntimeVertex(
            Pipeline pipeline,
            ExecutionVertex lookupVertex,
            List<PortAwareExecutionEdge> inputEdges,
            int subtaskIndex,
            int pipelineIndex,
            int totalPipelineNum) {
        DynamicLookupAction lookupAction = (DynamicLookupAction) lookupVertex.getAction();
        long taskGroupId = taskGroupIdGenerator.getNextId();
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(
                        jobImmutableInformation.getJobId(), pipelineIndex, taskGroupId);
        AtomicInteger taskInTaskGroupIndex = new AtomicInteger(0);
        Map<Integer, IntermediateQueue> inputQueues = createDynamicLookupInputQueues(lookupAction);
        IntermediateQueue factGateCommandQueue = createDynamicLookupGateCommandQueue(lookupAction);
        List<Flow> flows = new ArrayList<>();
        Map<Long, TaskLocation> sourceLocationsByAction = new HashMap<>();
        long factSourceActionId =
                inputEdges.stream()
                        .filter(edge -> edge.getTargetInputPort() == DynamicLookupAction.FACT_INPUT)
                        .map(edge -> edge.getLeftVertex().getAction().getId())
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Missing dynamic lookup fact source action"));

        for (PortAwareExecutionEdge inputEdge : inputEdges) {
            SourceAction<?, ?, ?> sourceAction =
                    (SourceAction<?, ?, ?>) inputEdge.getLeftVertex().getAction();
            IntermediateQueue queue = inputQueues.get(inputEdge.getTargetInputPort());
            IntermediateExecutionFlow<?> queueFlow = new IntermediateExecutionFlow<>(queue);
            flows.add(
                    new PhysicalExecutionFlow<>(
                            sourceAction, Collections.singletonList(queueFlow)));
        }

        PhysicalExecutionFlow<DynamicLookupAction, DynamicLookupConfig> lookupFlow =
                new PhysicalExecutionFlow<>(
                        lookupAction, getNextWrapper(pipeline.getEdges(), lookupAction));
        flows.add(lookupFlow);
        List<Flow> splitRoots = new ArrayList<>(Collections.singletonList(lookupFlow));
        for (Flow root : splitRoots) {
            flows.addAll(splitSinkFromFlow(root));
        }

        List<SeaTunnelTask> taskList =
                flows.stream()
                        .map(
                                flow -> {
                                    setFlowConfig(
                                            flow,
                                            inputQueues,
                                            factGateCommandQueue,
                                            factSourceActionId);
                                    TaskLocation taskLocation =
                                            new TaskLocation(
                                                    taskGroupLocation,
                                                    taskInTaskGroupIndex.getAndIncrement(),
                                                    subtaskIndex);
                                    if (flow instanceof PhysicalExecutionFlow
                                            && ((PhysicalExecutionFlow<?, ?>) flow).getAction()
                                                    instanceof SourceAction) {
                                        SourceSeaTunnelTask<?, ?> sourceTask =
                                                new SourceSeaTunnelTask<>(
                                                        jobImmutableInformation.getJobId(),
                                                        taskLocation,
                                                        subtaskIndex,
                                                        (PhysicalExecutionFlow<
                                                                        SourceAction, SourceConfig>)
                                                                flow,
                                                        jobImmutableInformation
                                                                .getJobConfig()
                                                                .getEnvOptions());
                                        sourceLocationsByAction.put(
                                                ((PhysicalExecutionFlow<?, ?>) flow)
                                                        .getAction()
                                                        .getId(),
                                                taskLocation);
                                        return sourceTask;
                                    }
                                    return new TransformSeaTunnelTask(
                                            jobImmutableInformation.getJobId(),
                                            taskLocation,
                                            subtaskIndex,
                                            flow);
                                })
                        .peek(this::fillCheckpointPlan)
                        .collect(Collectors.toList());

        for (Map.Entry<Long, TaskLocation> entry : sourceLocationsByAction.entrySet()) {
            sourceTaskLocations
                    .computeIfAbsent(entry.getKey(), ignored -> new HashMap<>())
                    .put(subtaskIndex, entry.getValue());
        }
        TaskLocation lookupTaskLocation =
                taskList.stream()
                        .filter(
                                task ->
                                        task.getActionStateKeys()
                                                .contains(ActionStateKey.of(lookupAction)))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Missing dynamic lookup task for "
                                                        + lookupAction.getName()))
                        .getTaskLocation();
        checkpointInputPorts.put(
                lookupTaskLocation,
                createInputPortDescriptors(
                        lookupVertex, lookupTaskLocation, subtaskIndex, inputEdges));

        List<Set<URL>> jars =
                taskList.stream().map(SeaTunnelTask::getJarsUrl).collect(Collectors.toList());
        List<Set<ConnectorJarIdentifier>> jarIdentifiers =
                taskList.stream()
                        .map(SeaTunnelTask::getConnectorPluginJars)
                        .collect(Collectors.toList());
        return new PhysicalVertex(
                subtaskIndex,
                lookupVertex.getParallelism(),
                new TaskGroupWithIntermediateBlockingQueue(
                        taskGroupLocation,
                        lookupAction.getName() + "-DynamicLookupTask",
                        taskList.stream().map(task -> (Task) task).collect(Collectors.toList())),
                flakeIdGenerator,
                pipelineIndex,
                totalPipelineNum,
                jars,
                jarIdentifiers,
                jobImmutableInformation,
                initializationTimestamp,
                nodeEngine,
                runningJobStateIMap,
                runningJobStateTimestampsIMap,
                checkpointInputPorts.get(lookupTaskLocation));
    }

    private static Map<Integer, IntermediateQueue> createDynamicLookupInputQueues(
            DynamicLookupAction lookupAction) {
        Map<Integer, IntermediateQueue> queues = new HashMap<>();
        queues.put(
                DynamicLookupAction.FACT_INPUT,
                new IntermediateQueue(
                        dynamicLookupInputQueueId(
                                lookupAction.getId(), DynamicLookupAction.FACT_INPUT),
                        lookupAction.getName() + "-FactQueue",
                        lookupAction.getParallelism()));
        queues.put(
                DynamicLookupAction.DIMENSION_INPUT,
                new IntermediateQueue(
                        dynamicLookupInputQueueId(
                                lookupAction.getId(), DynamicLookupAction.DIMENSION_INPUT),
                        lookupAction.getName() + "-DimensionQueue",
                        lookupAction.getParallelism()));
        return queues;
    }

    private static IntermediateQueue createDynamicLookupGateCommandQueue(
            DynamicLookupAction lookupAction) {
        return new IntermediateQueue(
                dynamicLookupGateCommandQueueId(lookupAction.getId()),
                lookupAction.getName() + "-FactGateCommandQueue",
                lookupAction.getParallelism(),
                1);
    }

    private static void validateDynamicLookupPipeline(Pipeline pipeline) {
        if (!containsDynamicLookup(pipeline)) {
            return;
        }
        Set<Long> portAwareSourceActionIds =
                pipeline.getEdges().stream()
                        .filter(PortAwareExecutionEdge.class::isInstance)
                        .map(PortAwareExecutionEdge.class::cast)
                        .map(edge -> edge.getLeftVertex().getAction().getId())
                        .collect(Collectors.toSet());
        boolean hasUnsupportedPortAwareTarget =
                pipeline.getEdges().stream()
                        .filter(PortAwareExecutionEdge.class::isInstance)
                        .anyMatch(
                                edge ->
                                        !(edge.getRightVertex().getAction()
                                                instanceof DynamicLookupAction));
        if (hasUnsupportedPortAwareTarget) {
            throw new IllegalArgumentException(
                    "Dynamic lookup currently supports only DynamicLookupAction as a port-aware"
                            + " action");
        }
        boolean hasUnownedSourceBranch =
                pipeline.getVertexes().values().stream()
                        .map(ExecutionVertex::getAction)
                        .filter(SourceAction.class::isInstance)
                        .map(Action::getId)
                        .anyMatch(
                                sourceActionId ->
                                        !portAwareSourceActionIds.contains(sourceActionId));
        if (hasUnownedSourceBranch) {
            throw new IllegalArgumentException(
                    "Dynamic lookup M0 requires every source in the pipeline to be owned by a "
                            + "DynamicLookupAction input port");
        }
        Set<String> operatorUids = new HashSet<>();
        for (ExecutionVertex lookupVertex :
                pipeline.getVertexes().values().stream()
                        .filter(vertex -> vertex.getAction() instanceof DynamicLookupAction)
                        .collect(Collectors.toList())) {
            DynamicLookupAction lookupAction = (DynamicLookupAction) lookupVertex.getAction();
            if (!operatorUids.add(lookupAction.getOperatorUid())) {
                throw new IllegalArgumentException(
                        "DYNAMIC_LOOKUP_OPERATOR_UID_COLLISION: operatorUid="
                                + lookupAction.getOperatorUid());
            }
            List<PortAwareExecutionEdge> inputEdges =
                    pipeline.getEdges().stream()
                            .filter(PortAwareExecutionEdge.class::isInstance)
                            .map(PortAwareExecutionEdge.class::cast)
                            .filter(
                                    edge ->
                                            edge.getRightVertexId()
                                                    .equals(lookupVertex.getVertexId()))
                            .collect(Collectors.toList());
            validateDynamicLookupInputEdges(lookupAction, inputEdges);
        }
    }

    private static void validateDynamicLookupInputEdges(
            DynamicLookupAction lookupAction, List<PortAwareExecutionEdge> inputEdges) {
        Set<Integer> ports =
                inputEdges.stream()
                        .map(PortAwareExecutionEdge::getTargetInputPort)
                        .collect(Collectors.toSet());
        if (inputEdges.size() != 2
                || !ports.contains(DynamicLookupAction.FACT_INPUT)
                || !ports.contains(DynamicLookupAction.DIMENSION_INPUT)) {
            throw new IllegalArgumentException(
                    "Dynamic lookup "
                            + lookupAction.getOperatorUid()
                            + " requires exactly fact port 0 and dimension port 1");
        }
        boolean allInputsAreSources =
                inputEdges.stream()
                        .allMatch(edge -> edge.getLeftVertex().getAction() instanceof SourceAction);
        if (!allInputsAreSources) {
            throw new IllegalArgumentException(
                    "Dynamic lookup M0 requires direct SourceAction inputs");
        }
        boolean allForwardInputsMatchTargetParallelism =
                inputEdges.stream()
                        .allMatch(
                                edge ->
                                        edge.getLeftVertex().getParallelism()
                                                == edge.getRightVertex().getParallelism());
        if (!allForwardInputsMatchTargetParallelism) {
            throw new IllegalArgumentException(
                    "Dynamic lookup M0 FORWARD input requires equal source and target parallelism");
        }
    }

    private List<InputPortDescriptor> createInputPortDescriptors(
            ExecutionVertex lookupVertex,
            TaskLocation targetTaskLocation,
            int targetSubtask,
            List<PortAwareExecutionEdge> inputEdges) {
        Map<Integer, List<PhysicalInputChannel>> channelsByPort = new java.util.TreeMap<>();
        for (PortAwareExecutionEdge inputEdge : inputEdges) {
            ExecutionVertex sourceVertex = inputEdge.getLeftVertex();
            Map<Integer, TaskLocation> sourceLocations =
                    sourceTaskLocations.get(sourceVertex.getAction().getId());
            if (sourceLocations == null || !sourceLocations.containsKey(targetSubtask)) {
                throw new IllegalStateException(
                        "Missing source task locations for action "
                                + sourceVertex.getAction().getName());
            }
            DynamicLookupAction lookupAction = (DynamicLookupAction) lookupVertex.getAction();
            LogicalChannelKey logicalChannelKey =
                    new LogicalChannelKey(
                            Long.toString(jobImmutableInformation.getJobId()),
                            lookupAction.getOperatorUid(),
                            lookupAction.getSourceActionUid(inputEdge.getTargetInputPort()),
                            inputEdge.getEdgeId(),
                            inputEdge.getTargetInputPort(),
                            targetSubtask,
                            targetSubtask);
            PhysicalInputChannel channel =
                    new PhysicalInputChannel(
                            logicalChannelKey,
                            sourceLocations.get(targetSubtask),
                            targetTaskLocation,
                            inputEdge.getExchangeDescriptor());
            channelsByPort
                    .computeIfAbsent(inputEdge.getTargetInputPort(), ignored -> new ArrayList<>())
                    .add(channel);
        }
        return channelsByPort.entrySet().stream()
                .map(entry -> new InputPortDescriptor(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private void fillCheckpointPlan(SeaTunnelTask task) {
        pipelineTasks.add(task.getTaskLocation());
        subtaskActions.put(
                task.getTaskLocation(),
                task.getActionStateKeys().stream()
                        .map(
                                stateKey ->
                                        Tuple2.tuple2(
                                                stateKey, task.getTaskLocation().getTaskIndex()))
                        .collect(Collectors.toSet()));
    }

    /**
     * set config for flow, some flow should have config support for execute on task.
     *
     * @param f flow
     */
    @SuppressWarnings("unchecked")
    private void setFlowConfig(Flow f) {
        setFlowConfig(f, Collections.emptyMap(), null, -1L);
    }

    private void setFlowConfig(
            Flow f,
            Map<Integer, IntermediateQueue> dynamicLookupInputQueues,
            IntermediateQueue dynamicLookupFactGateCommandQueue,
            long dynamicLookupFactSourceActionId) {

        if (f instanceof PhysicalExecutionFlow) {
            PhysicalExecutionFlow<?, FlowConfig> flow = (PhysicalExecutionFlow<?, FlowConfig>) f;
            if (flow.getAction() instanceof SourceAction) {
                SourceConfig config = new SourceConfig();
                config.setEnumeratorTask(
                        enumeratorTaskIDMap.get((SourceAction<?, ?, ?>) flow.getAction()));
                if (flow.getAction().getId() == dynamicLookupFactSourceActionId) {
                    config.setDynamicLookupFactGate(true);
                    config.setDynamicLookupGateCommandQueue(
                            new IntermediateQueueConfig(
                                    dynamicLookupFactGateCommandQueue.getId(),
                                    dynamicLookupFactGateCommandQueue.getCapacity()));
                }
                flow.setConfig(config);
            } else if (flow.getAction() instanceof SinkAction) {
                SinkConfig config = new SinkConfig();
                if (committerTaskIDMap.containsKey((SinkAction<?, ?, ?, ?>) flow.getAction())) {
                    config.setContainCommitter(true);
                    config.setCommitterTask(
                            committerTaskIDMap.get((SinkAction<?, ?, ?, ?>) flow.getAction()));
                }
                flow.setConfig(config);
            } else if (flow.getAction() instanceof DynamicLookupAction) {
                DynamicLookupAction dynamicLookupAction = (DynamicLookupAction) flow.getAction();
                Map<Integer, IntermediateQueueConfig> inputQueueConfigs = new HashMap<>();
                dynamicLookupInputQueues.forEach(
                        (inputPort, queue) ->
                                inputQueueConfigs.put(
                                        inputPort,
                                        new IntermediateQueueConfig(
                                                queue.getId(), queue.getCapacity())));
                flow.setConfig(
                        new DynamicLookupConfig(
                                inputQueueConfigs,
                                new IntermediateQueueConfig(
                                        dynamicLookupFactGateCommandQueue.getId(),
                                        dynamicLookupFactGateCommandQueue.getCapacity()),
                                dynamicLookupAction.getMaxLogicalStateBytesPerSubtask(),
                                dynamicLookupAction.getMaxResidentStateBytesPerSubtask()));
            }
        } else if (f instanceof IntermediateExecutionFlow) {
            ((IntermediateExecutionFlow<IntermediateQueueConfig>) f)
                    .setConfig(
                            new IntermediateQueueConfig(
                                    ((IntermediateExecutionFlow<?>) f).getQueue().getId(),
                                    ((IntermediateExecutionFlow<?>) f).getQueue().getCapacity()));
        } else {
            throw new UnknownFlowException(f);
        }

        if (!f.getNext().isEmpty()) {
            f.getNext()
                    .forEach(
                            next ->
                                    setFlowConfig(
                                            next,
                                            dynamicLookupInputQueues,
                                            dynamicLookupFactGateCommandQueue,
                                            dynamicLookupFactSourceActionId));
        }
    }

    /**
     * Use Java Queue to split flow which source to sink without partition transform
     *
     * @param flow need to be split flow
     * @return flows after split
     */
    private static List<Flow> splitSinkFromFlow(Flow flow) {
        // Only split when the producer is a normal PhysicalExecutionFlow. If the producer itself is
        // an IntermediateExecutionFlow (queue), the sink is already isolated and must not be
        // wrapped again; otherwise it may keep inserting nested queues.
        boolean allowDirectSinkSplit = flow instanceof PhysicalExecutionFlow;
        List<PhysicalExecutionFlow<?, ?>> sinkFlows =
                allowDirectSinkSplit
                        ? flow.getNext().stream()
                                .filter(f -> f instanceof PhysicalExecutionFlow)
                                .map(f -> (PhysicalExecutionFlow<?, ?>) f)
                                .filter(f -> f.getAction() instanceof SinkAction)
                                .collect(Collectors.toList())
                        : Collections.emptyList();
        List<Flow> allFlows = new ArrayList<>();
        flow.getNext().removeAll(sinkFlows);
        sinkFlows.forEach(
                s -> {
                    long queueId = sinkSplitQueueId(s.getAction().getId());
                    IntermediateQueue queue =
                            new IntermediateQueue(
                                    queueId,
                                    s.getAction().getName() + "-Queue",
                                    s.getAction().getParallelism());
                    IntermediateExecutionFlow<?> intermediateFlow =
                            new IntermediateExecutionFlow<>(queue);
                    flow.getNext().add(intermediateFlow);
                    IntermediateExecutionFlow<?> intermediateFlowQuote =
                            new IntermediateExecutionFlow<>(queue);
                    intermediateFlowQuote.getNext().add(s);
                    allFlows.add(intermediateFlowQuote);
                });

        if (!flow.getNext().isEmpty()) {
            allFlows.addAll(
                    flow.getNext().stream()
                            .flatMap(f -> splitSinkFromFlow(f).stream())
                            .collect(Collectors.toList()));
        }
        return allFlows;
    }

    private List<Flow> splitAsyncBoundaryFromFlow(Flow flow) {
        // Only split when the producer is a normal PhysicalExecutionFlow. If the producer itself
        // is an IntermediateExecutionFlow (queue), the boundary is already isolated at this edge
        // and must not be wrapped again; otherwise it may keep inserting nested queues.
        boolean allowDirectAsyncSplit = flow instanceof PhysicalExecutionFlow;
        List<PhysicalExecutionFlow<?, ?>> targetFlows =
                allowDirectAsyncSplit
                        ? flow.getNext().stream()
                                .filter(f -> f instanceof PhysicalExecutionFlow)
                                .map(f -> (PhysicalExecutionFlow<?, ?>) f)
                                .filter(f -> f.getAction() instanceof TransformChainAction)
                                .filter(
                                        f -> {
                                            TransformChainAction<?> action =
                                                    (TransformChainAction<?>) f.getAction();
                                            if (!(action.getConfig()
                                                    instanceof TransformChainConfig)) {
                                                return false;
                                            }
                                            String start =
                                                    ((TransformChainConfig) action.getConfig())
                                                            .getStartTransformName();
                                            return observabilityConfig
                                                    .getAsyncBoundaries()
                                                    .contains(start);
                                        })
                                .collect(Collectors.toList())
                        : Collections.emptyList();

        List<Flow> allFlows = new ArrayList<>();
        flow.getNext().removeAll(targetFlows);
        targetFlows.forEach(
                f -> {
                    TransformChainAction<?> action = (TransformChainAction<?>) f.getAction();
                    String start =
                            ((TransformChainConfig) action.getConfig()).getStartTransformName();
                    int capacity = Math.max(0, observabilityConfig.capacityForBoundary(start));
                    long queueId = asyncBoundaryQueueId(action.getId());
                    IntermediateQueue queue =
                            new IntermediateQueue(
                                    queueId,
                                    action.getName() + "-AsyncBoundary-Queue",
                                    action.getParallelism(),
                                    capacity);
                    IntermediateExecutionFlow<?> intermediateFlow =
                            new IntermediateExecutionFlow<>(queue);
                    flow.getNext().add(intermediateFlow);
                    IntermediateExecutionFlow<?> intermediateFlowQuote =
                            new IntermediateExecutionFlow<>(queue);
                    intermediateFlowQuote.getNext().add(f);
                    allFlows.add(intermediateFlowQuote);
                });

        if (!flow.getNext().isEmpty()) {
            allFlows.addAll(
                    flow.getNext().stream()
                            .flatMap(f -> splitAsyncBoundaryFromFlow(f).stream())
                            .collect(Collectors.toList()));
        }
        return allFlows;
    }

    /**
     * IntermediateQueue IDs must be stable and must not collide with Action IDs or other queue IDs.
     *
     * <p>We encode different queue types into the ID space to avoid collisions even if Action IDs
     * are not globally unique across different action types.
     */
    private static long asyncBoundaryQueueId(long actionId) {
        // negative even number
        return -((actionId * 2));
    }

    private static long sinkSplitQueueId(long actionId) {
        // negative odd number
        return -((actionId * 2) + 1);
    }

    private static long dynamicLookupInputQueueId(long actionId, int inputPort) {
        return dynamicLookupQueueBase(actionId) - 10 - inputPort;
    }

    private static long dynamicLookupGateCommandQueueId(long actionId) {
        return dynamicLookupQueueBase(actionId) - 90;
    }

    private static long dynamicLookupQueueBase(long actionId) {
        // Reserve dynamic lookup queues in a high positive range that is disjoint from the legacy
        // negative even/odd queue-id encodings used by async boundaries and sink-split queues.
        return Long.MAX_VALUE - (actionId * 100);
    }

    private List<Flow> getNextWrapper(List<ExecutionEdge> edges, Action start) {
        List<Action> actions =
                edges.stream()
                        .filter(e -> e.getLeftVertex().getAction().equals(start))
                        .map(e -> e.getRightVertex().getAction())
                        .collect(Collectors.toList());
        List<Flow> wrappers =
                actions.stream()
                        .filter(a -> a instanceof SinkAction)
                        .map(PhysicalExecutionFlow::new)
                        .collect(Collectors.toList());
        wrappers.addAll(
                actions.stream()
                        .filter(a -> !(a instanceof SinkAction))
                        .map(a -> new PhysicalExecutionFlow<>(a, getNextWrapper(edges, a)))
                        .collect(Collectors.toList()));
        return wrappers;
    }

    /**
     * Builds the legacy source-local flow only up to a port-aware target.
     *
     * <p>The target is materialized exactly once by {@link #getDynamicLookupRuntimeTask(Pipeline,
     * int, int)} instead of once per source root.
     */
    private List<Flow> getNextWrapperBeforePortAwareTarget(
            List<ExecutionEdge> edges, Action start) {
        List<Action> actions =
                edges.stream()
                        .filter(edge -> edge.getLeftVertex().getAction().equals(start))
                        .filter(edge -> !(edge instanceof PortAwareExecutionEdge))
                        .map(edge -> edge.getRightVertex().getAction())
                        .collect(Collectors.toList());
        List<Flow> wrappers =
                actions.stream()
                        .filter(action -> action instanceof SinkAction)
                        .map(PhysicalExecutionFlow::new)
                        .collect(Collectors.toList());
        wrappers.addAll(
                actions.stream()
                        .filter(action -> !(action instanceof SinkAction))
                        .map(
                                action ->
                                        new PhysicalExecutionFlow<>(
                                                action,
                                                getNextWrapperBeforePortAwareTarget(edges, action)))
                        .collect(Collectors.toList()));
        return wrappers;
    }
}
