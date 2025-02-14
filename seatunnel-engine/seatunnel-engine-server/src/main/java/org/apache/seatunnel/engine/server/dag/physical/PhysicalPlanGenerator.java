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
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleAction;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleConfig;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleMultipleRowStrategy;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleStrategy;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.internal.IntermediateQueue;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.ActionStateKey;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionEdge;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
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

    private final IdGenerator idGenerator = new IdGenerator();

    private final JobImmutableInformation jobImmutableInformation;

    private final long initializationTimestamp;

    private final ExecutorService executorService;

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

    private final IMap<Object, Object> runningJobStateIMap;

    private final IMap<Object, Object> runningJobStateTimestampsIMap;

    private final QueueType queueType;

    public PhysicalPlanGenerator(
            @NonNull ExecutionPlan executionPlan,
            @NonNull NodeEngine nodeEngine,
            @NonNull JobImmutableInformation jobImmutableInformation,
            long initializationTimestamp,
            @NonNull ExecutorService executorService,
            @NonNull FlakeIdGenerator flakeIdGenerator,
            @NonNull IMap runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap,
            @NonNull QueueType queueType) {
        this.pipelines = executionPlan.getPipelines();
        this.nodeEngine = nodeEngine;
        this.jobImmutableInformation = jobImmutableInformation;
        this.initializationTimestamp = initializationTimestamp;
        this.executorService = executorService;
        this.flakeIdGenerator = flakeIdGenerator;
        // the checkpoint of a pipeline
        this.pipelineTasks = new HashSet<>();
        this.startingTasks = new HashSet<>();
        this.subtaskActions = new HashMap<>();
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.queueType = queueType;
    }

    public Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> generate() {
        Map<String, String> tagFilter =
                (Map<String, String>)
                        jobImmutableInformation
                                .getJobConfig()
                                .getEnvOptions()
                                .get(EnvCommonOptions.NODE_TAG_FILTER.key());
        // TODO Determine which tasks do not need to be restored according to state
        CopyOnWriteArrayList<PassiveCompletableFuture<PipelineStatus>>
                waitForCompleteBySubPlanList = new CopyOnWriteArrayList<>();

        Map<Integer, CheckpointPlan> checkpointPlans = new HashMap<>();
        final int totalPipelineNum = pipelines.size();
        Stream<SubPlan> subPlanStream =
                pipelines.stream()
                        .map(
                                pipeline -> {
                                    this.pipelineTasks.clear();
                                    this.startingTasks.clear();
                                    this.subtaskActions.clear();
                                    final int pipelineId = pipeline.getId();
                                    final List<ExecutionEdge> edges = pipeline.getEdges();

                                    List<SourceAction<?, ?, ?>> sources = findSourceAction(edges);

                                    List<PhysicalVertex> coordinatorVertexList =
                                            getEnumeratorTask(
                                                    sources, pipelineId, totalPipelineNum);
                                    coordinatorVertexList.addAll(
                                            getCommitterTask(edges, pipelineId, totalPipelineNum));

                                    List<PhysicalVertex> physicalVertexList =
                                            getSourceTask(
                                                    edges, sources, pipelineId, totalPipelineNum);

                                    physicalVertexList.addAll(
                                            getShuffleTask(edges, pipelineId, totalPipelineNum));

                                    CompletableFuture<PipelineStatus> pipelineFuture =
                                            new CompletableFuture<>();
                                    waitForCompleteBySubPlanList.add(
                                            new PassiveCompletableFuture<>(pipelineFuture));

                                    checkpointPlans.put(
                                            pipelineId,
                                            CheckpointPlan.builder()
                                                    .pipelineId(pipelineId)
                                                    .pipelineSubtasks(pipelineTasks)
                                                    .startingSubtasks(startingTasks)
                                                    .pipelineActions(pipeline.getActions())
                                                    .subtaskActions(subtaskActions)
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
                            try {
                                sinkAggregatedCommitter =
                                        sinkAction.getSink().createAggregatedCommitter();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            // if sinkAggregatedCommitter is empty, don't create task.
                            if (sinkAggregatedCommitter.isPresent()) {
                                long taskGroupID = idGenerator.getNextId();
                                long taskTypeId = idGenerator.getNextId();
                                TaskGroupLocation taskGroupLocation =
                                        new TaskGroupLocation(
                                                jobImmutableInformation.getJobId(),
                                                pipelineIndex,
                                                taskGroupID);
                                TaskLocation taskLocation =
                                        new TaskLocation(taskGroupLocation, taskTypeId, 0);
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

    private List<PhysicalVertex> getShuffleTask(
            List<ExecutionEdge> edges, int pipelineIndex, int totalPipelineNum) {
        return edges.stream()
                .filter(s -> s.getLeftVertex().getAction() instanceof ShuffleAction)
                .map(q -> (ShuffleAction) q.getLeftVertex().getAction())
                .collect(Collectors.toSet())
                .stream()
                .map(q -> new PhysicalExecutionFlow(q, getNextWrapper(edges, q)))
                .flatMap(
                        flow -> {
                            List<PhysicalVertex> physicalVertices = new ArrayList<>();

                            ShuffleAction shuffleAction = (ShuffleAction) flow.getAction();
                            ShuffleConfig shuffleConfig = shuffleAction.getConfig();
                            ShuffleStrategy shuffleStrategy = shuffleConfig.getShuffleStrategy();
                            if (shuffleStrategy instanceof ShuffleMultipleRowStrategy) {
                                ShuffleMultipleRowStrategy shuffleMultipleRowStrategy =
                                        (ShuffleMultipleRowStrategy) shuffleStrategy;
                                for (Flow nextFlow : flow.getNext()) {
                                    PhysicalExecutionFlow sinkFlow =
                                            (PhysicalExecutionFlow) nextFlow;
                                    SinkAction sinkAction = (SinkAction) sinkFlow.getAction();
                                    String sinkTableId =
                                            sinkAction.getConfig().getTablePath().toString();

                                    long taskIDPrefix = idGenerator.getNextId();
                                    long taskGroupIDPrefix = idGenerator.getNextId();
                                    int parallelismIndex = 0;

                                    ShuffleStrategy shuffleStrategyOfSinkFlow =
                                            shuffleMultipleRowStrategy
                                                    .toBuilder()
                                                    .targetTableId(sinkTableId)
                                                    .build();
                                    ShuffleConfig shuffleConfigOfSinkFlow =
                                            shuffleConfig
                                                    .toBuilder()
                                                    .shuffleStrategy(shuffleStrategyOfSinkFlow)
                                                    .build();
                                    long shuffleActionId = idGenerator.getNextId();
                                    String shuffleActionName =
                                            String.format(
                                                    "%s -> %s -> %s",
                                                    shuffleAction.getName(),
                                                    sinkTableId,
                                                    sinkAction.getName());
                                    ShuffleAction shuffleActionOfSinkFlow =
                                            new ShuffleAction(
                                                    shuffleActionId,
                                                    shuffleActionName,
                                                    shuffleConfigOfSinkFlow);
                                    shuffleActionOfSinkFlow.setParallelism(1);
                                    PhysicalExecutionFlow shuffleFlow =
                                            new PhysicalExecutionFlow(
                                                    shuffleActionOfSinkFlow,
                                                    Collections.singletonList(sinkFlow));
                                    setFlowConfig(shuffleFlow);

                                    long taskGroupID =
                                            mixIDPrefixAndIndex(
                                                    taskGroupIDPrefix, parallelismIndex);
                                    TaskGroupLocation taskGroupLocation =
                                            new TaskGroupLocation(
                                                    jobImmutableInformation.getJobId(),
                                                    pipelineIndex,
                                                    taskGroupID);
                                    TaskLocation taskLocation =
                                            new TaskLocation(
                                                    taskGroupLocation,
                                                    taskIDPrefix,
                                                    parallelismIndex);
                                    SeaTunnelTask seaTunnelTask =
                                            new TransformSeaTunnelTask(
                                                    jobImmutableInformation.getJobId(),
                                                    taskLocation,
                                                    parallelismIndex,
                                                    shuffleFlow);

                                    // checkpoint
                                    fillCheckpointPlan(seaTunnelTask);
                                    physicalVertices.add(
                                            new PhysicalVertex(
                                                    parallelismIndex,
                                                    shuffleFlow.getAction().getParallelism(),
                                                    new TaskGroupDefaultImpl(
                                                            taskGroupLocation,
                                                            shuffleFlow.getAction().getName()
                                                                    + "-ShuffleTask",
                                                            Collections.singletonList(
                                                                    seaTunnelTask)),
                                                    flakeIdGenerator,
                                                    pipelineIndex,
                                                    totalPipelineNum,
                                                    Collections.singletonList(
                                                            seaTunnelTask.getJarsUrl()),
                                                    Collections.singletonList(
                                                            seaTunnelTask.getConnectorPluginJars()),
                                                    jobImmutableInformation,
                                                    initializationTimestamp,
                                                    nodeEngine,
                                                    runningJobStateIMap,
                                                    runningJobStateTimestampsIMap));
                                }
                            } else {
                                long taskIDPrefix = idGenerator.getNextId();
                                long taskGroupIDPrefix = idGenerator.getNextId();
                                for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                                    long taskGroupID = mixIDPrefixAndIndex(taskGroupIDPrefix, i);
                                    TaskGroupLocation taskGroupLocation =
                                            new TaskGroupLocation(
                                                    jobImmutableInformation.getJobId(),
                                                    pipelineIndex,
                                                    taskGroupID);
                                    TaskLocation taskLocation =
                                            new TaskLocation(taskGroupLocation, taskIDPrefix, i);
                                    setFlowConfig(flow);
                                    SeaTunnelTask seaTunnelTask =
                                            new TransformSeaTunnelTask(
                                                    jobImmutableInformation.getJobId(),
                                                    taskLocation,
                                                    i,
                                                    flow);
                                    // checkpoint
                                    fillCheckpointPlan(seaTunnelTask);
                                    physicalVertices.add(
                                            new PhysicalVertex(
                                                    i,
                                                    flow.getAction().getParallelism(),
                                                    new TaskGroupDefaultImpl(
                                                            taskGroupLocation,
                                                            flow.getAction().getName()
                                                                    + "-ShuffleTask",
                                                            Lists.newArrayList(seaTunnelTask)),
                                                    flakeIdGenerator,
                                                    pipelineIndex,
                                                    totalPipelineNum,
                                                    Collections.singletonList(
                                                            seaTunnelTask.getJarsUrl()),
                                                    Collections.singletonList(
                                                            seaTunnelTask.getConnectorPluginJars()),
                                                    jobImmutableInformation,
                                                    initializationTimestamp,
                                                    nodeEngine,
                                                    runningJobStateIMap,
                                                    runningJobStateTimestampsIMap));
                                }
                            }
                            return physicalVertices.stream();
                        })
                .collect(Collectors.toList());
    }

    private List<PhysicalVertex> getEnumeratorTask(
            List<SourceAction<?, ?, ?>> sources, int pipelineIndex, int totalPipelineNum) {
        AtomicInteger atomicInteger = new AtomicInteger(-1);

        return sources.stream()
                .map(
                        sourceAction -> {
                            long taskGroupID = idGenerator.getNextId();
                            long taskTypeId = idGenerator.getNextId();
                            TaskGroupLocation taskGroupLocation =
                                    new TaskGroupLocation(
                                            jobImmutableInformation.getJobId(),
                                            pipelineIndex,
                                            taskGroupID);
                            TaskLocation taskLocation =
                                    new TaskLocation(taskGroupLocation, taskTypeId, 0);
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

    private List<PhysicalVertex> getSourceTask(
            List<ExecutionEdge> edges,
            List<SourceAction<?, ?, ?>> sources,
            int pipelineIndex,
            int totalPipelineNum) {
        return sources.stream()
                .map(s -> new PhysicalExecutionFlow(s, getNextWrapper(edges, s)))
                .flatMap(
                        flow -> {
                            List<PhysicalVertex> t = new ArrayList<>();
                            List<Flow> flows = new ArrayList<>(Collections.singletonList(flow));
                            if (sourceWithSink(flow)) {
                                flows.addAll(splitSinkFromFlow(flow));
                            }
                            long taskGroupIDPrefix = idGenerator.getNextId();
                            Map<Long, Long> flowTaskIDPrefixMap = new HashMap<>();
                            for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                                int finalParallelismIndex = i;
                                long taskGroupID = mixIDPrefixAndIndex(taskGroupIDPrefix, i);
                                TaskGroupLocation taskGroupLocation =
                                        new TaskGroupLocation(
                                                jobImmutableInformation.getJobId(),
                                                pipelineIndex,
                                                taskGroupID);
                                List<SeaTunnelTask> taskList =
                                        flows.stream()
                                                .map(
                                                        f -> {
                                                            setFlowConfig(f);
                                                            long taskIDPrefix =
                                                                    flowTaskIDPrefixMap
                                                                            .computeIfAbsent(
                                                                                    f.getFlowID(),
                                                                                    id ->
                                                                                            idGenerator
                                                                                                    .getNextId());
                                                            final TaskLocation taskLocation =
                                                                    new TaskLocation(
                                                                            taskGroupLocation,
                                                                            taskIDPrefix,
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
                                                                        f);
                                                            }
                                                        })
                                                .peek(this::fillCheckpointPlan)
                                                .collect(Collectors.toList());
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

        if (f instanceof PhysicalExecutionFlow) {
            PhysicalExecutionFlow<?, FlowConfig> flow = (PhysicalExecutionFlow<?, FlowConfig>) f;
            if (flow.getAction() instanceof SourceAction) {
                SourceConfig config = new SourceConfig();
                config.setEnumeratorTask(
                        enumeratorTaskIDMap.get((SourceAction<?, ?, ?>) flow.getAction()));
                flow.setConfig(config);
            } else if (flow.getAction() instanceof SinkAction) {
                SinkConfig config = new SinkConfig();
                if (committerTaskIDMap.containsKey((SinkAction<?, ?, ?, ?>) flow.getAction())) {
                    config.setContainCommitter(true);
                    config.setCommitterTask(
                            committerTaskIDMap.get((SinkAction<?, ?, ?, ?>) flow.getAction()));
                }
                flow.setConfig(config);
            }
        } else if (f instanceof IntermediateExecutionFlow) {
            ((IntermediateExecutionFlow<IntermediateQueueConfig>) f)
                    .setConfig(
                            new IntermediateQueueConfig(
                                    ((IntermediateExecutionFlow<?>) f).getQueue().getId()));
        } else {
            throw new UnknownFlowException(f);
        }

        if (!f.getNext().isEmpty()) {
            f.getNext().forEach(this::setFlowConfig);
        }
    }

    /**
     * Use Java Queue to split flow which source to sink without partition transform
     *
     * @param flow need to be split flow
     * @return flows after split
     */
    private static List<Flow> splitSinkFromFlow(Flow flow) {
        List<PhysicalExecutionFlow<?, ?>> sinkFlows =
                flow.getNext().stream()
                        .filter(f -> f instanceof PhysicalExecutionFlow)
                        .map(f -> (PhysicalExecutionFlow<?, ?>) f)
                        .filter(f -> f.getAction() instanceof SinkAction)
                        .collect(Collectors.toList());
        List<Flow> allFlows = new ArrayList<>();
        flow.getNext().removeAll(sinkFlows);
        sinkFlows.forEach(
                s -> {
                    IntermediateQueue queue =
                            new IntermediateQueue(
                                    s.getAction().getId(),
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

        if (flow.getNext().size() > sinkFlows.size()) {
            allFlows.addAll(
                    flow.getNext().stream()
                            .flatMap(f -> splitSinkFromFlow(f).stream())
                            .collect(Collectors.toList()));
        }
        return allFlows;
    }

    private static boolean sourceWithSink(PhysicalExecutionFlow<?, ?> flow) {
        return flow.getAction() instanceof SinkAction
                || flow.getNext().stream()
                        .map(f -> (PhysicalExecutionFlow<?, ?>) f)
                        .map(PhysicalPlanGenerator::sourceWithSink)
                        .collect(Collectors.toList())
                        .contains(true);
    }

    private long mixIDPrefixAndIndex(long idPrefix, int index) {
        return idPrefix * 10000 + index;
    }

    private List<Flow> getNextWrapper(List<ExecutionEdge> edges, Action start) {
        List<Action> actions =
                edges.stream()
                        .filter(e -> e.getLeftVertex().getAction().equals(start))
                        .map(e -> e.getRightVertex().getAction())
                        .collect(Collectors.toList());
        List<Flow> wrappers =
                actions.stream()
                        .filter(a -> a instanceof ShuffleAction || a instanceof SinkAction)
                        .map(PhysicalExecutionFlow::new)
                        .collect(Collectors.toList());
        wrappers.addAll(
                actions.stream()
                        .filter(a -> !(a instanceof ShuffleAction || a instanceof SinkAction))
                        .map(a -> new PhysicalExecutionFlow<>(a, getNextWrapper(edges, a)))
                        .collect(Collectors.toList()));
        return wrappers;
    }
}
