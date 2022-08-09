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

import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.common.utils.NonCompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.PhysicalSourceAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.internal.IntermediateDataQueue;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineState;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionEdge;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;

import com.google.common.collect.Lists;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PhysicalPlanGenerator {

    private final List<List<ExecutionEdge>> edgesList;

    private final NodeEngine nodeEngine;

    private final IdGenerator idGenerator = new IdGenerator();

    private final JobImmutableInformation jobImmutableInformation;

    private final long initializationTimestamp;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    public PhysicalPlanGenerator(@NonNull ExecutionPlan executionPlan,
                                 @NonNull NodeEngine nodeEngine,
                                 @NonNull JobImmutableInformation jobImmutableInformation,
                                 long initializationTimestamp,
                                 @NonNull ExecutorService executorService,
                                 @NonNull FlakeIdGenerator flakeIdGenerator) {
        edgesList = executionPlan.getPipelines().stream().map(Pipeline::getEdges).collect(Collectors.toList());
        this.nodeEngine = nodeEngine;
        this.jobImmutableInformation = jobImmutableInformation;
        this.initializationTimestamp = initializationTimestamp;
        this.executorService = executorService;
        this.flakeIdGenerator = flakeIdGenerator;
    }

    public PhysicalPlan generate() {

        // TODO Determine which tasks do not need to be restored according to state
        AtomicInteger index = new AtomicInteger(-1);
        CopyOnWriteArrayList<NonCompletableFuture<PipelineState>> waitForCompleteBySubPlanList =
            new CopyOnWriteArrayList<>();

        Stream<SubPlan> subPlanStream = edgesList.stream().map(edges -> {
            int currIndex = index.incrementAndGet();
            CopyOnWriteArrayList<NonCompletableFuture<TaskExecutionState>> waitForCompleteByPhysicalVertexList =
                new CopyOnWriteArrayList<>();
            List<PhysicalSourceAction<?, ?, ?>> sources = findSourceAction(edges);

            List<PhysicalVertex> coordinatorVertexList =
                getEnumeratorTask(sources, currIndex, edgesList.size(), waitForCompleteByPhysicalVertexList);

            List<PhysicalVertex> physicalVertexList =
                getSourceTask(edges, sources, currIndex, edgesList.size(), waitForCompleteByPhysicalVertexList);

            physicalVertexList.addAll(
                getPartitionTask(edges, currIndex, edgesList.size(), waitForCompleteByPhysicalVertexList));

            coordinatorVertexList.addAll(
                getCommitterTask(edges, currIndex, edgesList.size(), waitForCompleteByPhysicalVertexList));

            CompletableFuture<PipelineState> pipelineFuture = new CompletableFuture<>();
            waitForCompleteBySubPlanList.add(new NonCompletableFuture<>(pipelineFuture));

            return new SubPlan(currIndex,
                edgesList.size(),
                initializationTimestamp,
                physicalVertexList,
                coordinatorVertexList,
                pipelineFuture,
                waitForCompleteByPhysicalVertexList.toArray(
                    new NonCompletableFuture[waitForCompleteByPhysicalVertexList.size()]));
        });

        PhysicalPlan physicalPlan = new PhysicalPlan(subPlanStream.collect(Collectors.toList()),
            executorService,
            jobImmutableInformation,
            initializationTimestamp,
            waitForCompleteBySubPlanList.toArray(new NonCompletableFuture[waitForCompleteBySubPlanList.size()]));
        return physicalPlan;
    }

    private List<PhysicalSourceAction<?, ?, ?>> findSourceAction(List<ExecutionEdge> edges) {
        return edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof PhysicalSourceAction)
            .map(s -> (PhysicalSourceAction<?, ?, ?>) s.getLeftVertex().getAction())
            .collect(Collectors.toList());
    }

    private List<PhysicalVertex> getCommitterTask(List<ExecutionEdge> edges,
                                                  int pipelineIndex,
                                                  int totalPipelineNum,
                                                  CopyOnWriteArrayList<NonCompletableFuture<TaskExecutionState>> waitForCompleteByPhysicalVertexList) {
        AtomicInteger atomicInteger = new AtomicInteger(-1);
        List<ExecutionEdge> collect = edges.stream().filter(s -> s.getRightVertex().getAction() instanceof SinkAction)
            .collect(Collectors.toList());

        return collect.stream().map(s -> (SinkAction<?, ?, ?, ?>) s.getRightVertex().getAction())
            .map(s -> {
                SinkAggregatedCommitterTask t =
                    new SinkAggregatedCommitterTask(idGenerator.getNextId(), s);

                CompletableFuture<TaskExecutionState> taskFuture = new CompletableFuture<>();
                waitForCompleteByPhysicalVertexList.add(new NonCompletableFuture<>(taskFuture));

                return new PhysicalVertex(idGenerator.getNextId(),
                    atomicInteger.incrementAndGet(),
                    executorService,
                    collect.size(),
                    new TaskGroup("SinkAggregatedCommitterTask", Lists.newArrayList(t)),
                    taskFuture,
                    flakeIdGenerator,
                    pipelineIndex,
                    totalPipelineNum,
                    null);
            }).collect(Collectors.toList());
    }

    private List<PhysicalVertex> getPartitionTask(List<ExecutionEdge> edges,
                                                  int pipelineIndex,
                                                  int totalPipelineNum,
                                                  CopyOnWriteArrayList<NonCompletableFuture<TaskExecutionState>> waitForCompleteByPhysicalVertexList) {
        return edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof PartitionTransformAction)
            .map(q -> (PartitionTransformAction) q.getLeftVertex().getAction())
            .map(q -> new PhysicalExecutionFlow(q, getNextWrapper(edges, q)))
            .flatMap(flow -> {
                List<PhysicalVertex> t = new ArrayList<>();
                for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                    SeaTunnelTask seaTunnelTask = new SeaTunnelTask(idGenerator.getNextId(), flow);

                    CompletableFuture<TaskExecutionState> taskFuture = new CompletableFuture<>();
                    waitForCompleteByPhysicalVertexList.add(new NonCompletableFuture<>(taskFuture));

                    t.add(new PhysicalVertex(idGenerator.getNextId(),
                        i,
                        executorService,
                        flow.getAction().getParallelism(),
                        new TaskGroup("PartitionTransformTask", Lists.newArrayList(seaTunnelTask)),
                        taskFuture,
                        flakeIdGenerator,
                        pipelineIndex,
                        totalPipelineNum,
                        seaTunnelTask.getJarsUrl()));
                }
                return t.stream();
            }).collect(Collectors.toList());
    }

    private List<PhysicalVertex> getEnumeratorTask(List<PhysicalSourceAction<?, ?, ?>> sources,
                                                   int pipelineIndex,
                                                   int totalPipelineNum,
                                                   CopyOnWriteArrayList<NonCompletableFuture<TaskExecutionState>> waitForCompleteByPhysicalVertexList) {
        AtomicInteger atomicInteger = new AtomicInteger(-1);

        return sources.stream().map(s -> {
            SourceSplitEnumeratorTask<?> t = new SourceSplitEnumeratorTask<>(idGenerator.getNextId(), s);
            CompletableFuture<TaskExecutionState> taskFuture = new CompletableFuture<>();
            waitForCompleteByPhysicalVertexList.add(new NonCompletableFuture<>(taskFuture));

            return new PhysicalVertex(idGenerator.getNextId(),
                atomicInteger.incrementAndGet(),
                executorService,
                sources.size(),
                new TaskGroup(s.getName(), Lists.newArrayList(t)),
                taskFuture,
                flakeIdGenerator,
                pipelineIndex,
                totalPipelineNum,
                t.getJarsUrl());
        }).collect(Collectors.toList());
    }

    private List<PhysicalVertex> getSourceTask(List<ExecutionEdge> edges,
                                               List<PhysicalSourceAction<?, ?, ?>> sources,
                                               int pipelineIndex,
                                               int totalPipelineNum,
                                               CopyOnWriteArrayList<NonCompletableFuture<TaskExecutionState>> waitForCompleteByPhysicalVertexList) {
        return sources.stream()
            .map(s -> new PhysicalExecutionFlow(s, getNextWrapper(edges, s)))
            .flatMap(flow -> {
                List<PhysicalVertex> t = new ArrayList<>();
                List<Flow> flows = new ArrayList<>(Collections.singletonList(flow));
                if (sourceWithSink(flow)) {
                    flows.addAll(splitSinkFromFlow(flow));
                }
                for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                    List<SeaTunnelTask> taskList =
                        flows.stream().map(f -> new SeaTunnelTask(idGenerator.getNextId(), f))
                            .collect(Collectors.toList());
                    Set<URL> jars =
                        taskList.stream().flatMap(task -> task.getJarsUrl().stream()).collect(Collectors.toSet());

                    CompletableFuture<TaskExecutionState> taskFuture = new CompletableFuture<>();
                    waitForCompleteByPhysicalVertexList.add(new NonCompletableFuture<>(taskFuture));

                    // TODO We need give every task a appropriate name
                    t.add(new PhysicalVertex(idGenerator.getNextId(),
                        i,
                        executorService,
                        flow.getAction().getParallelism(),
                        new TaskGroup("SourceTask",
                            taskList.stream().map(task -> (Task) task).collect(Collectors.toList())),
                        taskFuture,
                        flakeIdGenerator,
                        pipelineIndex,
                        totalPipelineNum,
                        jars));
                }
                return t.stream();
            }).collect(Collectors.toList());
    }

    private static List<Flow> splitSinkFromFlow(Flow flow) {
        List<PhysicalExecutionFlow> sinkFlows =
            flow.getNext().stream().filter(f -> f instanceof PhysicalExecutionFlow).map(f -> (PhysicalExecutionFlow) f)
                .filter(f -> f.getAction() instanceof SinkAction).collect(Collectors.toList());
        List<Flow> allFlows = new ArrayList<>();
        flow.getNext().removeAll(sinkFlows);
        sinkFlows.forEach(s -> {
            IntermediateDataQueue queue = new IntermediateDataQueue(s.getAction().getId(),
                s.getAction().getName() + "-Queue", s.getAction().getParallelism());
            IntermediateExecutionFlow intermediateFlow = new IntermediateExecutionFlow(queue);
            flow.getNext().add(intermediateFlow);
            IntermediateExecutionFlow intermediateFlowQuote = new IntermediateExecutionFlow(queue);
            intermediateFlowQuote.getNext().add(s);
            allFlows.add(intermediateFlowQuote);
        });

        if (flow.getNext().size() > sinkFlows.size()) {
            allFlows.addAll(
                flow.getNext().stream().flatMap(f -> splitSinkFromFlow(f).stream()).collect(Collectors.toList()));
        }
        return allFlows;
    }

    private static boolean sourceWithSink(PhysicalExecutionFlow flow) {
        return flow.getAction() instanceof SinkAction ||
            flow.getNext().stream().map(f -> (PhysicalExecutionFlow) f).map(PhysicalPlanGenerator::sourceWithSink)
                .collect(Collectors.toList()).contains(true);
    }

    private List<Flow> getNextWrapper(List<ExecutionEdge> edges, Action start) {
        List<Action> actions = edges.stream().filter(e -> e.getLeftVertex().getAction().equals(start))
            .map(e -> e.getLeftVertex().getAction()).collect(Collectors.toList());
        List<Flow> wrappers = actions.stream()
            .filter(a -> a instanceof PartitionTransformAction || a instanceof SinkAction)
            .map(PhysicalExecutionFlow::new).collect(Collectors.toList());
        wrappers.addAll(actions.stream()
            .filter(a -> !(a instanceof PartitionTransformAction || a instanceof SinkAction))
            .map(a -> new PhysicalExecutionFlow(a, getNextWrapper(edges, a))).collect(Collectors.toList()));
        return wrappers;
    }

    private Data toData(Object object) {
        return this.nodeEngine.getSerializationService().toData(object);
    }
}
