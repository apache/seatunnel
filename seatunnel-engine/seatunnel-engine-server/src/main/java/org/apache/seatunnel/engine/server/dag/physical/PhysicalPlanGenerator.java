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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.internal.IntermediateQueue;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionEdge;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
import org.apache.seatunnel.engine.server.dag.physical.config.FlowConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.PartitionConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SinkConfig;
import org.apache.seatunnel.engine.server.dag.physical.config.SourceConfig;
import org.apache.seatunnel.engine.server.dag.physical.flow.Flow;
import org.apache.seatunnel.engine.server.dag.physical.flow.IntermediateExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.PhysicalExecutionFlow;
import org.apache.seatunnel.engine.server.dag.physical.flow.UnknownFlowException;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.task.MiddleSeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.TaskGroupInfo;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PhysicalPlanGenerator {

    private final List<List<ExecutionEdge>> edgesList;

    private final NodeEngine nodeEngine;

    private final IdGenerator idGenerator = new IdGenerator();

    /**
     * Save the enumerator task ID corresponding to source
     */
    private final Map<SourceAction<?, ?, ?>, Integer> enumeratorTaskIDMap = new HashMap<>();
    /**
     * Save the committer task ID corresponding to sink
     */
    private final Map<SinkAction<?, ?, ?, ?>, Integer> committerTaskIDMap = new HashMap<>();

    public PhysicalPlanGenerator(ExecutionPlan executionPlan, NodeEngine nodeEngine) {
        edgesList = executionPlan.getPipelines().stream().map(Pipeline::getEdges).collect(Collectors.toList());
        this.nodeEngine = nodeEngine;
    }

    public PhysicalPlan generate() {

        // TODO Determine which tasks do not need to be restored according to state
        return new PhysicalPlan(edgesList.stream().map(edges -> {
            List<SourceAction<?, ?, ?>> sources = findSourceAction(edges);

            List<TaskGroupInfo> coordinatorTasks = getEnumeratorTask(sources);

            coordinatorTasks.addAll(getCommitterTask(edges));

            List<TaskGroupInfo> tasks = getSourceTask(edges, sources);

            tasks.addAll(getPartitionTask(edges));

            return new PhysicalPlan.SubPlan(tasks, coordinatorTasks);
        }).collect(Collectors.toList()));
    }

    private List<SourceAction<?, ?, ?>> findSourceAction(List<ExecutionEdge> edges) {
        return edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof SourceAction)
                .map(s -> (SourceAction<?, ?, ?>) s.getLeftVertex().getAction())
                .collect(Collectors.toList());
    }

    private List<TaskGroupInfo> getCommitterTask(List<ExecutionEdge> edges) {
        return edges.stream().filter(s -> s.getRightVertex().getAction() instanceof SinkAction)
                .map(s -> (SinkAction<?, ?, ?, ?>) s.getRightVertex().getAction())
                .map(s -> {
                    Optional<? extends SinkAggregatedCommitter<?, ?>> sinkAggregatedCommitter;
                    try {
                        sinkAggregatedCommitter = s.getSink().createAggregatedCommitter();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    // if sinkAggregatedCommitter is empty, don't create task.
                    if (sinkAggregatedCommitter.isPresent()) {
                        SinkAggregatedCommitterTask<?> t =
                                new SinkAggregatedCommitterTask(idGenerator.getNextId(), s,
                                        sinkAggregatedCommitter.get());
                        committerTaskIDMap.put(s, t.getTaskID().intValue());
                        return new TaskGroupInfo(toData(new TaskGroup(t)), t.getJarsUrl());
                    } else {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private List<TaskGroupInfo> getPartitionTask(List<ExecutionEdge> edges) {
        return edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof PartitionTransformAction)
                .map(q -> (PartitionTransformAction) q.getLeftVertex().getAction())
                .map(q -> new PhysicalExecutionFlow<>(q, getNextWrapper(edges, q)))
                .flatMap(flow -> {
                    List<TaskGroupInfo> t = new ArrayList<>();
                    for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                        SeaTunnelTask<?> seaTunnelTask = new MiddleSeaTunnelTask(idGenerator.getNextId(),
                                flow);
                        t.add(new TaskGroupInfo(toData(new TaskGroup(seaTunnelTask)),
                                seaTunnelTask.getJarsUrl()));
                    }
                    return t.stream();
                }).collect(Collectors.toList());
    }

    private List<TaskGroupInfo> getEnumeratorTask(List<SourceAction<?, ?, ?>> sources) {
        return sources.stream().map(s -> {
            SourceSplitEnumeratorTask<?> t = new SourceSplitEnumeratorTask<>(idGenerator.getNextId(), s);
            enumeratorTaskIDMap.put(s, t.getTaskID().intValue());
            return new TaskGroupInfo(toData(new TaskGroup(t)), t.getJarsUrl());
        }).collect(Collectors.toList());
    }

    private List<TaskGroupInfo> getSourceTask(List<ExecutionEdge> edges,
                                              List<SourceAction<?, ?, ?>> sources) {
        return sources.stream()
                .map(s -> new PhysicalExecutionFlow<SourceAction<?, ?, ?>, SourceConfig>(s,
                        getNextWrapper(edges, s)))
                .flatMap(flow -> {
                    List<TaskGroupInfo> t = new ArrayList<>();
                    List<Flow> flows = new ArrayList<>(Collections.singletonList(flow));
                    // TODO move source split sink logic to seatunnel source task
                    if (sourceWithSink(flow)) {
                        flows.addAll(splitSinkFromFlow(flow));
                    }
                    for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                        int finalParallelismIndex = i;
                        List<SeaTunnelTask<?>> taskList =
                                flows.stream().map(f -> {
                                    setFlowConfig(f, finalParallelismIndex);
                                    return new SourceSeaTunnelTask<>(idGenerator.getNextId(), f);
                                }).collect(Collectors.toList());
                        Set<URL> jars =
                                taskList.stream().flatMap(task -> task.getJarsUrl().stream()).collect(Collectors.toSet());
                        t.add(new TaskGroupInfo(toData(new TaskGroup(taskList.stream().map(task -> (Task) task).collect(Collectors.toList()))), jars));
                    }
                    return t.stream();
                }).collect(Collectors.toList());
    }

    /**
     * set config for flow, some flow should have config support for execute on task.
     *
     * @param f                flow
     * @param parallelismIndex the parallelism index of flow
     */
    @SuppressWarnings("unchecked")
    private void setFlowConfig(Flow f, int parallelismIndex) {

        if (f instanceof PhysicalExecutionFlow) {
            PhysicalExecutionFlow<?, FlowConfig> flow = (PhysicalExecutionFlow<?, FlowConfig>) f;
            if (flow.getAction() instanceof SourceAction) {
                SourceConfig config = new SourceConfig();
                config.setEnumeratorTaskID(enumeratorTaskIDMap.get((SourceAction<?, ?, ?>) flow.getAction()));
                flow.setConfig(config);
            } else if (flow.getAction() instanceof SinkAction) {
                SinkConfig config = new SinkConfig();
                if (committerTaskIDMap.containsKey((SinkAction<?, ?, ?, ?>) flow.getAction())) {
                    config.setContainCommitter(true);
                    config.setCommitterTaskID(committerTaskIDMap.get((SinkAction<?, ?, ?, ?>) flow.getAction()));
                }
                flow.setConfig(config);
            } else if (flow.getAction() instanceof PartitionTransformAction) {
                PartitionConfig config =
                        new PartitionConfig(((PartitionTransformAction) flow.getAction()).getPartitionTransformation().getPartitionCount(),
                                ((PartitionTransformAction) flow.getAction()).getPartitionTransformation().getTargetCount(),
                                parallelismIndex);
                flow.setConfig(config);
            }

        } else if (f instanceof IntermediateExecutionFlow) {
            // TODO remove it after move to seatunnel source task
            IntermediateExecutionFlow flow = (IntermediateExecutionFlow) f;
        } else {
            throw new UnknownFlowException(f);
        }

        if (!f.getNext().isEmpty()) {
            f.getNext().forEach(n -> setFlowConfig(n, parallelismIndex));
        }

    }

    private static List<Flow> splitSinkFromFlow(Flow flow) {
        List<PhysicalExecutionFlow<?, ?>> sinkFlows =
                flow.getNext().stream().filter(f -> f instanceof PhysicalExecutionFlow).map(f -> (PhysicalExecutionFlow<?, ?>) f)
                        .filter(f -> f.getAction() instanceof SinkAction).collect(Collectors.toList());
        List<Flow> allFlows = new ArrayList<>();
        flow.getNext().removeAll(sinkFlows);
        sinkFlows.forEach(s -> {
            IntermediateQueue queue = new IntermediateQueue(s.getAction().getId(),
                    s.getAction().getName() + "-Queue", s.getAction().getParallelism());
            IntermediateExecutionFlow intermediateFlow = new IntermediateExecutionFlow(queue);
            flow.getNext().add(intermediateFlow);
            IntermediateExecutionFlow intermediateFlowQuote = new IntermediateExecutionFlow(queue);
            intermediateFlowQuote.getNext().add(s);
            allFlows.add(intermediateFlowQuote);
        });

        if (flow.getNext().size() > sinkFlows.size()) {
            allFlows.addAll(flow.getNext().stream().flatMap(f -> splitSinkFromFlow(f).stream()).collect(Collectors.toList()));
        }
        return allFlows;
    }

    private static boolean sourceWithSink(PhysicalExecutionFlow<?, ?> flow) {
        return flow.getAction() instanceof SinkAction ||
                flow.getNext().stream().map(f -> (PhysicalExecutionFlow<?, ?>) f).map(PhysicalPlanGenerator::sourceWithSink)
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
                .map(a -> new PhysicalExecutionFlow<>(a, getNextWrapper(edges, a))).collect(Collectors.toList()));
        return wrappers;
    }

    private Data toData(Object object) {
        return this.nodeEngine.getSerializationService().toData(object);
    }
}
