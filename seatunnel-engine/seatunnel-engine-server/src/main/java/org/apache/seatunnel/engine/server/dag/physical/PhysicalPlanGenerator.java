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
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.JavaQueueAction;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.PhysicalSourceAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionEdge;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.TaskGroupInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PhysicalPlanGenerator {

    private final List<List<ExecutionEdge>> edgesList;

    public PhysicalPlanGenerator(ExecutionPlan executionPlan) {
        edgesList = executionPlan.getPipelines().stream().map(Pipeline::getEdges).collect(Collectors.toList());
    }

    public PhysicalPlan generate() {

        // TODO Determine which tasks do not need to be restored according to state
        return new PhysicalPlan(edgesList.stream().map(edges -> {
            List<PhysicalSourceAction<?, ?, ?>> sources =
                    edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof PhysicalSourceAction)
                            .map(s -> (PhysicalSourceAction<?, ?, ?>) s.getLeftVertex().getAction())
                            .collect(Collectors.toList());

            IdGenerator idGenerator = new IdGenerator();
            // Source Split Enumerator
            List<TaskGroupInfo> coordinatorTasks =
                    sources.stream().map(s -> {
                        SourceSplitEnumeratorTask t = new SourceSplitEnumeratorTask(idGenerator.getNextId(), s);
                        return new TaskGroupInfo(new TaskGroup(t), t.getJarsUrl());
                    }).collect(Collectors.toList());
            // Source Task
            List<TaskGroupInfo> tasks = sources.stream()
                    .map(s -> new PhysicalExecutionFlow(s, getNextWrapper(edges, s)))
                    .flatMap(flow -> {
                        List<TaskGroupInfo> t = new ArrayList<>();
                        List<PhysicalExecutionFlow> flows = Collections.singletonList(flow);
                        if (sourceWithSink(flow)) {
                            flows = splitSinkFromFlow(flow);
                        }
                        for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                            t.addAll(flows.stream().map(f -> {
                                SeaTunnelTask seaTunnelTask = new SeaTunnelTask(idGenerator.getNextId(), f);
                                return new TaskGroupInfo(new TaskGroup(seaTunnelTask), seaTunnelTask.getJarsUrl());
                            }).collect(Collectors.toList()));
                        }
                        return t.stream();
                    }).collect(Collectors.toList());

            // Queue Task
            List<TaskGroupInfo> fromPartition =
                    edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof PartitionTransformAction)
                            .map(q -> (PartitionTransformAction) q.getLeftVertex().getAction())
                            .map(q -> new PhysicalExecutionFlow(q, getNextWrapper(edges, q)))
                            .flatMap(flow -> {
                                List<TaskGroupInfo> t = new ArrayList<>();
                                for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                                    SeaTunnelTask seaTunnelTask = new SeaTunnelTask(idGenerator.getNextId(), flow);
                                    t.add(new TaskGroupInfo(new TaskGroup(seaTunnelTask), seaTunnelTask.getJarsUrl()));
                                }
                                return t.stream();
                            }).collect(Collectors.toList());
            tasks.addAll(fromPartition);

            // Aggregated Committer
            coordinatorTasks.addAll(edges.stream().filter(s -> s.getRightVertex().getAction() instanceof SinkAction)
                    .map(s -> (SinkAction<?, ?, ?, ?>) s.getRightVertex().getAction())
                    .map(s -> {
                        SinkAggregatedCommitterTask t =
                                new SinkAggregatedCommitterTask(idGenerator.getNextId(), s);
                        return new TaskGroupInfo(new TaskGroup(t), t.getJarsUrl());
                    }).collect(Collectors.toList()));

            return new PhysicalPlan.SubPlan(tasks, coordinatorTasks);
        }).collect(Collectors.toList()));
    }

    private static List<PhysicalExecutionFlow> splitSinkFromFlow(PhysicalExecutionFlow flow) {
        List<PhysicalExecutionFlow> sinkFlows =
                flow.getNext().stream().filter(f -> f.getAction() instanceof SinkAction).collect(Collectors.toList());
        List<PhysicalExecutionFlow> allFlows = new ArrayList<>();
        flow.getNext().removeAll(sinkFlows);
        sinkFlows.forEach(s -> {
            PhysicalExecutionFlow queue =
                    new PhysicalExecutionFlow(new JavaQueueAction(s.getAction().getId(),
                            s.getAction().getName() + "-Queue"));
            flow.getNext().add(queue);
            PhysicalExecutionFlow queueQuote =
                    new PhysicalExecutionFlow(new JavaQueueAction(s.getAction().getId(),
                            s.getAction().getName() + "-Queue"));
            queueQuote.getNext().add(s);
            allFlows.add(queueQuote);
        });

        if (flow.getNext().size() > sinkFlows.size()) {
            allFlows.addAll(flow.getNext().stream().flatMap(f -> splitSinkFromFlow(f).stream()).collect(Collectors.toList()));
        }
        return allFlows;
    }

    private static boolean sourceWithSink(PhysicalExecutionFlow flow) {
        return flow.getAction() instanceof SinkAction ||
                flow.getNext().stream().map(PhysicalPlanGenerator::sourceWithSink)
                        .collect(Collectors.toList()).contains(true);
    }

    private List<PhysicalExecutionFlow> getNextWrapper(List<ExecutionEdge> edges, Action start) {
        List<Action> actions = edges.stream().filter(e -> e.getLeftVertex().getAction().equals(start))
                .map(e -> e.getLeftVertex().getAction()).collect(Collectors.toList());
        List<PhysicalExecutionFlow> wrappers = actions.stream()
                .filter(a -> a instanceof PartitionTransformAction || a instanceof SinkAction)
                .map(PhysicalExecutionFlow::new).collect(Collectors.toList());
        wrappers.addAll(actions.stream()
                .filter(a -> !(a instanceof PartitionTransformAction || a instanceof SinkAction))
                .map(a -> new PhysicalExecutionFlow(a, getNextWrapper(edges, a))).collect(Collectors.toList()));
        return wrappers;
    }
}
