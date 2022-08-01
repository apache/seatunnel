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

import org.apache.seatunnel.engine.core.dag.Edge;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.PhysicalSourceAction;
import org.apache.seatunnel.engine.core.dag.actions.QueueAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.dag.pipeline.Pipelines;
import org.apache.seatunnel.engine.server.task.CoordinatorTask;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PhysicalPlanGenerator {

    private final List<List<Edge>> edgesList;

    public PhysicalPlanGenerator(Pipelines pipeline) {
        edgesList = pipeline.getPipelines().stream().map(Pipelines.Pipeline::getEdges).collect(Collectors.toList());
    }

    public PhysicalPlan generate() {

        // TODO Determine which tasks do not need to be restored according to state
        return new PhysicalPlan(edgesList.stream().map(edges -> {
            List<PhysicalSourceAction<?, ?, ?>> sources =
                    edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof PhysicalSourceAction)
                            .map(s -> (PhysicalSourceAction<?, ?, ?>) s.getLeftVertex().getAction())
                            .collect(Collectors.toList());

            // Source Split Enumerator
            List<CoordinatorTask> coordinatorTasks =
                    sources.stream().map(SourceSplitEnumeratorTask::new).collect(Collectors.toList());
            // Source Task
            List<SeaTunnelTask> tasks = sources.stream()
                    .map(s -> new ActionWrapper(s, getNextWrapper(edges, s)))
                    .flatMap(actionWrapper -> {
                        List<SeaTunnelTask> t = new ArrayList<>();
                        for (int i = 0; i < actionWrapper.getAction().getParallelism(); i++) {
                            t.add(new SeaTunnelTask(actionWrapper));
                        }
                        return t.stream();
                    }).collect(Collectors.toList());

            // Queue Task
            List<SeaTunnelTask> fromQueue =
                    edges.stream().filter(s -> s.getLeftVertex().getAction() instanceof QueueAction)
                            .map(q -> (QueueAction) q.getLeftVertex().getAction())
                            .map(q -> new ActionWrapper(q, getNextWrapper(edges, q)))
                            .flatMap(actionWrapper -> {
                                List<SeaTunnelTask> t = new ArrayList<>();
                                for (int i = 0; i < actionWrapper.getAction().getParallelism(); i++) {
                                    t.add(new SeaTunnelTask(actionWrapper));
                                }
                                return t.stream();
                            }).collect(Collectors.toList());
            tasks.addAll(fromQueue);

            // Aggregated Committer
            coordinatorTasks.addAll(edges.stream().filter(s -> s.getRightVertex().getAction() instanceof SinkAction)
                    .map(s -> (SinkAction<?, ?, ?, ?>) s.getRightVertex().getAction())
                    .map(SinkAggregatedCommitterTask::new).collect(Collectors.toList()));

            return new PhysicalPlan.SubPlan(tasks, coordinatorTasks);
        }).collect(Collectors.toList()));
    }

    private List<ActionWrapper> getNextWrapper(List<Edge> edges, Action start) {
        List<Action> actions = edges.stream().filter(e -> e.getLeftVertex().getAction().equals(start))
                .map(e -> e.getLeftVertex().getAction()).collect(Collectors.toList());
        List<ActionWrapper> wrappers = actions.stream()
                .filter(a -> a instanceof QueueAction || a instanceof SinkAction)
                .map(ActionWrapper::new).collect(Collectors.toList());
        wrappers.addAll(actions.stream()
                .filter(a -> !(a instanceof QueueAction || a instanceof SinkAction))
                .map(a -> new ActionWrapper(a, getNextWrapper(edges, a))).collect(Collectors.toList()));
        return wrappers;
    }
}
