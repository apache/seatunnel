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

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.PhysicalSourceAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecutionPlanGenerator {

    private final Map<Integer, List<Integer>> edgeMap = new HashMap<>();
    private final Map<Integer, Action> actions = new HashMap<>();

    private final Map<Integer, ExecutionVertex> executionVertexes;
    private final List<ExecutionEdge> executionEdges;

    public ExecutionPlanGenerator(LogicalDag logicalPlan) {
        this.executionVertexes = logicalPlan.getLogicalVertexMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                vertex -> new ExecutionVertex(vertex.getValue().getVertexId(), vertex.getValue().getAction(),
                        vertex.getValue().getParallelism())));
        this.executionEdges = logicalPlan.getEdges().stream()
                .map(edge -> new ExecutionEdge(executionVertexes.get(edge.getLeftVertexId()),
                        executionVertexes.get(edge.getRightVertexId()))).collect(Collectors.toList());
    }

    public ExecutionPlan generate() {

        if (executionVertexes == null) {
            throw new IllegalArgumentException("ExecutionPlan Builder must have LogicalPlan and Action");
        }
        List<ExecutionVertex> next = executionVertexes.values().stream().filter(a -> a.getAction() instanceof SourceAction)
                .collect(Collectors.toList());
        while (!next.isEmpty()) {
            List<ExecutionVertex> newNext = new ArrayList<>();
            next.forEach(n -> {
                List<ExecutionVertex> nextVertex = convertExecutionActions(executionEdges, n.getAction());
                newNext.addAll(nextVertex);
            });
            next = newNext;
        }

        Map<Integer, ExecutionVertex> vertexes = new HashMap<>();
        actions.forEach((key, value) -> vertexes.put(key, new ExecutionVertex(key, value,
                executionVertexes.get(key).getParallelism())));

        return new ExecutionPlan(PipelineGenerator.generatePipelines(edgeMap.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(d -> new ExecutionEdge(vertexes.get(e.getKey()),
                        vertexes.get(d))))
                .collect(Collectors.toList())));
    }

    private List<ExecutionVertex> convertExecutionActions(List<ExecutionEdge> executionEdges, Action start) {

        Action end = start;
        Action executionAction;
        if (start instanceof PartitionTransformAction) {
            executionAction = start;
            actions.put(start.getId(), start);
        } else if (start instanceof SinkAction) {
            actions.put(start.getId(), start);
            return Collections.emptyList();
        } else {
            List<SeaTunnelTransform> transforms = new ArrayList<>();
            for (TransformAction t : findMigrateTransform(executionEdges, start)) {
                transforms.add(t.getTransform());
                end = t;
            }
            if (start instanceof SourceAction) {
                SourceAction<?, ?, ?> source = (SourceAction<?, ?, ?>) start;
                executionAction = new PhysicalSourceAction<>(start.getId(), start.getName(),
                        source.getSource(), transforms);
                actions.put(start.getId(), start);
            } else if (start instanceof TransformAction) {

                TransformAction transform = (TransformAction) start;
                transforms.add(0, transform.getTransform());
                executionAction = new TransformChainAction(start.getId(), start.getName(), transforms);
                actions.put(start.getId(), executionAction);
            } else {
                throw new UnknownActionException(start);
            }
        }

        final Action e = end;
        // find next should be converted action
        List<ExecutionVertex> nextStarts = executionEdges.stream().filter(edge -> edge.getLeftVertex().getAction().equals(e))
                .map(ExecutionEdge::getRightVertex).collect(Collectors.toList());
        for (ExecutionVertex n : nextStarts) {
            if (!edgeMap.containsKey(executionAction.getId())) {
                edgeMap.put(executionAction.getId(), new ArrayList<>());
            }
            edgeMap.get(executionAction.getId()).add(n.getAction().getId());
        }
        return nextStarts;
    }

    private List<TransformAction> findMigrateTransform(List<ExecutionEdge> executionEdges, Action start) {
        List<Action> actionAfterStart =
                executionEdges.stream().filter(edge -> edge.getLeftVertex().getAction().equals(start))
                        .map(edge -> edge.getRightVertex().getAction()).collect(Collectors.toList());
        // make sure the start's next only have one LogicalTransform, so it can be migrated.
        if (actionAfterStart.size() != 1 || actionAfterStart.get(0) instanceof PartitionTransformAction ||
                actionAfterStart.get(0) instanceof SinkAction) {
            return Collections.emptyList();
        } else {
            List<TransformAction> transforms = new ArrayList<>();
            transforms.add((TransformAction) actionAfterStart.get(0));
            transforms.addAll(findMigrateTransform(executionEdges, actionAfterStart.get(0)));
            return transforms;
        }
    }
}
