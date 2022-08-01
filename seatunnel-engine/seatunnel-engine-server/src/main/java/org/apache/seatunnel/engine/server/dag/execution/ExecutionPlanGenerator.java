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
import org.apache.seatunnel.engine.core.dag.Edge;
import org.apache.seatunnel.engine.core.dag.Vertex;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.PhysicalSourceAction;
import org.apache.seatunnel.engine.core.dag.actions.QueueAction;
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

    private final Map<Integer, Vertex> logicalVertexes;
    private final List<Edge> logicalEdges;

    public ExecutionPlanGenerator(LogicalDag logicalPlan) {
        this.logicalVertexes = new HashMap<>(logicalPlan.getLogicalVertexMap());
        this.logicalEdges = new ArrayList<>(logicalPlan.getEdges());
    }

    public ExecutionPlan generate() {

        if (logicalVertexes == null) {
            throw new IllegalArgumentException("ExecutionPlan Builder must have LogicalPlan and Action");
        }
        List<Vertex> next = logicalVertexes.values().stream().filter(a -> a.getAction() instanceof SourceAction)
                .collect(Collectors.toList());
        while (!next.isEmpty()) {
            List<Vertex> newNext = new ArrayList<>();
            next.forEach(n -> {
                List<Vertex> nextVertex = convertExecutionActions(logicalEdges, n.getAction());
                newNext.addAll(nextVertex);
            });
            next = newNext;
        }

        Map<Integer, Vertex> vertexes = new HashMap<>();
        actions.forEach((key, value) -> vertexes.put(key, new Vertex(key, value,
                logicalVertexes.get(key).getParallelism())));

        return new ExecutionPlan(edgeMap.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(d -> new Edge(vertexes.get(e.getKey()), vertexes.get(d))))
                .collect(Collectors.toList()), vertexes);
    }

    private List<Vertex> convertExecutionActions(List<Edge> logicalEdges, Action start) {

        Action end = start;
        Action executionAction;
        if (start instanceof PartitionTransformAction) {
            PartitionTransformAction shuffle = (PartitionTransformAction) start;
            executionAction = new QueueAction(shuffle.getId(), shuffle.getParallelism(), shuffle.getName());
            actions.put(start.getId(), executionAction);
        } else if (start instanceof SinkAction) {
            actions.put(start.getId(), start);
            return Collections.emptyList();
        } else {
            List<SeaTunnelTransform> transforms = new ArrayList<>();
            for (TransformAction t : findMigrateTransform(logicalEdges, start)) {
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
        List<Vertex> nextStarts = logicalEdges.stream().filter(edge -> edge.getLeftVertex().getAction().equals(e))
                .map(Edge::getRightVertex).collect(Collectors.toList());
        for (Vertex n : nextStarts) {
            if (!edgeMap.containsKey(executionAction.getId())) {
                edgeMap.put(executionAction.getId(), new ArrayList<>());
            }
            edgeMap.get(executionAction.getId()).add(n.getAction().getId());
        }
        return nextStarts;
    }

    private List<TransformAction> findMigrateTransform(List<Edge> logicalEdges, Action start) {
        List<Action> actionAfterStart =
                logicalEdges.stream().filter(edge -> edge.getLeftVertex().getAction().equals(start))
                        .map(edge -> edge.getRightVertex().getAction()).collect(Collectors.toList());
        // make sure the start's next only have one LogicalTransform, so it can be migrated.
        if (actionAfterStart.size() != 1 || actionAfterStart.get(0) instanceof PartitionTransformAction ||
                actionAfterStart.get(0) instanceof SinkAction) {
            return Collections.emptyList();
        } else {
            List<TransformAction> transforms = new ArrayList<>();
            transforms.add((TransformAction) actionAfterStart.get(0));
            transforms.addAll(findMigrateTransform(logicalEdges, actionAfterStart.get(0)));
            return transforms;
        }
    }
}
