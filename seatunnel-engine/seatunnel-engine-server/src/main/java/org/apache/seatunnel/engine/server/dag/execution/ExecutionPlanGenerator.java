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
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecutionPlanGenerator {

    private final Map<Integer, List<Integer>> edgeMap = new HashMap<>();
    private final Map<Integer, Action> actions = new HashMap<>();

    private final Map<Integer, LogicalVertex> logicalVertexes;
    private final List<LogicalEdge> logicalEdges;

    public ExecutionPlanGenerator(LogicalDag logicalPlan) {
        this.logicalVertexes = new HashMap<>(logicalPlan.getLogicalVertexMap());
        this.logicalEdges = new ArrayList<>(logicalPlan.getEdges());
    }

    public ExecutionPlan generate() {

        if (logicalVertexes == null) {
            throw new IllegalArgumentException("ExecutionPlan Builder must have LogicalPlan and Action");
        }
        List<LogicalVertex> next = logicalVertexes.values().stream().filter(a -> a.getAction() instanceof SourceAction)
                .collect(Collectors.toList());
        while (!next.isEmpty()) {
            List<LogicalVertex> newNext = new ArrayList<>();
            next.forEach(n -> {
                List<LogicalVertex> nextVertex = convertExecutionActions(logicalEdges, n.getAction());
                newNext.addAll(nextVertex);
            });
            next = newNext;
        }

        Map<Integer, LogicalVertex> vertexes = new HashMap<>();
        actions.forEach((key, value) -> vertexes.put(key, new LogicalVertex(key, value,
                logicalVertexes.get(key).getParallelism())));

        return new ExecutionPlan(PipelineGenerator.generatePipelines(edgeMap.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(d -> new ExecutionEdge(convertFromLogical(vertexes.get(e.getKey())),
                        convertFromLogical(vertexes.get(d)))))
                .collect(Collectors.toList())));
    }

    private ExecutionVertex convertFromLogical(LogicalVertex vertex) {
        return new ExecutionVertex(vertex.getVertexId(), vertex.getAction(), vertex.getParallelism());
    }

    private List<LogicalVertex> convertExecutionActions(List<LogicalEdge> logicalEdges, Action start) {

        Action end = start;
        Action executionAction;
        if (start instanceof PartitionTransformAction) {
            executionAction = start;
            actions.put(start.getId(), start);
        } else if (start instanceof SinkAction) {
            actions.put(start.getId(), start);
            return Collections.emptyList();
        } else if (start instanceof SourceAction) {
            executionAction = start;
            actions.put(start.getId(), start);
        } else if (start instanceof TransformAction) {

            List<SeaTunnelTransform<?>> transforms = new ArrayList<>();
            Set<URL> jars = new HashSet<>();
            for (TransformAction t : findMigrateTransform(logicalEdges, start)) {
                transforms.add(t.getTransform());
                jars.addAll(t.getJarUrls());
                end = t;
            }
            TransformAction transform = (TransformAction) start;
            jars.addAll(transform.getJarUrls());
            transforms.add(0, transform.getTransform());
            executionAction = new TransformChainAction(start.getId(), start.getName(),
                    new ArrayList<>(jars), transforms);
            actions.put(start.getId(), executionAction);
        } else {
            throw new UnknownActionException(start);
        }

        final Action e = end;
        // find next should be converted action
        List<LogicalVertex> nextStarts = logicalEdges.stream().filter(edge -> edge.getLeftVertex().getAction().equals(e))
                .map(LogicalEdge::getRightVertex).collect(Collectors.toList());
        for (LogicalVertex n : nextStarts) {
            if (!edgeMap.containsKey(executionAction.getId())) {
                edgeMap.put(executionAction.getId(), new ArrayList<>());
            }
            edgeMap.get(executionAction.getId()).add(n.getAction().getId());
        }
        return nextStarts;
    }

    private List<TransformAction> findMigrateTransform(List<LogicalEdge> executionEdges, Action start) {
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
