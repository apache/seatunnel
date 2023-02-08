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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
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
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import lombok.NonNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecutionPlanGenerator {
    /**
     * The action ID needs to be regenerated because of the source, sink, and chain.
     */
    private final IdGenerator idGenerator = new IdGenerator();

    /**
     * key: the id of the execution vertex.
     * <br> value: the execution vertex.
     */
    private final Map<Long, ExecutionVertex> executionVertexMap = new HashMap<>();

    /**
     * key: the vertex id.
     * <br> value: The input vertices of this vertex.
     *
     * <p>When chaining vertices, it need to query whether the vertex has multiple input vertices. </p>
     */
    private final Map<Long, List<LogicalVertex>> inputVerticesMap = new HashMap<>();

    /**
     * key: the vertex id.
     * <br> value: The target vertices of this vertex.
     *
     * <p>When chaining vertices, it need to query whether the vertex has multiple target vertices. </p>
     */
    private final Map<Long, List<LogicalVertex>> targetVerticesMap = new HashMap<>();

    /**
     * key: logical vertex id.
     * <br> value: execution vertex id.
     *
     * <p>The chaining will cause multiple {@link LogicalVertex} mapping to the same {@link ExecutionVertex}. </p>
     */
    private final Map<Long, Long> logicalToExecutionMap = new HashMap<>();

    /**
     * When chaining, the edge will be removed {@link #findChainedVertices} if it can be chained.
     */
    private final List<LogicalEdge> logicalEdges;

    private final List<LogicalVertex> logicalVertices;

    private final JobImmutableInformation jobImmutableInformation;

    public ExecutionPlanGenerator(@NonNull LogicalDag logicalPlan,
                                  @NonNull JobImmutableInformation jobImmutableInformation) {
        checkArgument(logicalPlan.getEdges().size() > 0, "ExecutionPlan Builder must have LogicalPlan.");

        this.logicalEdges = new ArrayList<>(logicalPlan.getEdges());
        this.logicalVertices = new ArrayList<>(logicalPlan.getLogicalVertexMap().values());
        this.jobImmutableInformation = jobImmutableInformation;
    }

    public ExecutionPlan generate() {
        fillVerticesMap();
        getSourceVertices().forEach(sourceVertex -> {
            List<LogicalVertex> vertices = new ArrayList<>();
            vertices.add(sourceVertex);
            for (int index = 0; index < vertices.size(); index++) {
                LogicalVertex vertex = vertices.get(index);
                createExecutionVertex(vertex);
                if (targetVerticesMap.containsKey(vertex.getVertexId())) {
                    vertices.addAll(targetVerticesMap.get(vertex.getVertexId()));
                }
            }
        });
        List<ExecutionEdge> executionEdges = createExecutionEdges();
        return new ExecutionPlan(new PipelineGenerator(executionVertexMap.values(), executionEdges)
            .generatePipelines(), jobImmutableInformation);
    }

    public List<ExecutionEdge> createExecutionEdges() {
        return logicalEdges.stream()
            .map(logicalEdge -> new ExecutionEdge(
                executionVertexMap.get(logicalToExecutionMap.get(logicalEdge.getInputVertexId())),
                executionVertexMap.get(logicalToExecutionMap.get(logicalEdge.getTargetVertexId())))
            ).collect(Collectors.toList());
    }

    private void fillVerticesMap() {
        logicalEdges.forEach(logicalEdge -> {
            inputVerticesMap.computeIfAbsent(logicalEdge.getTargetVertexId(), id -> new ArrayList<>())
                .add(logicalEdge.getInputVertex());
            targetVerticesMap.computeIfAbsent(logicalEdge.getInputVertexId(), id -> new ArrayList<>())
                .add(logicalEdge.getTargetVertex());
        });
    }

    private List<LogicalVertex> getSourceVertices() {
        List<LogicalVertex> sourceVertices = new ArrayList<>();
        for (LogicalVertex logicalVertex : logicalVertices) {
            List<LogicalVertex> inputVertices = inputVerticesMap.get(logicalVertex.getVertexId());
            if (inputVertices == null || inputVertices.size() == 0) {
                sourceVertices.add(logicalVertex);
            }
        }
        return sourceVertices;
    }

    private void createExecutionVertex(LogicalVertex logicalVertex) {
        if (logicalToExecutionMap.containsKey(logicalVertex.getVertexId())) {
            return;
        }
        final ArrayList<LogicalVertex> chainedVertices = new ArrayList<>();
        findChainedVertices(logicalVertex, chainedVertices);
        final long newId = idGenerator.getNextId();
        Action newAction;
        if (chainedVertices.size() < 1) {
            newAction = recreateAction(logicalVertex.getAction(), newId, logicalVertex.getParallelism());
        } else {
            List<SeaTunnelTransform> transforms = new ArrayList<>(chainedVertices.size());
            List<String> names = new ArrayList<>(chainedVertices.size());
            Set<URL> jars = new HashSet<>();
            chainedVertices.stream()
                .peek(vertex -> logicalToExecutionMap.put(vertex.getVertexId(), newId))
                .map(LogicalVertex::getAction)
                .map(action -> (TransformAction) action)
                .forEach(action -> {
                    transforms.add(action.getTransform());
                    jars.addAll(action.getJarUrls());
                    names.add(action.getName());
                });
            newAction = new TransformChainAction(newId,
                String.join("->", names),
                jars,
                transforms);
            newAction.setParallelism(logicalVertex.getAction().getParallelism());
        }
        ExecutionVertex executionVertex = new ExecutionVertex(newId, newAction, logicalVertex.getParallelism());
        executionVertexMap.put(newId, executionVertex);
        logicalToExecutionMap.put(logicalVertex.getVertexId(), executionVertex.getVertexId());
    }

    public static Action recreateAction(Action action, Long id, int parallelism) {
        Action newAction;
        if (action instanceof PartitionTransformAction) {
            newAction = new PartitionTransformAction(id,
                action.getName(),
                ((PartitionTransformAction) action).getPartitionTransformation(),
                action.getJarUrls());
        } else if (action instanceof SinkAction) {
            newAction = new SinkAction<>(id,
                action.getName(),
                ((SinkAction<?, ?, ?, ?>) action).getSink(),
                action.getJarUrls());
        } else if (action instanceof SourceAction) {
            newAction = new SourceAction<>(id,
                action.getName(),
                ((SourceAction<?, ?, ?>) action).getSource(),
                action.getJarUrls());
        } else if (action instanceof TransformChainAction) {
            newAction = new TransformChainAction(id,
                action.getName(),
                action.getJarUrls(),
                ((TransformChainAction<?>) action).getTransforms());
        } else {
            throw new UnknownActionException(action);
        }
        newAction.setParallelism(parallelism);
        return newAction;
    }

    private void findChainedVertices(LogicalVertex logicalVertex, List<LogicalVertex> chainedVertices) {
        Action action = logicalVertex.getAction();
        // Currently only support Transform action chaining.
        if (action instanceof TransformAction) {
            if (chainedVertices.size() == 0) {
                chainedVertices.add(logicalVertex);
            } else if (inputVerticesMap.get(logicalVertex.getVertexId()).size() == 1) {
                // It cannot be chained to any input vertex if it has multiple input vertices.
                logicalEdges.remove(new LogicalEdge(chainedVertices.get(chainedVertices.size() - 1), logicalVertex));
                chainedVertices.add(logicalVertex);
            } else {
                return;
            }
        } else {
            return;
        }

        // It cannot chain to any target vertex if it has multiple target vertices.
        if (targetVerticesMap.get(logicalVertex.getVertexId()).size() == 1) {
            findChainedVertices(targetVerticesMap.get(logicalVertex.getVertexId()).get(0), chainedVertices);
        }
    }
}
