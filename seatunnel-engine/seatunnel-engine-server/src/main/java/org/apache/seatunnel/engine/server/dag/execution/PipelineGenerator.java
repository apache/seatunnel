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

import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

public class PipelineGenerator {
    /** The action & vertex ID needs to be regenerated because of split pipeline. */
    private final IdGenerator idGenerator = new IdGenerator();

    /**
     * key: the vertex id. <br>
     * value: The input vertices of this vertex.
     *
     * <p>When chaining vertices, it need to query whether the vertex has multiple input vertices.
     */
    private final Map<Long, List<ExecutionVertex>> inputVerticesMap = new HashMap<>();

    /**
     * key: the vertex id. <br>
     * value: The target vertices of this vertex.
     *
     * <p>When chaining vertices, it need to query whether the vertex has multiple target vertices.
     */
    private final Map<Long, List<ExecutionVertex>> targetVerticesMap = new HashMap<>();

    private final Collection<ExecutionVertex> vertices;

    private final List<ExecutionEdge> edges;

    public PipelineGenerator(Collection<ExecutionVertex> vertices, List<ExecutionEdge> edges) {
        this.vertices = vertices;
        this.edges = edges;
    }

    public List<Pipeline> generatePipelines() {
        List<ExecutionEdge> executionEdges = expandEdgeByParallelism(edges);

        // Split into multiple unrelated pipelines
        List<List<ExecutionEdge>> edgesList = splitUnrelatedEdges(executionEdges);

        edgesList =
                edgesList.stream()
                        .flatMap(e -> this.splitUnionEdge(e).stream())
                        .collect(Collectors.toList());

        // just convert execution plan to pipeline at now. We should split it to multi pipeline with
        // cache in the future
        IdGenerator idGenerator = new IdGenerator();
        return edgesList.stream()
                .map(
                        e -> {
                            Map<Long, ExecutionVertex> vertexes = new HashMap<>();
                            List<ExecutionEdge> pipelineEdges =
                                    e.stream()
                                            .map(
                                                    edge -> {
                                                        if (!vertexes.containsKey(
                                                                edge.getLeftVertexId())) {
                                                            vertexes.put(
                                                                    edge.getLeftVertexId(),
                                                                    edge.getLeftVertex());
                                                        }
                                                        ExecutionVertex source =
                                                                vertexes.get(
                                                                        edge.getLeftVertexId());
                                                        if (!vertexes.containsKey(
                                                                edge.getRightVertexId())) {
                                                            vertexes.put(
                                                                    edge.getRightVertexId(),
                                                                    edge.getRightVertex());
                                                        }
                                                        ExecutionVertex destination =
                                                                vertexes.get(
                                                                        edge.getRightVertexId());
                                                        return new ExecutionEdge(
                                                                source, destination);
                                                    })
                                            .collect(Collectors.toList());
                            return new Pipeline(
                                    (int) idGenerator.getNextId(), pipelineEdges, vertexes);
                        })
                .collect(Collectors.toList());
    }

    private static List<ExecutionEdge> expandEdgeByParallelism(List<ExecutionEdge> edges) {
        /*
         *TODO
         * use SupportCoordinate interface to determine whether the Pipeline needs to be split.
         * Pipelines without coordinator support can be split into multiple pipelines that do not
         * interfere with each other
         */
        return edges;
    }

    private List<List<ExecutionEdge>> splitUnionEdge(List<ExecutionEdge> edges) {
        fillVerticesMap(edges);
        if (checkCanSplit(edges)) {
            List<ExecutionVertex> sourceVertices = getSourceVertices();
            List<List<ExecutionEdge>> pipelines = new ArrayList<>();
            sourceVertices.forEach(
                    sourceVertex -> splitUnionVertex(pipelines, new ArrayList<>(), sourceVertex));
            return pipelines;
        } else {
            return Collections.singletonList(edges);
        }
    }

    /** If this execution vertex have partition transform, can't be spilt */
    private boolean checkCanSplit(List<ExecutionEdge> edges) {
        return edges.stream()
                        .noneMatch(e -> e.getRightVertex().getAction() instanceof ShuffleAction)
                && edges.stream()
                        .anyMatch(e -> inputVerticesMap.get(e.getRightVertexId()).size() > 1);
    }

    private void splitUnionVertex(
            List<List<ExecutionEdge>> pipelines,
            List<ExecutionVertex> pipeline,
            ExecutionVertex currentVertex) {
        pipeline.add(
                recreateVertex(
                        currentVertex,
                        pipeline.size() == 0
                                ? currentVertex.getParallelism()
                                : pipeline.get(pipeline.size() - 1).getParallelism()));
        List<ExecutionVertex> targetVertices = targetVerticesMap.get(currentVertex.getVertexId());
        if (targetVertices == null || targetVertices.size() == 0) {
            pipelines.add(createExecutionEdges(pipeline));
            return;
        }
        for (int i = 0; i < targetVertices.size(); i++) {
            if (i > 0) {
                pipeline = recreatePipeline(pipeline);
            }
            splitUnionVertex(pipelines, pipeline, targetVertices.get(i));
            pipeline.remove(pipeline.size() - 1);
        }
    }

    private List<ExecutionEdge> createExecutionEdges(List<ExecutionVertex> pipeline) {
        checkArgument(pipeline != null && pipeline.size() > 1);
        List<ExecutionEdge> edges = new ArrayList<>(pipeline.size() - 1);
        for (int i = 1; i < pipeline.size(); i++) {
            edges.add(new ExecutionEdge(pipeline.get(i - 1), pipeline.get(i)));
        }
        return edges;
    }

    private List<ExecutionVertex> recreatePipeline(List<ExecutionVertex> pipeline) {
        return pipeline.stream()
                .map(vertex -> recreateVertex(vertex, vertex.getParallelism()))
                .collect(Collectors.toList());
    }

    private ExecutionVertex recreateVertex(ExecutionVertex vertex, int parallelism) {
        long id = idGenerator.getNextId();
        Action action = vertex.getAction();
        return new ExecutionVertex(
                id,
                ExecutionPlanGenerator.recreateAction(action, id, parallelism),
                action instanceof ShuffleAction ? vertex.getParallelism() : parallelism);
    }

    private void fillVerticesMap(List<ExecutionEdge> edges) {
        inputVerticesMap.clear();
        targetVerticesMap.clear();
        edges.forEach(
                edge -> {
                    inputVerticesMap
                            .computeIfAbsent(edge.getRightVertexId(), id -> new ArrayList<>())
                            .add(edge.getLeftVertex());
                    targetVerticesMap
                            .computeIfAbsent(edge.getLeftVertexId(), id -> new ArrayList<>())
                            .add(edge.getRightVertex());
                });
    }

    private List<ExecutionVertex> getSourceVertices() {
        List<ExecutionVertex> sourceVertices = new ArrayList<>();
        for (ExecutionVertex vertex : vertices) {
            List<ExecutionVertex> inputVertices = inputVerticesMap.get(vertex.getVertexId());
            if (inputVertices == null || inputVertices.size() == 0) {
                sourceVertices.add(vertex);
            }
        }
        return sourceVertices;
    }

    private static List<List<ExecutionEdge>> splitUnrelatedEdges(List<ExecutionEdge> edges) {

        List<List<ExecutionEdge>> edgeList = new ArrayList<>();
        while (!edges.isEmpty()) {
            edgeList.add(findVertexRelatedEdge(edges, edges.get(0).getLeftVertex()));
        }
        return edgeList;
    }

    private static List<ExecutionEdge> findVertexRelatedEdge(
            List<ExecutionEdge> edges, ExecutionVertex vertex) {

        List<ExecutionEdge> sourceEdges =
                edges.stream()
                        .filter(edge -> edge.getLeftVertex().equals(vertex))
                        .collect(Collectors.toList());
        List<ExecutionEdge> destinationEdges =
                edges.stream()
                        .filter(edge -> edge.getRightVertex().equals(vertex))
                        .collect(Collectors.toList());

        List<ExecutionEdge> relatedEdges = new ArrayList<>(sourceEdges);
        relatedEdges.addAll(destinationEdges);

        List<ExecutionVertex> relatedActions =
                sourceEdges.stream()
                        .map(ExecutionEdge::getRightVertex)
                        .collect(Collectors.toList());
        relatedActions.addAll(
                destinationEdges.stream()
                        .map(ExecutionEdge::getLeftVertex)
                        .collect(Collectors.toList()));

        edges.removeAll(relatedEdges);

        relatedEdges.addAll(
                relatedActions.stream()
                        .flatMap(d -> findVertexRelatedEdge(edges, d).stream())
                        .collect(Collectors.toList()));

        return relatedEdges;
    }
}
