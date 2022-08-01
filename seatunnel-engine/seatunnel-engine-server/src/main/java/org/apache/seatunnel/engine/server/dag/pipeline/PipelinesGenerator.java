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

package org.apache.seatunnel.engine.server.dag.pipeline;

import org.apache.seatunnel.engine.core.dag.Edge;
import org.apache.seatunnel.engine.core.dag.Vertex;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipelinesGenerator {

    private List<Edge> edges;

    public PipelinesGenerator(ExecutionPlan executionPlan) {
        edges = expandEdgeByParallelism(new ArrayList<>(executionPlan.getEdges()));
    }

    public Pipelines generate() {

        // Split into multiple unrelated pipelines
        List<List<Edge>> edgesList = splitUnrelatedEdges(edges);

        // just convert execution plan to pipeline at now. We should split it to multi pipeline with
        // cache in the future

        return new Pipelines(edgesList.stream().map(e -> {
            Map<Integer, Vertex> vertexes = new HashMap<>();
            List<Edge> pipelineEdges = e.stream().map(edge -> {
                if (!vertexes.containsKey(edge.getLeftVertexId())) {
                    vertexes.put(edge.getLeftVertexId(), edge.getLeftVertex());
                }
                Vertex source = vertexes.get(edge.getLeftVertexId());
                if (!vertexes.containsKey(edge.getRightVertexId())) {
                    vertexes.put(edge.getRightVertexId(), edge.getRightVertex());
                }
                Vertex destination = vertexes.get(edge.getRightVertexId());
                return new Edge(source, destination);
            }).collect(Collectors.toList());
            return new Pipelines.Pipeline(pipelineEdges, vertexes);
        }).collect(Collectors.toList()));
    }

    private List<Edge> expandEdgeByParallelism(List<Edge> edges) {
        /*
         *TODO
         * use SupportCoordinate interface to determine whether the Pipeline needs to be split.
         * Pipelines without coordinator support can be split into multiple pipelines that do not
         * interfere with each other
         */
        return edges;
    }

    private List<List<Edge>> splitUnrelatedEdges(List<Edge> edges) {

        List<List<Edge>> edgeList = new ArrayList<>();
        while (!edges.isEmpty()) {
            edgeList.add(findVertexRelatedEdge(edges, edges.get(0).getLeftVertex()));
        }
        return edgeList;
    }

    private List<Edge> findVertexRelatedEdge(List<Edge> edges, Vertex vertex) {

        List<Edge> sourceEdges = edges.stream().filter(edge -> edge.getLeftVertex().equals(vertex))
                .collect(Collectors.toList());
        List<Edge> destinationEdges = edges.stream().filter(edge -> edge.getRightVertex().equals(vertex))
                .collect(Collectors.toList());

        List<Edge> relatedEdges = new ArrayList<>(sourceEdges);
        relatedEdges.addAll(destinationEdges);

        List<Vertex> relatedActions = sourceEdges.stream().map(Edge::getRightVertex).collect(Collectors.toList());
        relatedActions.addAll(destinationEdges.stream().map(Edge::getLeftVertex).collect(Collectors.toList()));

        edges.removeAll(relatedEdges);

        relatedEdges.addAll(relatedActions.stream()
                .flatMap(d -> findVertexRelatedEdge(edges, d).stream()).collect(Collectors.toList()));

        return relatedEdges;

    }
}
