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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipelineGenerator {

    public static List<Pipeline> generatePipelines(List<ExecutionEdge> edges) {

        // Split into multiple unrelated pipelines
        List<List<ExecutionEdge>> edgesList = splitUnrelatedEdges(expandEdgeByParallelism(edges));

        // just convert execution plan to pipeline at now. We should split it to multi pipeline with
        // cache in the future
        IdGenerator idGenerator = new IdGenerator();
        return edgesList.stream().map(e -> {
            Map<Long, ExecutionVertex> vertexes = new HashMap<>();
            List<ExecutionEdge> pipelineEdges = e.stream().map(edge -> {
                if (!vertexes.containsKey(edge.getLeftVertexId())) {
                    vertexes.put(edge.getLeftVertexId(), edge.getLeftVertex());
                }
                ExecutionVertex source = vertexes.get(edge.getLeftVertexId());
                if (!vertexes.containsKey(edge.getRightVertexId())) {
                    vertexes.put(edge.getRightVertexId(), edge.getRightVertex());
                }
                ExecutionVertex destination = vertexes.get(edge.getRightVertexId());
                return new ExecutionEdge(source, destination);
            }).collect(Collectors.toList());
            return new Pipeline((int) idGenerator.getNextId(), pipelineEdges, vertexes);
        }).collect(Collectors.toList());
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

    private static List<List<ExecutionEdge>> splitUnrelatedEdges(List<ExecutionEdge> edges) {

        List<List<ExecutionEdge>> edgeList = new ArrayList<>();
        while (!edges.isEmpty()) {
            edgeList.add(findVertexRelatedEdge(edges, edges.get(0).getLeftVertex()));
        }
        return edgeList;
    }

    private static List<ExecutionEdge> findVertexRelatedEdge(List<ExecutionEdge> edges, ExecutionVertex vertex) {

        List<ExecutionEdge> sourceEdges = edges.stream().filter(edge -> edge.getLeftVertex().equals(vertex))
                .collect(Collectors.toList());
        List<ExecutionEdge> destinationEdges = edges.stream().filter(edge -> edge.getRightVertex().equals(vertex))
                .collect(Collectors.toList());

        List<ExecutionEdge> relatedEdges = new ArrayList<>(sourceEdges);
        relatedEdges.addAll(destinationEdges);

        List<ExecutionVertex> relatedActions =
                sourceEdges.stream().map(ExecutionEdge::getRightVertex).collect(Collectors.toList());
        relatedActions.addAll(destinationEdges.stream().map(ExecutionEdge::getLeftVertex).collect(Collectors.toList()));

        edges.removeAll(relatedEdges);

        relatedEdges.addAll(relatedActions.stream()
                .flatMap(d -> findVertexRelatedEdge(edges, d).stream()).collect(Collectors.toList()));

        return relatedEdges;

    }
}
