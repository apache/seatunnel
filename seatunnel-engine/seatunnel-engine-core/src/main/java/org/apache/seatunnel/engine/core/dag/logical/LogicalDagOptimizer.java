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

package org.apache.seatunnel.engine.core.dag.logical;

import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.PartitionTransformAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogicalDagOptimizer {

    /**
     * The action ID needs to be regenerated because of the source, sink, and chain.
     */
    private final IdGenerator idGenerator = new IdGenerator();

    private final Map<Long, LogicalVertex> newLogicalVertexMap;

    private final List<LogicalVertex> logicalJobVertices;

    private final Set<LogicalEdge> oldEdges;

    /**
     * key: old vertex id, value: new vertex id;
     */
    private final Map<Long, Long> oldToNewIdMap = new HashMap<>();

    private final LogicalDag dag;

    public LogicalDagOptimizer(final Map<Long, LogicalVertex> logicalVertexMap,
                               final Set<LogicalEdge> edges,
                               JobConfig jobConfig) {
        this.newLogicalVertexMap = new HashMap<>(logicalVertexMap.size());
        this.logicalJobVertices = new ArrayList<>(logicalVertexMap.values());
        this.oldEdges = edges;
        this.dag = new LogicalDag(jobConfig, idGenerator);
    }

    public LogicalDag expandAndOptimize() {
        getSourceVertices().forEach(sourceVertex -> {
            List<LogicalVertex> vertices = new ArrayList<>();
            vertices.add(sourceVertex);
            for (int index = 0; index < vertices.size(); index++) {
                LogicalVertex vertex = vertices.get(index);
                createChainedVertex(vertex);
                vertex.getOutputs().forEach(edge -> vertices.add(edge.getTargetVertex()));
            }
        });
        connectVertices();
        dag.getLogicalVertexMap().putAll(newLogicalVertexMap);
        return dag;
    }

    private void connectVertices() {
        oldEdges.forEach(oldEdge -> {
            Long oldInputVertexId = oldEdge.getInputVertex().getVertexId();
            Long oldTargetVertexId = oldEdge.getTargetVertex().getVertexId();
            LogicalVertex inputVertex = newLogicalVertexMap.get(oldToNewIdMap.get(oldInputVertexId));
            LogicalVertex targetVertex = newLogicalVertexMap.get(oldToNewIdMap.get(oldTargetVertexId));
            LogicalEdge edge = new LogicalEdge(inputVertex, targetVertex);
            inputVertex.addOutput(targetVertex);
            targetVertex.addInput(inputVertex);
            dag.addEdge(edge);
        });
    }

    private List<LogicalVertex> getSourceVertices() {
        List<LogicalVertex> vertices = new ArrayList<>();
        for (LogicalVertex vertex : logicalJobVertices) {
            if (vertex.getInputs().size() == 0) {
                vertices.add(vertex);
            }
        }
        return vertices;
    }

    private void createChainedVertex(LogicalVertex logicalVertex) {
        if (oldToNewIdMap.containsKey(logicalVertex.getVertexId())) {
            return;
        }
        final ArrayList<LogicalVertex> chainedVertices = new ArrayList<>();
        chainedVertices(logicalVertex, chainedVertices);
        final long newId = idGenerator.getNextId();
        Action newAction;
        if (chainedVertices.size() < 1) {
            Action action = logicalVertex.getAction();
            if (action instanceof PartitionTransformAction) {
                newAction = new PartitionTransformAction(newId,
                    action.getName(),
                    action.getUpstream(),
                    ((PartitionTransformAction) action).getPartitionTransformation(),
                    action.getJarUrls());
            } else if (action instanceof SinkAction) {
                newAction = new SinkAction<>(newId,
                    action.getName(),
                    action.getUpstream(),
                    ((SinkAction<?, ?, ?, ?>) action).getSink(),
                    action.getJarUrls());
            } else if (action instanceof SourceAction){
                newAction = new SourceAction<>(newId,
                    action.getName(),
                    ((SourceAction<?, ?, ?>) action).getSource(),
                    action.getJarUrls());
            } else {
                throw new UnknownActionException(action);
            }
        } else {
            List<SeaTunnelTransform> transforms = new ArrayList<>(chainedVertices.size());
            List<String> names = new ArrayList<>(chainedVertices.size());
            Set<URL> jars = new HashSet<>();
            chainedVertices.stream()
                .peek(vertex -> oldToNewIdMap.put(vertex.getVertexId(), newId))
                .map(LogicalVertex::getAction)
                .map(action -> (TransformAction) action)
                .forEach(action -> {
                    transforms.add(action.getTransform());
                    jars.addAll(action.getJarUrls());
                    names.add(action.getName());
                });
            newAction = new TransformChainAction(newId,
                String.join("->", names),
                new ArrayList<>(jars),
                transforms);
        }
        LogicalVertex transformVertex =
            new LogicalVertex(newId,
                newAction,
                logicalVertex.getParallelism());
        newLogicalVertexMap.put(newId, transformVertex);
        oldToNewIdMap.put(logicalVertex.getVertexId(), newId);
    }

    private void chainedVertices(LogicalVertex logicalVertex, List<LogicalVertex> chainedVertices) {
        Action action = logicalVertex.getAction();
        if (action instanceof TransformAction) {
            if (chainedVertices.size() == 0) {
                chainedVertices.add(logicalVertex);
            } else if (logicalVertex.getInputs().size() == 1) {
                oldEdges.remove(new LogicalEdge(chainedVertices.get(chainedVertices.size() - 1), logicalVertex));
                chainedVertices.add(logicalVertex);
            } else {
                return;
            }
        } else {
            return;
        }
        if (logicalVertex.getOutputs().size() == 1) {
            chainedVertices(logicalVertex.getOutputs().get(0).getTargetVertex(), chainedVertices);
        }
    }
}
