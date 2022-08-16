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

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogicalDagGenerator {
    private static final ILogger LOGGER = Logger.getLogger(LogicalDagGenerator.class);
    private List<Action> actions;
    private JobConfig jobConfig;
    private IdGenerator idGenerator;

    private final Map<Long, LogicalVertex> logicalVertexMap = new HashMap<>();
    private final Set<LogicalEdge> logicalEdges = new LinkedHashSet<>();

    private final Map<Long, Set<Long>> inputVerticesMap = new HashMap<>();

    private final Map<Long, Set<Long>> outputVerticesMap = new HashMap<>();

    public LogicalDagGenerator(@NonNull List<Action> actions,
                               @NonNull JobConfig jobConfig,
                               @NonNull IdGenerator idGenerator) {
        this.actions = actions;
        this.jobConfig = jobConfig;
        this.idGenerator = idGenerator;
        if (actions.size() <= 0) {
            throw new IllegalStateException("No actions define in the job. Cannot execute.");
        }
    }

    public LogicalDag generate() {
        actions.forEach(this::createLogicalVertex);
        connectVertices();
        return new LogicalDagOptimizer(logicalVertexMap, logicalEdges, jobConfig)
            .expandAndOptimize();
    }

    private void createLogicalVertex(Action action) {
        final Long logicalVertexId = action.getId();
        // connection vertices info
        final Set<Long> inputs = inputVerticesMap.computeIfAbsent(logicalVertexId, id -> new HashSet<>());
        action.getUpstream().forEach(inputAction -> {
            createLogicalVertex(inputAction);
            inputs.add(inputAction.getId());
            outputVerticesMap.computeIfAbsent(inputAction.getId(), id -> new HashSet<>())
                .add(logicalVertexId);
        });

        final LogicalVertex logicalVertex = new LogicalVertex(logicalVertexId, action, action.getParallelism());
        logicalVertexMap.put(logicalVertexId, logicalVertex);
    }

    private void connectVertices() {
        logicalVertexMap.forEach((id, logicalVertex) -> {
            inputVerticesMap.getOrDefault(id, new HashSet<>())
                .forEach(inputVertexId -> {
                    LogicalEdge edge = new LogicalEdge(logicalVertexMap.get(inputVertexId), logicalVertex);
                    logicalVertex.addInput(logicalVertexMap.get(inputVertexId));
                    logicalEdges.add(edge);
                });

            outputVerticesMap.getOrDefault(id, new HashSet<>())
                .forEach(outputVertexId -> {
                    LogicalEdge edge = new LogicalEdge(logicalVertex, logicalVertexMap.get(outputVertexId));
                    logicalVertex.addOutput(logicalVertexMap.get(outputVertexId));
                    logicalEdges.add(edge);
                });
        });
    }
}
