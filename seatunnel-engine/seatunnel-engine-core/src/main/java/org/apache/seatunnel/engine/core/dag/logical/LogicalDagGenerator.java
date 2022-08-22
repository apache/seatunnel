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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalDagGenerator {
    private static final ILogger LOGGER = Logger.getLogger(LogicalDagGenerator.class);
    private List<Action> actions;
    private JobConfig jobConfig;
    private IdGenerator idGenerator;

    private final Map<Long, LogicalVertex> logicalVertexMap = new HashMap<>();

    /**
     * key: input vertex id;
     * <br> value: target vertices id;
     */
    private final Map<Long, Set<Long>> inputVerticesMap = new HashMap<>();

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
        Set<LogicalEdge> logicalEdges = createLogicalEdges();
        LogicalDag logicalDag = new LogicalDag(jobConfig, idGenerator);
        logicalDag.getEdges().addAll(logicalEdges);
        logicalDag.getLogicalVertexMap().putAll(logicalVertexMap);
        return logicalDag;
    }

    private void createLogicalVertex(Action action) {
        final Long logicalVertexId = action.getId();
        if (logicalVertexMap.containsKey(logicalVertexId)) {
            return;
        }
        // connection vertices info
        action.getUpstream().forEach(inputAction -> {
            createLogicalVertex(inputAction);
            inputVerticesMap.computeIfAbsent(inputAction.getId(), id -> new HashSet<>())
                .add(logicalVertexId);
        });

        final LogicalVertex logicalVertex = new LogicalVertex(logicalVertexId, action, action.getParallelism());
        logicalVertexMap.put(logicalVertexId, logicalVertex);
    }

    private Set<LogicalEdge> createLogicalEdges() {
        return inputVerticesMap.entrySet()
                .stream()
                .map(entry -> entry.getValue()
                        .stream()
                        .map(targetId -> new LogicalEdge(logicalVertexMap.get(entry.getKey()),
                                logicalVertexMap.get(targetId)))
                        .collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }
}
