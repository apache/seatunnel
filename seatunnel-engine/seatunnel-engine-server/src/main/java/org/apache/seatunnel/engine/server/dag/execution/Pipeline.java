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

import org.apache.seatunnel.engine.core.dag.actions.Action;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Pipeline {

    /** The ID of the pipeline. */
    private final Integer id;

    private final List<ExecutionEdge> edges;

    private final Map<Long, ExecutionVertex> vertexes;

    Pipeline(Integer id, List<ExecutionEdge> edges, Map<Long, ExecutionVertex> vertexes) {
        this.id = id;
        this.edges = edges;
        this.vertexes = vertexes;
    }

    public Integer getId() {
        return id;
    }

    public List<ExecutionEdge> getEdges() {
        return edges;
    }

    public Map<Long, ExecutionVertex> getVertexes() {
        return vertexes;
    }

    public Map<Long, Integer> getActions() {
        return vertexes.values()
            .stream().map(ExecutionVertex::getAction)
            .collect(Collectors.toMap(Action::getId, Action::getParallelism));
    }
}
