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

import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExecutionPlanGenerator {
    private final List<LogicalEdge> logicalEdges;

    private final JobImmutableInformation jobImmutableInformation;

    private final long initializationTimestamp;

    public ExecutionPlanGenerator(@NonNull LogicalDag logicalPlan,
                                  @NonNull JobImmutableInformation jobImmutableInformation,
                                  long initializationTimestamp) {
        checkArgument(logicalPlan.getEdges().size() > 0, "ExecutionPlan Builder must have LogicalPlan.");

        this.logicalEdges = new ArrayList<>(logicalPlan.getEdges());
        this.jobImmutableInformation = jobImmutableInformation;
        this.initializationTimestamp = initializationTimestamp;
    }

    public ExecutionPlan generate() {
        List<ExecutionEdge> executionEdges = createExecutionEdges(logicalEdges);
        return new ExecutionPlan(PipelineGenerator.generatePipelines(executionEdges), jobImmutableInformation);
    }

    private static ExecutionVertex convertFromLogical(LogicalVertex vertex) {
        return new ExecutionVertex(vertex.getVertexId(), vertex.getAction(), vertex.getParallelism());
    }

    public static List<ExecutionEdge> createExecutionEdges(List<LogicalEdge> logicalEdges) {
        return logicalEdges.stream()
            .map(logicalEdge -> new ExecutionEdge(
                convertFromLogical(logicalEdge.getInputVertex()),
                convertFromLogical(logicalEdge.getTargetVertex())))
            .collect(Collectors.toList());
    }
}
