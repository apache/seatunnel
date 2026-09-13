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

package org.apache.seatunnel.benchmark.dag;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.VertexInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Creates deterministic production DAG values without submitting fixture jobs. */
public final class JobDagFixtureFactory {

    private JobDagFixtureFactory() {}

    /**
     * Builds a production {@link JobDAGInfo} with deterministic source-to-sink pipelines.
     *
     * <p>Each pipeline contains one source vertex, one sink vertex, and one connecting edge. The
     * resulting value is intended for storage benchmarks; this method does not submit or execute a
     * SeaTunnel job.
     *
     * @param pipelineCount exact number of pipelines to include
     * @return a serializable DAG value accepted by the production finished-job IMap
     * @throws IllegalArgumentException if {@code pipelineCount} is not positive
     */
    public static JobDAGInfo create(int pipelineCount) {
        if (pipelineCount <= 0) {
            throw new IllegalArgumentException("pipelineCount must be greater than zero");
        }

        Map<Integer, List<Edge>> pipelineEdges = new HashMap<>();
        Map<Long, VertexInfo> vertices = new HashMap<>();
        List<TablePath> tablePaths = Collections.singletonList(TablePath.DEFAULT);

        for (int pipelineId = 1; pipelineId <= pipelineCount; pipelineId++) {
            long sourceVertexId = pipelineId * 2L - 1L;
            long sinkVertexId = sourceVertexId + 1L;
            List<Edge> edges = new ArrayList<>(1);
            edges.add(new Edge(sourceVertexId, sinkVertexId));
            pipelineEdges.put(pipelineId, edges);
            vertices.put(
                    sourceVertexId,
                    new VertexInfo(
                            sourceVertexId,
                            PluginType.SOURCE,
                            "BenchmarkSource-" + pipelineId,
                            tablePaths));
            vertices.put(
                    sinkVertexId,
                    new VertexInfo(
                            sinkVertexId,
                            PluginType.SINK,
                            "BenchmarkSink-" + pipelineId,
                            tablePaths));
        }

        Map<String, Object> envOptions = new HashMap<>();
        envOptions.put("job.mode", "STREAMING");
        envOptions.put("parallelism", 4);
        ExecutionAddress member = new ExecutionAddress("127.0.0.1", 5801);
        Set<ExecutionAddress> executionPlan = new HashSet<>();
        executionPlan.add(member);
        return new JobDAGInfo(1L, envOptions, pipelineEdges, vertices, member, executionPlan);
    }
}
