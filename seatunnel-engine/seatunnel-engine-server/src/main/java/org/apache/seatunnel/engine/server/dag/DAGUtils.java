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

package org.apache.seatunnel.engine.server.dag;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.core.dag.actions.ActionUtils;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.VertexInfo;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlanGenerator;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;

import java.util.function.Function;
import java.util.stream.Collectors;

public class DAGUtils {

    public static JobDAGInfo getJobDAGInfo(
            LogicalDag logicalDag,
            JobImmutableInformation jobImmutableInformation,
            EngineConfig engineConfig,
            boolean isPhysicalDAGIInfo) {
        List<Pipeline> pipelines =
                new ExecutionPlanGenerator(logicalDag, jobImmutableInformation, engineConfig)
                        .generate()
                        .getPipelines();
        if (isPhysicalDAGIInfo) {
            // Generate ExecutePlan DAG
            Map<Integer, List<Edge>> pipelineWithEdges = new HashMap<>();
            Map<Long, VertexInfo> vertexInfoMap = new HashMap<>();
            pipelines.forEach(
                    pipeline -> {
                        pipelineWithEdges.put(
                                pipeline.getId(),
                                pipeline.getEdges().stream()
                                        .map(
                                                e ->
                                                        new Edge(
                                                                e.getLeftVertexId(),
                                                                e.getRightVertexId()))
                                        .collect(Collectors.toList()));
                        pipeline.getVertexes()
                                .forEach(
                                        (id, vertex) -> {
                                            List<String> tablePaths = new ArrayList<>();
                                            if (vertex.getAction() instanceof SourceAction) {
                                                SourceAction sourceAction =
                                                        (SourceAction) vertex.getAction();
                                                List<CatalogTable> producedCatalogTables =
                                                        sourceAction
                                                                .getSource()
                                                                .getProducedCatalogTables();
                                                List<String> collect =
                                                        producedCatalogTables.stream()
                                                                .map(
                                                                        catalogTable ->
                                                                                catalogTable
                                                                                        .getTablePath()
                                                                                        .toString())
                                                                .collect(Collectors.toList());
                                                tablePaths.addAll(collect);
                                            } else if (vertex.getAction() instanceof SinkAction) {
                                                SeaTunnelSink seaTunnelSink =
                                                        ((SinkAction<?, ?, ?, ?>)
                                                                        vertex.getAction())
                                                                .getSink();
                                                if (seaTunnelSink instanceof MultiTableSink) {
                                                    List<String> collect =
                                                            ((MultiTableSink) seaTunnelSink)
                                                                    .getSinkTables().stream()
                                                                            .map(
                                                                                    TablePath
                                                                                            ::toString)
                                                                            .collect(
                                                                                    Collectors
                                                                                            .toList());
                                                    tablePaths.addAll(collect);
                                                } else {
                                                    Optional<CatalogTable> catalogTable =
                                                            seaTunnelSink.getWriteCatalogTable();
                                                    catalogTable.ifPresent(
                                                            table ->
                                                                    tablePaths.add(
                                                                            table.getTablePath()
                                                                                    .getFullName()));
                                                }
                                            }
                                            vertexInfoMap.put(
                                                    id,
                                                    new VertexInfo(
                                                            vertex.getVertexId(),
                                                            ActionUtils.getActionType(
                                                                    vertex.getAction()),
                                                            vertex.getAction().getName(),
                                                            tablePaths));
                                        });
                    });
            return new JobDAGInfo(
                    jobImmutableInformation.getJobId(), pipelineWithEdges, vertexInfoMap);
        } else {
            // Generate LogicalPlan DAG
            List<Edge> edges =
                    logicalDag.getEdges().stream()
                            .map(e -> new Edge(e.getInputVertexId(), e.getTargetVertexId()))
                            .collect(Collectors.toList());

            Map<Long, LogicalVertex> logicalVertexMap = logicalDag.getLogicalVertexMap();
            Map<Long, VertexInfo> vertexInfoMap =
                    logicalVertexMap.values().stream()
                            .map(
                                    v -> {
                                        List<String> tablePaths = new ArrayList<>();
                                        if (v.getAction() instanceof SourceAction) {
                                            SourceAction sourceAction =
                                                    (SourceAction) v.getAction();
                                            List<CatalogTable> producedCatalogTables =
                                                    sourceAction
                                                            .getSource()
                                                            .getProducedCatalogTables();
                                            List<String> collect =
                                                    producedCatalogTables.stream()
                                                            .map(
                                                                    catalogTable ->
                                                                            catalogTable
                                                                                    .getTablePath()
                                                                                    .toString())
                                                            .collect(Collectors.toList());
                                            tablePaths.addAll(collect);
                                        } else if (v.getAction() instanceof SinkAction) {
                                            SeaTunnelSink seaTunnelSink =
                                                    ((SinkAction<?, ?, ?, ?>) v.getAction())
                                                            .getSink();
                                            if (seaTunnelSink instanceof MultiTableSink) {
                                                List<String> collect =
                                                        ((MultiTableSink) seaTunnelSink)
                                                                .getSinkTables().stream()
                                                                        .map(TablePath::toString)
                                                                        .collect(
                                                                                Collectors
                                                                                        .toList());
                                                tablePaths.addAll(collect);
                                            } else {
                                                Optional<CatalogTable> catalogTable =
                                                        seaTunnelSink.getWriteCatalogTable();
                                                catalogTable.ifPresent(
                                                        table ->
                                                                tablePaths.add(
                                                                        table.getTablePath()
                                                                                .getFullName()));
                                            }
                                        }

                                        return new VertexInfo(
                                                v.getVertexId(),
                                                ActionUtils.getActionType(v.getAction()),
                                                v.getAction().getName(),
                                                tablePaths);
                                    })
                            .collect(
                                    Collectors.toMap(VertexInfo::getVertexId, Function.identity()));

            Map<Integer, List<Edge>> pipelineWithEdges =
                    edges.stream()
                            .collect(
                                    Collectors.groupingBy(
                                            e -> {
                                                LogicalVertex info =
                                                        logicalVertexMap.get(
                                                                e.getInputVertexId() != null
                                                                        ? e.getInputVertexId()
                                                                        : e.getTargetVertexId());
                                                return pipelines.stream()
                                                        .filter(
                                                                p ->
                                                                        p.getActions()
                                                                                .containsKey(
                                                                                        info.getAction()
                                                                                                .getId()))
                                                        .findFirst()
                                                        .get()
                                                        .getId();
                                            },
                                            Collectors.toList()));
            return new JobDAGInfo(
                    jobImmutableInformation.getJobId(), pipelineWithEdges, vertexInfoMap);
        }
    }
}
