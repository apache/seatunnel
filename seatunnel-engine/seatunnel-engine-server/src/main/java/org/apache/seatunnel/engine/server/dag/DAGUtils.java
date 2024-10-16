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
import org.apache.seatunnel.engine.core.dag.actions.Action;
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

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
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
                                            vertexInfoMap.put(
                                                    id,
                                                    new VertexInfo(
                                                            vertex.getVertexId(),
                                                            ActionUtils.getActionType(
                                                                    vertex.getAction()),
                                                            vertex.getAction().getName(),
                                                            getTablePaths(vertex.getAction())));
                                        });
                    });
            return new JobDAGInfo(
                    jobImmutableInformation.getJobId(),
                    logicalDag.getJobConfig().getEnvOptions(),
                    pipelineWithEdges,
                    vertexInfoMap);
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
                                    v ->
                                            new VertexInfo(
                                                    v.getVertexId(),
                                                    ActionUtils.getActionType(v.getAction()),
                                                    v.getAction().getName(),
                                                    getTablePaths(v.getAction())))
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
                    jobImmutableInformation.getJobId(),
                    logicalDag.getJobConfig().getEnvOptions(),
                    pipelineWithEdges,
                    vertexInfoMap);
        }
    }

    private static List<TablePath> getTablePaths(Action action) {

        List<TablePath> tablePaths = new ArrayList<>();
        if (action instanceof SourceAction) {
            SourceAction sourceAction = (SourceAction) action;

            try {

                List<CatalogTable> producedCatalogTables =
                        sourceAction.getSource().getProducedCatalogTables();
                List<TablePath> sourceTablePaths =
                        producedCatalogTables.stream()
                                .map(CatalogTable::getTablePath)
                                .collect(Collectors.toList());
                tablePaths.addAll(sourceTablePaths);
            } catch (UnsupportedOperationException e) {
                // ignore
                log.warn(
                        "SourceAction {} does not support getProducedCatalogTables, fallback to default table path",
                        action.getName());
                tablePaths.add(TablePath.DEFAULT);
            }
        } else if (action instanceof SinkAction) {
            SeaTunnelSink seaTunnelSink = ((SinkAction<?, ?, ?, ?>) action).getSink();
            if (seaTunnelSink instanceof MultiTableSink) {
                List<TablePath> sinkTablePaths =
                        new ArrayList<>(((MultiTableSink) seaTunnelSink).getSinkTables());
                tablePaths.addAll(sinkTablePaths);
            } else {
                Optional<CatalogTable> catalogTable = seaTunnelSink.getWriteCatalogTable();
                catalogTable.ifPresent(table -> tablePaths.add(table.getTablePath()));
            }
        }

        return tablePaths;
    }
}
