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
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.ActionUtils;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.VertexInfo;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionEdge;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionPlanGenerator;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class DAGUtils {

    public static LogicalDag restoreLogicalDag(
            JobImmutableInformation jobImmutableInformation,
            SerializationService serializationService,
            List<ClassLoader> classLoaders) {
        LogicalDag logicalDag =
                serializationService.toObject(jobImmutableInformation.getLogicalDag());
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            List<Data> logicalVertexDataList = jobImmutableInformation.getLogicalVertexDataList();
            for (int i = 0; i < jobImmutableInformation.getLogicalVertexDataList().size(); i++) {
                Thread.currentThread().setContextClassLoader(classLoaders.get(i));
                logicalDag.addLogicalVertex(
                        serializationService.toObject(logicalVertexDataList.get(i)));
            }
            return logicalDag;
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    public static LogicalDag restoreLogicalDag(
            JobImmutableInformation jobImmutableInformation,
            SerializationService serializationService,
            ClassLoaderService classLoaderService) {
        List<Set<URL>> logicalVertexJarsList = jobImmutableInformation.getLogicalVertexJarsList();
        List<ClassLoader> classLoaders = new ArrayList<>();
        try {
            for (Set<URL> urls : logicalVertexJarsList) {
                classLoaders.add(
                        classLoaderService.getClassLoader(
                                jobImmutableInformation.getJobId(), urls));
            }
            return restoreLogicalDag(jobImmutableInformation, serializationService, classLoaders);
        } finally {
            for (Set<URL> urls : logicalVertexJarsList) {
                classLoaderService.releaseClassLoader(jobImmutableInformation.getJobId(), urls);
            }
        }
    }

    public static JobDAGInfo getJobDAGInfo(
            LogicalDag logicalDag,
            JobImmutableInformation jobImmutableInformation,
            EngineConfig engineConfig,
            boolean isPhysicalDAGInfo,
            ExecutionAddress master,
            Set<ExecutionAddress> historyExecutionAddress) {
        List<Pipeline> pipelines =
                new ExecutionPlanGenerator(logicalDag, jobImmutableInformation, engineConfig)
                        .generate()
                        .getPipelines();
        if (isPhysicalDAGInfo) {
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
                    vertexInfoMap,
                    master,
                    historyExecutionAddress);
        } else {
            // Generate LogicalPlan DAG
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
                    logicalEdgesByPipeline(logicalDag, pipelines);
            return new JobDAGInfo(
                    jobImmutableInformation.getJobId(),
                    logicalDag.getJobConfig().getEnvOptions(),
                    pipelineWithEdges,
                    vertexInfoMap,
                    master,
                    historyExecutionAddress);
        }
    }

    /**
     * Assigns each logical edge to the pipelines that run it.
     *
     * <p>The execution plan copies actions but keeps the source, sink and transform instances of
     * the logical plan, so pipeline vertices are traced back to logical vertices by instance. A
     * chained transform vertex stands for several logical vertices, and a union split runs the
     * shared part of the graph in several pipelines, so an edge can belong to more than one
     * pipeline.
     */
    static Map<Integer, List<Edge>> logicalEdgesByPipeline(
            LogicalDag logicalDag, List<Pipeline> pipelines) {
        Map<Object, Long> logicalVertexByPlugin = new IdentityHashMap<>();
        for (LogicalVertex vertex : logicalDag.getLogicalVertexMap().values()) {
            Long previous =
                    logicalVertexByPlugin.put(plugin(vertex.getAction()), vertex.getVertexId());
            if (previous != null) {
                throw new IllegalStateException(
                        String.format(
                                "Logical vertices %s and %s share one plugin instance",
                                previous, vertex.getVertexId()));
            }
        }

        Set<Edge> logicalEdges = new HashSet<>();
        for (LogicalEdge edge : logicalDag.getEdges()) {
            logicalEdges.add(new Edge(edge.getInputVertexId(), edge.getTargetVertexId()));
        }

        Map<Integer, List<Edge>> edgesByPipeline = new LinkedHashMap<>();
        Set<Edge> assigned = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            Set<Edge> edges = new HashSet<>();
            for (ExecutionVertex vertex : pipeline.getVertexes().values()) {
                List<Long> chained = logicalVertexIds(vertex.getAction(), logicalVertexByPlugin);
                for (int i = 1; i < chained.size(); i++) {
                    edges.add(new Edge(chained.get(i - 1), chained.get(i)));
                }
            }
            for (ExecutionEdge edge : pipeline.getEdges()) {
                List<Long> left =
                        logicalVertexIds(edge.getLeftVertex().getAction(), logicalVertexByPlugin);
                List<Long> right =
                        logicalVertexIds(edge.getRightVertex().getAction(), logicalVertexByPlugin);
                edges.add(new Edge(left.get(left.size() - 1), right.get(0)));
            }
            for (Edge edge : edges) {
                if (!logicalEdges.contains(edge)) {
                    throw new IllegalStateException(
                            String.format(
                                    "Pipeline %s has edge %s -> %s that is not in the logical plan",
                                    pipeline.getId(),
                                    edge.getInputVertexId(),
                                    edge.getTargetVertexId()));
                }
            }
            List<Edge> sorted = new ArrayList<>(edges);
            sorted.sort(
                    Comparator.comparing(Edge::getInputVertexId)
                            .thenComparing(Edge::getTargetVertexId));
            edgesByPipeline.put(pipeline.getId(), sorted);
            assigned.addAll(edges);
        }

        for (Edge edge : logicalEdges) {
            if (!assigned.contains(edge)) {
                throw new IllegalStateException(
                        String.format(
                                "Logical edge %s -> %s is not part of any pipeline",
                                edge.getInputVertexId(), edge.getTargetVertexId()));
            }
        }
        return edgesByPipeline;
    }

    /** Logical vertex ids an execution action stands for, in chain order. */
    private static List<Long> logicalVertexIds(
            Action action, Map<Object, Long> logicalVertexByPlugin) {
        List<Object> plugins = new ArrayList<>();
        if (action instanceof TransformChainAction) {
            plugins.addAll(((TransformChainAction<?>) action).getTransforms());
        } else {
            plugins.add(plugin(action));
        }
        List<Long> ids = new ArrayList<>(plugins.size());
        for (Object plugin : plugins) {
            Long id = logicalVertexByPlugin.get(plugin);
            if (id == null) {
                throw new IllegalStateException(
                        String.format(
                                "Action %s is not traceable to the logical plan",
                                action.getName()));
            }
            ids.add(id);
        }
        return ids;
    }

    private static Object plugin(Action action) {
        if (action instanceof SourceAction) {
            return ((SourceAction<?, ?, ?>) action).getSource();
        }
        if (action instanceof SinkAction) {
            return ((SinkAction<?, ?, ?, ?>) action).getSink();
        }
        if (action instanceof TransformAction) {
            return ((TransformAction) action).getTransform();
        }
        throw new UnknownActionException(action);
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
