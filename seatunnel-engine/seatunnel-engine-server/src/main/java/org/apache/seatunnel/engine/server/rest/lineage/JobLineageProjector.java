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

package org.apache.seatunnel.engine.server.rest.lineage;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonEncoding;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.core.job.Edge;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.VertexInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Projects a {@link JobDAGInfo} snapshot into the bounded job lineage JSON document.
 *
 * <p>Only whitelisted topology fields are read; environment options, addresses and other {@link
 * JobDAGInfo} content never reach the output. The input is not modified. The output is
 * deterministic for a given snapshot: nodes, edges, warnings and table paths are sorted, and
 * identical edges within a pipeline are written once.
 */
public final class JobLineageProjector {

    public static final int SCHEMA_VERSION = 1;

    static final String DATASET_METADATA_UNAVAILABLE = "DATASET_METADATA_UNAVAILABLE";

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private static final Comparator<LineageEdge> EDGE_ORDER =
            Comparator.comparingInt((LineageEdge e) -> e.pipelineId)
                    .thenComparingLong(e -> e.sourceNodeId)
                    .thenComparingLong(e -> e.targetNodeId);

    private JobLineageProjector() {}

    /**
     * Validates and serializes the snapshot as UTF-8 JSON.
     *
     * @throws JobLineageException if the snapshot is not a valid graph or exceeds {@code limits}
     */
    public static byte[] project(JobDAGInfo dagInfo, JobLineageLimits limits) {
        if (dagInfo == null || dagInfo.getJobId() == null || dagInfo.getJobId() <= 0) {
            throw unavailable();
        }
        Map<Long, VertexInfo> vertices = dagInfo.getVertexInfoMap();
        Map<Integer, List<Edge>> pipelineEdges = dagInfo.getPipelineEdges();
        if (vertices == null || vertices.isEmpty() || pipelineEdges == null) {
            throw unavailable();
        }
        checkCounts(vertices, pipelineEdges, limits);

        SortedMap<Long, LineageNode> nodes = projectNodes(vertices, limits);
        SortedSet<LineageEdge> edges = projectEdges(pipelineEdges, nodes);
        checkAcyclic(nodes, edges);
        return serialize(dagInfo.getJobId(), nodes, edges, limits);
    }

    private static void checkCounts(
            Map<Long, VertexInfo> vertices,
            Map<Integer, List<Edge>> pipelineEdges,
            JobLineageLimits limits) {
        if (vertices.size() > limits.getMaxNodes()) {
            throw tooLarge();
        }
        long edgeCount = 0;
        for (List<Edge> edges : pipelineEdges.values()) {
            if (edges == null) {
                throw unavailable();
            }
            edgeCount += edges.size();
        }
        if (edgeCount > limits.getMaxEdges()) {
            throw tooLarge();
        }
        long tablePathCount = 0;
        for (VertexInfo vertex : vertices.values()) {
            if (vertex != null && vertex.getTablePaths() != null) {
                tablePathCount += vertex.getTablePaths().size();
            }
        }
        if (tablePathCount > limits.getMaxTablePaths()) {
            throw tooLarge();
        }
    }

    private static SortedMap<Long, LineageNode> projectNodes(
            Map<Long, VertexInfo> vertices, JobLineageLimits limits) {
        SortedMap<Long, LineageNode> nodes = new TreeMap<>();
        // Every kept name and path is written at least once, so their total also bounds the
        // response; checking it here rejects oversized graphs before all strings are retained.
        long retainedBytes = 0;
        for (Map.Entry<Long, VertexInfo> entry : vertices.entrySet()) {
            VertexInfo vertex = entry.getValue();
            if (entry.getKey() == null
                    || vertex == null
                    || entry.getKey() != vertex.getVertexId()
                    || vertex.getType() == null) {
                throw unavailable();
            }
            String name = vertex.getConnectorType() == null ? "" : vertex.getConnectorType();
            retainedBytes += checkStringBytes(name, limits);

            SortedSet<String> tablePaths = new TreeSet<>();
            boolean pathsOmitted = false;
            if (vertex.getType() != PluginType.TRANSFORM && vertex.getTablePaths() != null) {
                for (TablePath tablePath : vertex.getTablePaths()) {
                    if (tablePath == null || TablePath.DEFAULT.equals(tablePath)) {
                        pathsOmitted = true;
                        continue;
                    }
                    String path = tablePath.toString();
                    int pathBytes = checkStringBytes(path, limits);
                    if (tablePaths.add(path)) {
                        retainedBytes += pathBytes;
                    }
                }
            }
            if (retainedBytes > limits.getMaxResponseBytes()) {
                throw tooLarge();
            }
            nodes.put(
                    entry.getKey(),
                    new LineageNode(
                            vertex.getVertexId(),
                            vertex.getType(),
                            name,
                            tablePaths,
                            pathsOmitted || tablePaths.isEmpty()));
        }
        return nodes;
    }

    private static SortedSet<LineageEdge> projectEdges(
            Map<Integer, List<Edge>> pipelineEdges, Map<Long, LineageNode> nodes) {
        SortedSet<LineageEdge> edges = new TreeSet<>(EDGE_ORDER);
        for (Map.Entry<Integer, List<Edge>> entry : pipelineEdges.entrySet()) {
            if (entry.getKey() == null) {
                throw unavailable();
            }
            for (Edge edge : entry.getValue()) {
                if (edge == null
                        || edge.getInputVertexId() == null
                        || edge.getTargetVertexId() == null
                        || !nodes.containsKey(edge.getInputVertexId())
                        || !nodes.containsKey(edge.getTargetVertexId())
                        || edge.getInputVertexId().equals(edge.getTargetVertexId())) {
                    throw unavailable();
                }
                edges.add(
                        new LineageEdge(
                                entry.getKey(), edge.getInputVertexId(), edge.getTargetVertexId()));
            }
        }
        return edges;
    }

    /** Kahn's algorithm over the union of all pipelines; iterative to keep stack use bounded. */
    private static void checkAcyclic(Map<Long, LineageNode> nodes, Set<LineageEdge> edges) {
        Map<Long, Set<Long>> successors = new HashMap<>();
        Map<Long, Integer> inDegree = new HashMap<>();
        for (LineageEdge edge : edges) {
            if (successors
                    .computeIfAbsent(edge.sourceNodeId, k -> new HashSet<>())
                    .add(edge.targetNodeId)) {
                inDegree.merge(edge.targetNodeId, 1, Integer::sum);
            }
        }
        Deque<Long> ready = new ArrayDeque<>();
        for (Long nodeId : nodes.keySet()) {
            if (!inDegree.containsKey(nodeId)) {
                ready.add(nodeId);
            }
        }
        int visited = 0;
        while (!ready.isEmpty()) {
            Long nodeId = ready.poll();
            visited++;
            for (Long next : successors.getOrDefault(nodeId, Collections.emptySet())) {
                if (inDegree.merge(next, -1, Integer::sum) == 0) {
                    ready.add(next);
                }
            }
        }
        if (visited != nodes.size()) {
            throw unavailable();
        }
    }

    private static byte[] serialize(
            long jobId,
            Map<Long, LineageNode> nodes,
            Set<LineageEdge> edges,
            JobLineageLimits limits) {
        BoundedOutputStream out = new BoundedOutputStream(limits.getMaxResponseBytes());
        try (JsonGenerator gen = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
            gen.writeStartObject();
            gen.writeNumberField("schemaVersion", SCHEMA_VERSION);
            gen.writeStringField("jobId", Long.toString(jobId));
            gen.writeStringField("graphKind", "EXECUTION");
            gen.writeStringField("idScope", "JOB");

            gen.writeArrayFieldStart("nodes");
            for (LineageNode node : nodes.values()) {
                gen.writeStartObject();
                gen.writeStringField("id", Long.toString(node.id));
                gen.writeStringField("kind", node.kind.name());
                gen.writeStringField("name", node.name);
                gen.writeArrayFieldStart("tablePaths");
                for (String path : node.tablePaths) {
                    gen.writeString(path);
                }
                gen.writeEndArray();
                gen.writeStringField("datasetMetadata", node.datasetMetadata());
                gen.writeEndObject();
            }
            gen.writeEndArray();

            gen.writeArrayFieldStart("edges");
            for (LineageEdge edge : edges) {
                gen.writeStartObject();
                gen.writeNumberField("pipelineId", edge.pipelineId);
                gen.writeStringField("sourceNodeId", Long.toString(edge.sourceNodeId));
                gen.writeStringField("targetNodeId", Long.toString(edge.targetNodeId));
                gen.writeEndObject();
            }
            gen.writeEndArray();

            gen.writeArrayFieldStart("warnings");
            for (LineageNode node : nodes.values()) {
                if (node.hasMetadataWarning()) {
                    gen.writeStartObject();
                    gen.writeStringField("code", DATASET_METADATA_UNAVAILABLE);
                    gen.writeStringField("nodeId", Long.toString(node.id));
                    gen.writeEndObject();
                }
            }
            gen.writeEndArray();
            gen.writeEndObject();
        } catch (ResponseLimitExceededException e) {
            throw tooLarge();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    /** Returns the UTF-8 length of {@code value}, rejecting values over the string limit. */
    private static int checkStringBytes(String value, JobLineageLimits limits) {
        if (value.length() > limits.getMaxStringBytes()) {
            throw tooLarge();
        }
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > limits.getMaxStringBytes()) {
            throw tooLarge();
        }
        return bytes;
    }

    private static JobLineageException unavailable() {
        return new JobLineageException(JobLineageException.Reason.LINEAGE_UNAVAILABLE);
    }

    private static JobLineageException tooLarge() {
        return new JobLineageException(JobLineageException.Reason.LINEAGE_GRAPH_TOO_LARGE);
    }

    private static final class LineageNode {
        private final long id;
        private final PluginType kind;
        private final String name;
        private final SortedSet<String> tablePaths;
        private final boolean metadataIncomplete;

        private LineageNode(
                long id,
                PluginType kind,
                String name,
                SortedSet<String> tablePaths,
                boolean metadataIncomplete) {
            this.id = id;
            this.kind = kind;
            this.name = name;
            this.tablePaths = tablePaths;
            this.metadataIncomplete = metadataIncomplete;
        }

        private String datasetMetadata() {
            if (kind == PluginType.TRANSFORM) {
                return "NOT_APPLICABLE";
            }
            return tablePaths.isEmpty() ? "UNAVAILABLE" : "REPORTED";
        }

        private boolean hasMetadataWarning() {
            return kind != PluginType.TRANSFORM && metadataIncomplete;
        }
    }

    private static final class LineageEdge {
        private final int pipelineId;
        private final long sourceNodeId;
        private final long targetNodeId;

        private LineageEdge(int pipelineId, long sourceNodeId, long targetNodeId) {
            this.pipelineId = pipelineId;
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
        }
    }

    private static final class ResponseLimitExceededException extends IOException {
        private static final long serialVersionUID = 1L;
    }

    /** Fails as soon as the serialized document would exceed the response byte limit. */
    private static final class BoundedOutputStream extends OutputStream {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final int maxBytes;

        private BoundedOutputStream(int maxBytes) {
            this.maxBytes = maxBytes;
        }

        @Override
        public void write(int b) throws IOException {
            ensureCapacity(1);
            buffer.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            ensureCapacity(len);
            buffer.write(b, off, len);
        }

        private void ensureCapacity(int len) throws ResponseLimitExceededException {
            if ((long) buffer.size() + len > maxBytes) {
                throw new ResponseLimitExceededException();
            }
        }

        private byte[] toByteArray() {
            return buffer.toByteArray();
        }
    }
}
