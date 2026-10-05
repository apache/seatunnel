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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.UpdateStrategy;
import org.apache.hugegraph.structure.graph.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Dual-bucket batch buffer that independently accumulates and flushes vertices and edges. Each
 * bucket triggers flush when reaching batch_size; both buckets are flushed by the engine timer,
 * prepareCommit, or close.
 *
 * <p>Vertex-before-edge ordering is enforced only when {@code check_vertex} is true — the server
 * then rejects edges whose endpoint vertices do not yet exist, so a filling edge bucket first
 * flushes any pending vertices, and {@link #flush()} writes vertices before edges. When {@code
 * check_vertex} is false (the default) the server already accepts orphan edges, so the buckets
 * flush independently for higher throughput (no forced, undersized vertex flushes).
 */
public class BatchBuffer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuffer.class);

    private final List<GraphElementEnvelope> vertexBuffer = new ArrayList<>();
    private final List<GraphElementEnvelope> edgeBuffer = new ArrayList<>();
    private final int batchSize;
    private volatile boolean closed = false;
    private final HugeGraphClient client;
    private final boolean batchFailureFallback;
    private final boolean checkVertex;
    // Fail the task once this many records have been skipped by the single-record fallback;
    // negative means unlimited. Guards against the previously unbounded silent skipping.
    private final int maxInsertErrors;
    // Optional directory for skipped-record failure samples; null = do not persist.
    private final String failureDataPath;
    private final int subtaskIndex;
    // Cumulative count of records skipped by the fallback across this writer's lifetime. Only
    // mutated inside synchronized flush paths, so a plain long is sufficient.
    private long insertFailureCount;
    // Lazily opened on the first persisted sample; disabled after an I/O error so a broken
    // failure-log path never turns into a second failure that masks the real one.
    private BufferedWriter failureWriter;
    private boolean failureWriterDisabled;

    /**
     * Backward-compatible constructor that retains the original 3-argument signature. Defaults
     * {@code batchFailureFallback} and {@code checkVertex} to {@code false}, matching the pre-2.x
     * behaviour where neither feature existed.
     *
     * @deprecated Use {@link #BatchBuffer(HugeGraphClient, int, long, boolean, boolean)} instead so
     *     callers explicitly opt into failure-fallback and vertex-checking semantics.
     */
    @Deprecated
    public BatchBuffer(HugeGraphClient client, int batchSize, long batchIntervalMs) {
        this(client, batchSize, batchIntervalMs, false, false);
    }

    public BatchBuffer(
            HugeGraphClient client,
            int batchSize,
            long batchIntervalMs,
            boolean batchFailureFallback,
            boolean checkVertex) {
        this(client, batchSize, batchIntervalMs, batchFailureFallback, checkVertex, -1, null, 0);
    }

    public BatchBuffer(
            HugeGraphClient client,
            int batchSize,
            long batchIntervalMs,
            boolean batchFailureFallback,
            boolean checkVertex,
            int maxInsertErrors,
            String failureDataPath,
            int subtaskIndex) {
        // batchIntervalMs remains in the public signature for source compatibility. Timer flush is
        // registered by HugeGraphSinkWriter with the engine instead of creating a connector thread.
        this.batchSize = batchSize;
        this.client = client;
        this.batchFailureFallback = batchFailureFallback;
        this.checkVertex = checkVertex;
        this.maxInsertErrors = maxInsertErrors;
        this.failureDataPath = failureDataPath;
        this.subtaskIndex = subtaskIndex;
        this.insertFailureCount = 0;
    }

    public synchronized void add(GraphElementEnvelope envelope) throws IOException {
        if (closed) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.BUFFER_ADD_FAILED,
                    "BatchBuffer is already closed.");
        }

        try {
            if (envelope.getElementType() == LabelType.VERTEX) {
                vertexBuffer.add(envelope);
                if (vertexBuffer.size() >= batchSize) {
                    doFlushVertices();
                }
            } else {
                edgeBuffer.add(envelope);
                if (edgeBuffer.size() >= batchSize) {
                    // Topology safety only matters when the server validates endpoints: with
                    // check_vertex=true, flush pending vertices before the edges so no edge is sent
                    // before its endpoints exist. With check_vertex=false the server already
                    // accepts
                    // orphan edges, so skip the forced (undersized) vertex flush and let the vertex
                    // bucket accumulate to a full batch — fewer, fuller vertex requests.
                    if (checkVertex && !vertexBuffer.isEmpty()) {
                        doFlushVertices();
                    }
                    doFlushEdges();
                }
            }
        } catch (Exception e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED, e);
        }
    }

    /**
     * Backward-compatible overload that wraps a plain {@link GraphElement} in a minimal envelope.
     *
     * @deprecated Use {@link #add(GraphElementEnvelope)} instead so the buffer receives complete
     *     mapping context (label name, element type) for failure diagnostics.
     */
    @Deprecated
    public synchronized void add(GraphElement element) throws IOException {
        LabelType type = element instanceof Vertex ? LabelType.VERTEX : LabelType.EDGE;
        add(new GraphElementEnvelope(null, type, element));
    }

    public synchronized void flush() throws IOException {
        if (closed && vertexBuffer.isEmpty() && edgeBuffer.isEmpty()) {
            return;
        }
        doFlushVertices();
        doFlushEdges();
    }

    private void doFlushVertices() {
        if (vertexBuffer.isEmpty()) {
            return;
        }
        List<GraphElementEnvelope> batch = new ArrayList<>(vertexBuffer);
        vertexBuffer.clear();
        // Route by each element's own mapping strategy: a group with no strategy is a plain insert,
        // a group with a strategy is an upsert. HugeGraph applies one strategy map per batch call,
        // so elements with different strategies must go in separate calls.
        for (Map.Entry<Map<String, UpdateStrategy>, List<GraphElementEnvelope>> group :
                groupByStrategy(batch).entrySet()) {
            flushVertexGroup(group.getValue(), group.getKey());
        }
    }

    private void flushVertexGroup(
            List<GraphElementEnvelope> batch, Map<String, UpdateStrategy> updateStrategies) {
        try {
            List<Vertex> vertices =
                    batch.stream()
                            .map(env -> (Vertex) env.getElement())
                            .collect(Collectors.toList());
            if (updateStrategies.isEmpty()) {
                client.batchWriteVertices(vertices);
            } else {
                client.batchUpdateVertices(vertices, updateStrategies);
            }
        } catch (Exception e) {
            if (!batchFailureFallback) {
                logBatchFailure(batch, e);
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        "Failed to write vertex batch",
                        e);
            }
            fallbackInsertSingly(batch, e);
        }
    }

    private void doFlushEdges() {
        if (edgeBuffer.isEmpty()) {
            return;
        }
        List<GraphElementEnvelope> batch = new ArrayList<>(edgeBuffer);
        edgeBuffer.clear();
        for (Map.Entry<Map<String, UpdateStrategy>, List<GraphElementEnvelope>> group :
                groupByStrategy(batch).entrySet()) {
            flushEdgeGroup(group.getValue(), group.getKey());
        }
    }

    private void flushEdgeGroup(
            List<GraphElementEnvelope> batch, Map<String, UpdateStrategy> updateStrategies) {
        try {
            List<Edge> edges =
                    batch.stream().map(env -> (Edge) env.getElement()).collect(Collectors.toList());
            if (updateStrategies.isEmpty()) {
                client.batchWriteEdges(edges, checkVertex);
            } else {
                client.batchUpdateEdges(edges, updateStrategies, checkVertex);
            }
        } catch (Exception e) {
            if (!batchFailureFallback) {
                logBatchFailure(batch, e);
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        "Failed to write edge batch",
                        e);
            }
            fallbackInsertSingly(batch, e);
        }
    }

    /**
     * Groups a batch by its elements' update-strategy map, preserving first-seen order so a flush
     * stays deterministic. Elements sharing the same strategy map flush together in one server
     * call.
     */
    private static Map<Map<String, UpdateStrategy>, List<GraphElementEnvelope>> groupByStrategy(
            List<GraphElementEnvelope> batch) {
        Map<Map<String, UpdateStrategy>, List<GraphElementEnvelope>> groups =
                new java.util.LinkedHashMap<>();
        for (GraphElementEnvelope envelope : batch) {
            groups.computeIfAbsent(envelope.getUpdateStrategies(), key -> new ArrayList<>())
                    .add(envelope);
        }
        return groups;
    }

    /**
     * A batch insert failed; retry each element on its own so a single poison record no longer
     * fails the whole batch. Failed records are logged and skipped; the rest succeed. If
     * <em>every</em> record fails, the failure is systemic (bad connection / schema), not a poison
     * record, so it is rethrown instead of silently dropping the whole batch.
     */
    private void fallbackInsertSingly(List<GraphElementEnvelope> batch, Exception batchFailure) {
        LOG.warn(
                "Batch write failed ({} element(s)); falling back to single-record insert. cause={}",
                batch.size(),
                batchFailure.getMessage());
        int failed = 0;
        Exception lastFailure = null;
        for (GraphElementEnvelope envelope : batch) {
            Map<String, UpdateStrategy> updateStrategies = envelope.getUpdateStrategies();
            try {
                if (envelope.getElementType() == LabelType.VERTEX) {
                    if (updateStrategies.isEmpty()) {
                        client.writeVertex((Vertex) envelope.getElement());
                    } else {
                        client.updateVertex((Vertex) envelope.getElement(), updateStrategies);
                    }
                } else {
                    if (updateStrategies.isEmpty()) {
                        client.writeEdge((Edge) envelope.getElement(), checkVertex);
                    } else {
                        client.updateEdge(
                                (Edge) envelope.getElement(), updateStrategies, checkVertex);
                    }
                }
            } catch (Exception single) {
                failed++;
                lastFailure = single;
                insertFailureCount++;
                LOG.error(
                        "Single-record write failure — {}",
                        formatFailureDiagnostic(envelope, single));
                writeFailureSample(envelope, single);
                // Bound the previously unlimited silent skipping: once the cumulative number of
                // skipped records reaches max_insert_errors, stop and fail the task instead of
                // continuing to drop data. Negative means unlimited.
                if (maxInsertErrors >= 0 && insertFailureCount >= maxInsertErrors) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            String.format(
                                    "Aborting: cumulative single-insert failures (%d) reached "
                                            + "max_insert_errors (%d). Last error: %s",
                                    insertFailureCount, maxInsertErrors, single.getMessage()),
                            single);
                }
            }
        }
        if (failed == batch.size()) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    String.format(
                            "All %d record(s) in the batch failed single-insert fallback",
                            batch.size()),
                    lastFailure);
        }
        if (failed > 0) {
            LOG.warn(
                    "Single-record fallback completed: {} succeeded, {} failed and were skipped "
                            + "({} skipped in total so far)",
                    batch.size() - failed,
                    failed,
                    insertFailureCount);
        }
    }

    /**
     * Appends one line describing a skipped record — the mapped element's id/label/properties plus
     * the server error — to the per-subtask failure file when {@code failure_data_path} is set.
     * Best-effort: a write/open error disables further persistence rather than failing the task, so
     * a broken debug path can never mask the real insert failure.
     */
    private void writeFailureSample(GraphElementEnvelope envelope, Exception failure) {
        if (failureDataPath == null || failureDataPath.isEmpty() || failureWriterDisabled) {
            return;
        }
        try {
            if (failureWriter == null) {
                File dir = new File(failureDataPath);
                if (!dir.exists() && !dir.mkdirs() && !dir.exists()) {
                    throw new IOException("Failed to create failure data directory: " + dir);
                }
                File file =
                        new File(dir, "hugegraph-sink-failures-subtask-" + subtaskIndex + ".log");
                failureWriter =
                        new BufferedWriter(
                                new OutputStreamWriter(
                                        new FileOutputStream(file, true), StandardCharsets.UTF_8));
                LOG.info("Persisting skipped-record failure samples to {}", file.getAbsolutePath());
            }
            failureWriter.write(formatFailureSample(envelope, failure));
            failureWriter.newLine();
            // Flush per record: failures are rare and losing samples on an abrupt crash defeats
            // their purpose.
            failureWriter.flush();
        } catch (IOException e) {
            failureWriterDisabled = true;
            LOG.warn(
                    "Failed to persist failure sample to '{}'; disabling failure-data persistence. cause={}",
                    failureDataPath,
                    e.getMessage());
        }
    }

    /**
     * One-line, tab-delimited failure sample. Newlines are stripped to keep one record per line.
     */
    static String formatFailureSample(GraphElementEnvelope envelope, Exception failure) {
        GraphElement element = envelope.getElement();
        String line =
                String.format(
                        "mapping=%s\ttype=%s\tid=%s\tlabel=%s\tproperties=%s\terror=%s",
                        envelope.getMappingLabel(),
                        envelope.getElementType(),
                        element == null ? null : element.id(),
                        element == null ? null : element.label(),
                        element == null ? null : element.properties(),
                        failure.getMessage());
        return line.replace('\n', ' ').replace('\r', ' ');
    }

    private void logBatchFailure(List<GraphElementEnvelope> batch, Exception e) {
        LOG.error(
                "Batch write failure — {} element(s), failureType={}, serverError={}",
                batch.size(),
                e.getClass().getName(),
                e.getMessage());
        for (GraphElementEnvelope envelope : batch) {
            LOG.error("Graph element write failure — {}", formatFailureDiagnostic(envelope, e));
        }
    }

    static String formatFailureDiagnostic(GraphElementEnvelope envelope, Exception failure) {
        // Log only the mapped graph element's id/label — bounded and non-sensitive. The raw source
        // row is intentionally not retained (see GraphElementEnvelope) to avoid unbounded memory
        // and
        // leaking excluded field content into logs.
        return String.format(
                "mapping=%s, elementType=%s, elementId=%s, elementLabel=%s, failureType=%s, serverError=%s",
                envelope.getMappingLabel(),
                envelope.getElementType(),
                envelope.getElement() == null ? null : envelope.getElement().id(),
                envelope.getElement() == null ? null : envelope.getElement().label(),
                failure.getClass().getName(),
                failure.getMessage());
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        LOG.info("Closing BatchBuffer, performing final flush...");
        try {
            flush();
        } finally {
            closeFailureWriter();
        }
        LOG.info("BatchBuffer closed.");
    }

    private void closeFailureWriter() {
        if (failureWriter != null) {
            try {
                failureWriter.close();
            } catch (IOException e) {
                LOG.warn("Failed to close failure-data writer. cause={}", e.getMessage());
            } finally {
                failureWriter = null;
            }
        }
    }
}
