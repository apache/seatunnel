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

import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Dual-bucket batch buffer that independently accumulates and flushes vertices and edges. Each
 * bucket triggers flush when reaching batch_size; both buckets are flushed on timer, prepareCommit,
 * or close.
 */
public class BatchBuffer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuffer.class);

    private final List<GraphElementEnvelope> vertexBuffer = new ArrayList<>();
    private final List<GraphElementEnvelope> edgeBuffer = new ArrayList<>();
    private final int batchSize;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> scheduledFuture;

    private volatile boolean closed = false;
    private volatile Exception flushException;
    private final HugeGraphClient client;

    public BatchBuffer(HugeGraphClient client, int batchSize, long batchIntervalMs) {
        this.batchSize = batchSize;
        this.client = client;

        if (batchIntervalMs > 0) {
            this.scheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable, "hugegraph-sink-flusher");
                                thread.setDaemon(true);
                                return thread;
                            });
            this.scheduledFuture =
                    this.scheduler.scheduleAtFixedRate(
                            () -> {
                                try {
                                    flush();
                                } catch (Exception e) {
                                    flushException = e;
                                }
                            },
                            batchIntervalMs,
                            batchIntervalMs,
                            TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    public synchronized void add(GraphElementEnvelope envelope) throws IOException {
        checkFlushException();
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
                    if (!vertexBuffer.isEmpty()) {
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

    public synchronized void flush() throws IOException {
        checkFlushException();
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
        try {
            List<Vertex> vertices =
                    batch.stream()
                            .map(env -> (Vertex) env.getElement())
                            .collect(Collectors.toList());
            client.batchWriteVertices(vertices);
        } catch (Exception e) {
            logBatchFailure(batch, e);
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Failed to write vertex batch",
                    e);
        }
    }

    private void doFlushEdges() {
        if (edgeBuffer.isEmpty()) {
            return;
        }
        List<GraphElementEnvelope> batch = new ArrayList<>(edgeBuffer);
        edgeBuffer.clear();
        try {
            List<Edge> edges =
                    batch.stream().map(env -> (Edge) env.getElement()).collect(Collectors.toList());
            client.batchWriteEdges(edges);
        } catch (Exception e) {
            logBatchFailure(batch, e);
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Failed to write edge batch",
                    e);
        }
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
        return String.format(
                "mapping=%s, elementType=%s, sourceRow=%s, failureType=%s, serverError=%s",
                envelope.getMappingLabel(),
                envelope.getElementType(),
                envelope.getSourceRow(),
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

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Closing BatchBuffer, performing final flush...");
        flush();
        checkFlushException();
        LOG.info("BatchBuffer closed.");
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ASYNCHRONOUS_FLUSH_FAILED, flushException);
        }
    }
}
