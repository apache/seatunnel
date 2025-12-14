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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.GraphElement;
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

public class BatchBuffer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuffer.class);

    private final List<GraphElement> buffer = new ArrayList<>();
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

    public synchronized void add(GraphElement element) throws IOException {
        checkFlushException();
        if (closed) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.BUFFER_ADD_FAILED,
                    "BatchBuffer is already closed.");
        }

        try {
            buffer.add(element);
            if (buffer.size() >= batchSize) {
                doFlush();
            }
        } catch (Exception e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED, e);
        }
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if (closed && buffer.isEmpty()) {
            return;
        }
        doFlush();
    }

    private void doFlush() {
        if (buffer.isEmpty()) {
            return;
        }
        try {
            GraphElement firstElement = buffer.get(0);
            if (firstElement instanceof Vertex) {
                List<Vertex> vertices =
                        buffer.stream()
                                .map(element -> (Vertex) element)
                                .collect(Collectors.toList());
                client.batchWriteVertices(vertices);
            } else {
                List<Edge> edges =
                        buffer.stream().map(element -> (Edge) element).collect(Collectors.toList());
                client.batchWriteEdges(edges);
            }

            buffer.clear();
        } catch (Exception e) {
            LOG.error("Failed to write batch data to HugeGraph", e);
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED, e.getMessage(), e);
        }
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
