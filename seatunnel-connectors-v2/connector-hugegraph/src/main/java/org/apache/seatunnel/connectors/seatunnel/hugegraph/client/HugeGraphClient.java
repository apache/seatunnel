/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hugegraph.client;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.exception.ServerException;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class HugeGraphClient {

    // TODO: Add handling for schema fetch failures.
    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphClient.class);

    private HugeClient client;
    private SchemaManager schema;
    private final HugeGraphSinkConfig config;
    private final int maxRetries;
    private final long retryBackoffMs;

    public HugeGraphClient(HugeGraphSinkConfig config) {
        this.client = null;
        this.schema = null;
        this.config = config;
        this.maxRetries = config.getMaxRetries() > 0 ? config.getMaxRetries() : 3;
        this.retryBackoffMs = config.getRetryBackoffMs() > 0 ? config.getRetryBackoffMs() : 5000L;
    }

    private HugeClient createClient(HugeGraphSinkConfig config) {
        try {
            String url = String.format("http://%s:%d", config.getHost(), config.getPort());
            LOG.debug("Creating new HugeClient for url: {}, graph: {}", url, config.getGraphName());

            HugeClient client =
                    HugeClient.builder(url, config.getGraphName())
                            .configUser(config.getUsername(), config.getPassword())
                            .configIdleTime(60)
                            .build();

            client.graph().listVertices();
            LOG.info("Successfully created and validated HugeClient instance.");
            return client;
        } catch (Exception e) {
            LOG.error("Failed to create HugeClient. Error: {}", e.getMessage());
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.BUILD_CLIENT_FAILED, e);
        }
    }

    @FunctionalInterface
    private interface GraphOperation {
        void execute(GraphManager graph) throws ServerException, ClientException;
    }

    private void ensureClientInitialized() throws HugeGraphConnectorException {
        if (this.client == null) {
            LOG.info("Client not initialized. Attempting to connect...");
            try {
                this.client = createClient(this.config);
                this.schema = this.client.schema();
                LOG.info("HugeClient initialized successfully.");
            } catch (Exception e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.BUILD_CLIENT_FAILED,
                        "Failed to establish initial connection",
                        e);
            }
        }
    }

    private void reconnect() {
        LOG.warn("Connection issue detected. Forcing reconnection...");
        if (this.client != null) {
            try {
                this.client.close();
            } catch (Exception e) {
                LOG.warn("Error closing potentially broken client: {}", e.getMessage());
            }
        }
        this.client = null;
        this.schema = null;
    }

    private void executeGraphOperation(GraphOperation operation) {
        for (int attempt = 1; attempt <= this.maxRetries; attempt++) {
            try {
                ensureClientInitialized();
                operation.execute(this.client.graph());
                return;
            } catch (ServerException | ClientException e) {
                LOG.warn(
                        "Graph operation failed on attempt {}/{}. Error: {}",
                        attempt,
                        this.maxRetries,
                        e.getMessage());
                reconnect();

                if (attempt == this.maxRetries) {
                    LOG.error("Max retries ({}) reached. Failing task.", this.maxRetries);
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            "Failed to execute graph operation after "
                                    + this.maxRetries
                                    + " attempts",
                            e);
                }

                try {
                    LOG.info("Will retry in {} ms...", retryBackoffMs);
                    Thread.sleep(retryBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.OPERATION_RETRY_INTERRUPTED,
                            "Graph operation retry was interrupted",
                            ie);
                }

            } catch (Exception e) {
                LOG.error("Non-retryable error executing graph operation: {}", e.getMessage(), e);
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        "Non-retryable error executing graph operation",
                        e);
            }
        }
    }

    private SchemaManager getSchema() {
        ensureClientInitialized();
        return this.schema;
    }

    public PropertyKey getPropertyKey(String propertyName) {
        return getSchema().getPropertyKey(propertyName);
    }

    public VertexLabel getVertexLabel(String label) {
        return getSchema().getVertexLabel(label);
    }

    public EdgeLabel getEdgeLabel(String label) {
        return getSchema().getEdgeLabel(label);
    }

    public String getVertexLabelId(String label) {
        VertexLabel vertexLabel = getSchema().getVertexLabel(label);
        return String.valueOf(vertexLabel.id());
    }

    public String getEdgeLabelId(String label) {
        EdgeLabel edgeLabel = getSchema().getEdgeLabel(label);
        return String.valueOf(edgeLabel.id());
    }

    public IdStrategy getIdStrategy(String label) {
        VertexLabel vertexLabel = getSchema().getVertexLabel(label);
        return vertexLabel.idStrategy();
    }

    public void writeVertex(Vertex vertex) {
        executeGraphOperation(graph -> graph.addVertex(vertex));
    }

    public void writeEdge(Edge edge) {
        executeGraphOperation(graph -> graph.addEdge(edge));
    }

    public void deleteVertex(Object vertexId) {
        executeGraphOperation(graph -> graph.removeVertex(vertexId));
    }

    public void deleteEdge(String edgeId) {
        executeGraphOperation(graph -> graph.removeEdge(edgeId));
    }

    public void deleteVertexWithEdges(Object vertexId) {
        executeGraphOperation(
                graph -> {
                    List<Edge> edges = graph.getEdges(vertexId);
                    for (Edge edge : edges) {
                        graph.removeEdge(edge.id());
                    }
                    graph.removeVertex(vertexId);
                });
    }

    public void batchWriteVertices(List<Vertex> buffer) {
        executeGraphOperation(graph -> graph.addVertices(buffer));
    }

    public void batchWriteEdges(List<Edge> buffer) {
        executeGraphOperation(graph -> graph.addEdges(buffer));
    }

    public void close() {
        if (this.client != null) {
            LOG.info("Closing HugeClient instance.");
            this.client.close();
            this.client = null;
            this.schema = null;
        }
    }
}
