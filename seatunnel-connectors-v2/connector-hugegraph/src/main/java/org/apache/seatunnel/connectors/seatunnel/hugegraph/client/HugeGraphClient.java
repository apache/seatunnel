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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.client;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.LabelOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.client.RestClient;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.exception.ServerException;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.rest.RestClientConfig;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Edges;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.graph.Vertices;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class HugeGraphClient implements HugeGraphOperations {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphClient.class);

    /** HugeGraph server per-request batch cap (server option batch.max_vertices_per_batch). */
    private static final int MAX_RECORDS_PER_BATCH_REQUEST = 500;

    private HugeClient client;
    private RestClient restClient;
    private VertexAPI vertexAPI;
    private EdgeAPI edgeAPI;
    private SchemaManager schema;
    private final HugeGraphConnectionConfig config;
    private final int maxRetries;
    private final long retryBackoffMs;

    public HugeGraphClient(HugeGraphConnectionConfig config) {
        this.client = null;
        this.restClient = null;
        this.vertexAPI = null;
        this.edgeAPI = null;
        this.schema = null;
        this.config = config;
        this.maxRetries = Math.max(0, config.getMaxRetries());
        this.retryBackoffMs = Math.max(0, config.getRetryBackoffMs());
    }

    /** Default graph space per HugeGraphOptions.GRAPH_SPACE.defaultValue(). */
    private static final String DEFAULT_GRAPH_SPACE = "DEFAULT";

    private HugeClient createClient(HugeGraphConnectionConfig config) {
        try {
            String url = buildServerUrl(config);
            String graphSpace =
                    config.getGraphSpace() != null ? config.getGraphSpace() : DEFAULT_GRAPH_SPACE;
            LOG.debug(
                    "Creating new HugeClient for url: {}, graphSpace: {}, graph: {}",
                    url,
                    graphSpace,
                    config.getGraphName());

            HugeClient client =
                    HugeClient.builder(url, graphSpace, config.getGraphName())
                            .configUser(config.getUsername(), config.getPassword())
                            .configIdleTime(60)
                            .build();

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

    @FunctionalInterface
    private interface ReadOperation<T> {
        T execute() throws ServerException, ClientException;
    }

    private void ensureClientInitialized() throws HugeGraphConnectorException {
        if (this.client == null) {
            LOG.info("Client not initialized. Attempting to connect...");
            try {
                this.client = createClient(this.config);
                this.schema = this.client.schema();
                createPageApis(this.config);
                LOG.info("HugeClient initialized successfully.");
            } catch (Exception e) {
                // Avoid leaking a partially-opened client (e.g. createPageApis failed after the
                // HugeClient was created) — release everything before surfacing the failure.
                reconnect();
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
        if (this.restClient != null) {
            try {
                this.restClient.close();
            } catch (Exception e) {
                LOG.warn("Error closing potentially broken REST client: {}", e.getMessage());
            }
        }
        this.restClient = null;
        this.vertexAPI = null;
        this.edgeAPI = null;
        this.schema = null;
    }

    private void createPageApis(HugeGraphConnectionConfig config) {
        String url = buildServerUrl(config);
        String graphSpace =
                config.getGraphSpace() != null ? config.getGraphSpace() : DEFAULT_GRAPH_SPACE;
        RestClientConfig restClientConfig =
                RestClientConfig.builder()
                        .user(config.getUsername() == null ? "" : config.getUsername())
                        .password(config.getPassword() == null ? "" : config.getPassword())
                        .build();
        this.restClient = new RestClient(url, restClientConfig);
        this.vertexAPI = new VertexAPI(this.restClient, graphSpace, config.getGraphName());
        this.edgeAPI = new EdgeAPI(this.restClient, graphSpace, config.getGraphName());
    }

    static String buildServerUrl(HugeGraphConnectionConfig config) {
        String protocol =
                config.getProtocol() == null || config.getProtocol().isEmpty()
                        ? "http"
                        : config.getProtocol().toLowerCase(java.util.Locale.ROOT);
        return String.format("%s://%s:%d", protocol, config.getHost(), config.getPort());
    }

    private void executeGraphOperation(GraphOperation operation) {
        int totalAttempts = this.maxRetries + 1;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                ensureClientInitialized();
                operation.execute(this.client.graph());
                return;
            } catch (ServerException | ClientException e) {
                if (!isRetryable(e)) {
                    LOG.error("Server rejected the request (non-retryable): {}", e.getMessage());
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            "Server rejected the request (non-retryable): " + e.getMessage(),
                            e);
                }
                LOG.warn(
                        "Graph operation failed on attempt {}/{}. Error: {}",
                        attempt,
                        totalAttempts,
                        e.getMessage());
                reconnect();

                if (attempt == totalAttempts) {
                    LOG.error("Max retries ({}) reached. Failing task.", this.maxRetries);
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            "Failed to execute graph operation after "
                                    + totalAttempts
                                    + " attempt(s). Last error: "
                                    + e.getMessage(),
                            e);
                }

                sleepBeforeRetry();
            } catch (HugeGraphConnectorException e) {
                if (!HugeGraphConnectorErrorCode.BUILD_CLIENT_FAILED
                        .getCode()
                        .equals(e.getSeaTunnelErrorCode().getCode())) {
                    throw e;
                }
                reconnect();
                if (attempt == totalAttempts) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.BUILD_CLIENT_FAILED,
                            "Failed to establish HugeGraph connection after "
                                    + totalAttempts
                                    + " attempt(s)",
                            e);
                }
                LOG.warn(
                        "HugeGraph connection failed on attempt {}/{}. Error: {}",
                        attempt,
                        totalAttempts,
                        e.getMessage());
                sleepBeforeRetry();
            } catch (Exception e) {
                LOG.error("Non-retryable error executing graph operation: {}", e.getMessage(), e);
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        "Non-retryable error executing graph operation: " + e.getMessage(),
                        e);
            }
        }
    }

    /**
     * Deterministic 4xx responses (bad request, semantic rejection such as exceeding the server
     * batch size cap) cannot succeed on retry. Only connection-level failures and 5xx server errors
     * are worth retrying.
     */
    static boolean isRetryable(Exception e) {
        if (e instanceof ServerException) {
            int status = ((ServerException) e).status();
            return status == 408 || status == 425 || status == 429 || status >= 500;
        }
        return true;
    }

    private <T> T executeReadOperation(ReadOperation<T> operation) {
        int totalAttempts = this.maxRetries + 1;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                ensureClientInitialized();
                return operation.execute();
            } catch (ServerException | ClientException e) {
                if (!isRetryable(e)) {
                    LOG.error("Server rejected the request (non-retryable): {}", e.getMessage());
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            "Server rejected the request (non-retryable): " + e.getMessage(),
                            e);
                }
                LOG.warn(
                        "Graph read operation failed on attempt {}/{}. Error: {}",
                        attempt,
                        totalAttempts,
                        e.getMessage());
                reconnect();

                if (attempt == totalAttempts) {
                    LOG.error("Max retries ({}) reached. Failing task.", this.maxRetries);
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            "Failed to execute graph read operation after "
                                    + totalAttempts
                                    + " attempt(s). Last error: "
                                    + e.getMessage(),
                            e);
                }

                sleepBeforeRetry();
            } catch (HugeGraphConnectorException e) {
                if (!HugeGraphConnectorErrorCode.BUILD_CLIENT_FAILED
                        .getCode()
                        .equals(e.getSeaTunnelErrorCode().getCode())) {
                    throw e;
                }
                reconnect();
                if (attempt == totalAttempts) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.BUILD_CLIENT_FAILED,
                            "Failed to establish HugeGraph connection after "
                                    + totalAttempts
                                    + " attempt(s)",
                            e);
                }
                LOG.warn(
                        "HugeGraph connection failed on attempt {}/{}. Error: {}",
                        attempt,
                        totalAttempts,
                        e.getMessage());
                sleepBeforeRetry();
            } catch (Exception e) {
                LOG.error(
                        "Non-retryable error executing graph read operation: {}",
                        e.getMessage(),
                        e);
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        "Non-retryable error executing graph read operation",
                        e);
            }
        }
        throw new HugeGraphConnectorException(
                HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                "Failed to execute graph read operation");
    }

    private void sleepBeforeRetry() {
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
    }

    private SchemaManager getSchema() {
        ensureClientInitialized();
        return this.schema;
    }

    // --- Schema read operations ---

    public PropertyKey getPropertyKey(String propertyName) {
        return executeReadOperation(() -> getSchema().getPropertyKey(propertyName));
    }

    public VertexLabel getVertexLabel(String label) {
        VertexLabel vertexLabel = getVertexLabelOrNull(label);
        if (vertexLabel == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    "Vertex label '"
                            + label
                            + "' does not exist in HugeGraph. "
                            + "Please create it first or check your configuration.");
        }
        return vertexLabel;
    }

    public EdgeLabel getEdgeLabel(String label) {
        EdgeLabel edgeLabel = getEdgeLabelOrNull(label);
        if (edgeLabel == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    "Edge label '"
                            + label
                            + "' does not exist in HugeGraph. "
                            + "Please create it first or check your configuration.");
        }
        return edgeLabel;
    }

    public String getVertexLabelId(String label) {
        VertexLabel vertexLabel = getVertexLabel(label);
        return String.valueOf(vertexLabel.id());
    }

    public String getEdgeLabelId(String label) {
        EdgeLabel edgeLabel = getEdgeLabel(label);
        return String.valueOf(edgeLabel.id());
    }

    public IdStrategy getIdStrategy(String label) {
        VertexLabel vertexLabel = getVertexLabel(label);
        return vertexLabel.idStrategy();
    }

    // --- Schema creation operations (idempotent, ifNotExist) ---

    public PropertyKey createPropertyKeyIfNotExist(
            String name,
            DataType dataType,
            org.apache.hugegraph.structure.constant.Cardinality cardinality) {
        return executeReadOperation(
                () ->
                        getSchema()
                                .propertyKey(name)
                                .dataType(dataType)
                                .cardinality(cardinality)
                                .ifNotExist()
                                .create());
    }

    public VertexLabel createVertexLabelIfNotExist(
            String label,
            IdStrategy idStrategy,
            List<String> primaryKeys,
            List<String> propertyNames,
            List<String> nullableKeys,
            LabelOptions options) {
        return executeReadOperation(
                () -> {
                    VertexLabel.Builder builder =
                            getSchema().vertexLabel(label).idStrategy(idStrategy);
                    if (idStrategy == IdStrategy.PRIMARY_KEY && primaryKeys != null) {
                        builder.primaryKeys(primaryKeys.toArray(new String[0]));
                    }
                    if (propertyNames != null && !propertyNames.isEmpty()) {
                        builder.properties(propertyNames.toArray(new String[0]));
                    }
                    if (nullableKeys != null && !nullableKeys.isEmpty()) {
                        builder.nullableKeys(nullableKeys.toArray(new String[0]));
                    }
                    if (options != null) {
                        if (options.getTtl() != null && options.getTtl() > 0) {
                            builder.ttl(options.getTtl());
                            if (options.getTtlStartTime() != null
                                    && !options.getTtlStartTime().isEmpty()) {
                                builder.ttlStartTime(options.getTtlStartTime());
                            }
                        }
                        if (options.getEnableLabelIndex() != null) {
                            builder.enableLabelIndex(options.getEnableLabelIndex());
                        }
                        if (options.getUserdata() != null) {
                            for (Map.Entry<String, Object> entry :
                                    options.getUserdata().entrySet()) {
                                builder.userdata(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                    return builder.ifNotExist().create();
                });
    }

    public EdgeLabel createEdgeLabelIfNotExist(
            String label,
            String sourceLabel,
            String targetLabel,
            Frequency frequency,
            List<String> sortKeys,
            List<String> propertyNames,
            List<String> nullableKeys,
            LabelOptions options) {
        return executeReadOperation(
                () -> {
                    EdgeLabel.Builder builder =
                            getSchema()
                                    .edgeLabel(label)
                                    .sourceLabel(sourceLabel)
                                    .targetLabel(targetLabel);
                    if (frequency != null) {
                        builder.frequency(frequency);
                    }
                    if (sortKeys != null && !sortKeys.isEmpty()) {
                        builder.sortKeys(sortKeys.toArray(new String[0]));
                    }
                    if (propertyNames != null && !propertyNames.isEmpty()) {
                        builder.properties(propertyNames.toArray(new String[0]));
                    }
                    if (nullableKeys != null && !nullableKeys.isEmpty()) {
                        builder.nullableKeys(nullableKeys.toArray(new String[0]));
                    }
                    if (options != null) {
                        if (options.getTtl() != null && options.getTtl() > 0) {
                            builder.ttl(options.getTtl());
                            if (options.getTtlStartTime() != null
                                    && !options.getTtlStartTime().isEmpty()) {
                                builder.ttlStartTime(options.getTtlStartTime());
                            }
                        }
                        if (options.getEnableLabelIndex() != null) {
                            builder.enableLabelIndex(options.getEnableLabelIndex());
                        }
                        if (options.getUserdata() != null) {
                            for (Map.Entry<String, Object> entry :
                                    options.getUserdata().entrySet()) {
                                builder.userdata(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                    return builder.ifNotExist().create();
                });
    }

    /** Check if a property key exists. Returns null if not found. */
    public PropertyKey getPropertyKeyOrNull(String name) {
        return executeReadOperation(
                () -> {
                    try {
                        return getSchema().getPropertyKey(name);
                    } catch (ServerException e) {
                        if (e.status() == 404
                                || (e.getMessage() != null
                                        && e.getMessage().contains("does not exist"))) {
                            return null;
                        }
                        throw e;
                    }
                });
    }

    /** Check if a vertex label exists. Returns null if not found. */
    public VertexLabel getVertexLabelOrNull(String label) {
        return executeReadOperation(
                () -> {
                    try {
                        return getSchema().getVertexLabel(label);
                    } catch (ServerException e) {
                        if (e.status() == 404
                                || (e.getMessage() != null
                                        && e.getMessage().contains("does not exist"))) {
                            return null;
                        }
                        throw e;
                    }
                });
    }

    /** Check if an edge label exists. Returns null if not found. */
    public EdgeLabel getEdgeLabelOrNull(String label) {
        return executeReadOperation(
                () -> {
                    try {
                        return getSchema().getEdgeLabel(label);
                    } catch (ServerException e) {
                        if (e.status() == 404
                                || (e.getMessage() != null
                                        && e.getMessage().contains("does not exist"))) {
                            return null;
                        }
                        throw e;
                    }
                });
    }

    @Override
    public Set<String> getVertexLabelPropertiesOrNull(String label) {
        VertexLabel vertexLabel = getVertexLabelOrNull(label);
        return vertexLabel == null ? null : vertexLabel.properties();
    }

    @Override
    public Set<String> getEdgeLabelPropertiesOrNull(String label) {
        EdgeLabel edgeLabel = getEdgeLabelOrNull(label);
        return edgeLabel == null ? null : edgeLabel.properties();
    }

    @Override
    public DataType getPropertyDataType(String propertyName) {
        return getPropertyKey(propertyName).dataType();
    }

    @Override
    public org.apache.hugegraph.structure.constant.Cardinality getPropertyCardinality(
            String propertyName) {
        return getPropertyKey(propertyName).cardinality();
    }

    // --- Graph write operations ---

    public void writeVertex(Vertex vertex) {
        executeGraphOperation(graph -> graph.addVertex(vertex));
    }

    public void writeEdge(Edge edge) {
        executeGraphOperation(graph -> graph.addEdge(edge));
    }

    /**
     * Writes vertices in chunks of at most {@link #MAX_RECORDS_PER_BATCH_REQUEST}. The HugeGraph
     * server rejects batch requests above its per-request cap (default 500, see server option
     * batch.max_vertices_per_batch), so a user-configured batch_size larger than the cap is split
     * client-side instead of failing wholesale.
     */
    public void batchWriteVertices(List<Vertex> buffer) {
        for (int start = 0; start < buffer.size(); start += MAX_RECORDS_PER_BATCH_REQUEST) {
            List<Vertex> chunk =
                    buffer.subList(
                            start, Math.min(start + MAX_RECORDS_PER_BATCH_REQUEST, buffer.size()));
            executeGraphOperation(graph -> graph.addVertices(chunk));
        }
    }

    /** Writes edges in server-cap-sized chunks. See {@link #batchWriteVertices}. */
    public void batchWriteEdges(List<Edge> buffer) {
        for (int start = 0; start < buffer.size(); start += MAX_RECORDS_PER_BATCH_REQUEST) {
            List<Edge> chunk =
                    buffer.subList(
                            start, Math.min(start + MAX_RECORDS_PER_BATCH_REQUEST, buffer.size()));
            executeGraphOperation(graph -> graph.addEdges(chunk, false));
        }
    }

    // --- Graph read operations ---

    /**
     * Lists one page of vertices. HugeGraph only enters paged mode when the {@code page} query
     * parameter is present — a null first page must be sent as an empty string, otherwise the
     * server returns a single non-paged batch without a next-page marker and the scan silently
     * stops after {@code limit} records.
     */
    @SuppressWarnings("unchecked")
    @Override
    public PageResult<Vertex> listVertices(String label, String page, int limit) {
        String effectivePage = page == null ? "" : page;
        return executeReadOperation(
                () -> {
                    Vertices vertices =
                            this.vertexAPI.list(label, null, false, 0, effectivePage, limit);
                    List<Vertex> records = (List<Vertex>) vertices.results();
                    return new PageResult<>(
                            records == null ? Collections.emptyList() : records, vertices.page());
                });
    }

    /** Lists one page of edges. See {@link #listVertices} for the empty-first-page contract. */
    @SuppressWarnings("unchecked")
    @Override
    public PageResult<Edge> listEdges(String label, String page, int limit) {
        String effectivePage = page == null ? "" : page;
        return executeReadOperation(
                () -> {
                    Edges edges =
                            this.edgeAPI.list(
                                    null, null, label, null, false, 0, effectivePage, limit);
                    List<Edge> records = (List<Edge>) edges.results();
                    return new PageResult<>(
                            records == null ? Collections.emptyList() : records, edges.page());
                });
    }

    // --- Graph delete operations ---

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

    @Override
    public void close() {
        RuntimeException closeFailure = null;
        if (this.client != null) {
            LOG.info("Closing HugeClient instance.");
            try {
                this.client.close();
            } catch (RuntimeException e) {
                closeFailure = e;
            }
            this.client = null;
        }
        if (this.restClient != null) {
            try {
                this.restClient.close();
            } catch (RuntimeException e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
            this.restClient = null;
        }
        this.vertexAPI = null;
        this.edgeAPI = null;
        this.schema = null;
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
