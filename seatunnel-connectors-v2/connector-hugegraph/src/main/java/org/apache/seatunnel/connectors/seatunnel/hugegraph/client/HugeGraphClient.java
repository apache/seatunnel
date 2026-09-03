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
import org.apache.hugegraph.structure.graph.BatchEdgeRequest;
import org.apache.hugegraph.structure.graph.BatchVertexRequest;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Edges;
import org.apache.hugegraph.structure.graph.Shard;
import org.apache.hugegraph.structure.graph.UpdateStrategy;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.graph.Vertices;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private final long retryBackoffMaxMs;

    public HugeGraphClient(HugeGraphConnectionConfig config) {
        this.client = null;
        this.restClient = null;
        this.vertexAPI = null;
        this.edgeAPI = null;
        this.schema = null;
        this.config = config;
        this.maxRetries = Math.max(0, config.getMaxRetries());
        this.retryBackoffMs = Math.max(0, config.getRetryBackoffMs());
        this.retryBackoffMaxMs = Math.max(0, config.getRetryBackoffMaxMs());
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

    /**
     * Executes a write operation that is safe to retry: UPSERT (updateVertices/updateEdges with
     * createIfNotExist=true) and DELETE (removeVertex/removeEdge). Idempotent operations are
     * retried on retryable errors because a second attempt cannot create duplicates.
     */
    private void executeIdempotentWrite(GraphOperation operation) {
        executeGraphOperation(operation, true);
    }

    /**
     * Executes a write operation that is NOT safe to retry: plain INSERT (addVertex/addVertices/
     * addEdge/addEdges). A retry after a server-committed-but-client-timed-out response would
     * create a duplicate element. Non-idempotent writes fail fast — the caller's single-record
     * fallback handles them individually instead.
     */
    private void executeNonIdempotentWrite(GraphOperation operation) {
        try {
            ensureClientInitialized();
            operation.execute(this.client.graph());
        } catch (ServerException | ClientException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Non-idempotent write failed (not retried to avoid duplicates): "
                            + e.getMessage(),
                    e);
        }
    }

    /**
     * Executes a graph write with optional retry. When {@code idempotent} is true, retryable server
     * errors (status &ge; 500, 408, 425, 429) are retried up to {@code maxRetries} times with
     * exponential backoff. When false, the operation is attempted once — if it fails the exception
     * propagates immediately so the caller can route through the single-record fallback or skip the
     * record.
     */
    private void executeGraphOperation(GraphOperation operation, boolean idempotent) {
        int totalAttempts = idempotent ? this.maxRetries + 1 : 1;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                ensureClientInitialized();
                operation.execute(this.client.graph());
                return;
            } catch (ServerException | ClientException e) {
                if (!isRetryable(e) || !idempotent) {
                    LOG.error(
                            "Server rejected the request ({}): {}",
                            idempotent ? "non-retryable" : "non-idempotent, not retrying",
                            e.getMessage());
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            "Server rejected the request"
                                    + (idempotent ? " (non-retryable)" : " (non-idempotent)")
                                    + ": "
                                    + e.getMessage(),
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

                sleepBeforeRetry(attempt);
            } catch (HugeGraphConnectorException e) {
                if (!HugeGraphConnectorErrorCode.BUILD_CLIENT_FAILED
                        .getCode()
                        .equals(e.getSeaTunnelErrorCode().getCode())) {
                    throw e;
                }
                if (!idempotent) {
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
                sleepBeforeRetry(attempt);
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

                sleepBeforeRetry(attempt);
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
                sleepBeforeRetry(attempt);
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

    private void sleepBeforeRetry(int attempt) {
        long delay = computeBackoffMs(retryBackoffMs, retryBackoffMaxMs, attempt);
        try {
            LOG.info("Will retry in {} ms (attempt {})...", delay, attempt);
            Thread.sleep(delay);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.OPERATION_RETRY_INTERRUPTED,
                    "Graph operation retry was interrupted",
                    ie);
        }
    }

    /**
     * Exponential backoff: {@code baseMs * 2^(attempt-1)}, capped at {@code maxMs} (a non-positive
     * {@code maxMs} means no cap). The shift is bounded so a large {@code maxRetries} cannot
     * overflow. {@code attempt} is 1-based (the first retry uses the base delay).
     */
    static long computeBackoffMs(long baseMs, long maxMs, int attempt) {
        if (baseMs <= 0) {
            return 0;
        }
        int shift = Math.min(Math.max(attempt - 1, 0), 30);
        long scaled = baseMs << shift;
        if (scaled < 0) {
            // Overflow guard (defensive; the shift cap already prevents this for int-range bases).
            return maxMs > 0 ? maxMs : Long.MAX_VALUE;
        }
        return (maxMs > 0) ? Math.min(scaled, maxMs) : scaled;
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
    public List<String> listVertexLabels() {
        return executeReadOperation(
                () -> {
                    List<String> names = new ArrayList<>();
                    for (VertexLabel vertexLabel : getSchema().getVertexLabels()) {
                        names.add(vertexLabel.name());
                    }
                    return names;
                });
    }

    @Override
    public List<String> listEdgeLabels() {
        return executeReadOperation(
                () -> {
                    List<String> names = new ArrayList<>();
                    for (EdgeLabel edgeLabel : getSchema().getEdgeLabels()) {
                        names.add(edgeLabel.name());
                    }
                    return names;
                });
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

    /**
     * Plain vertex insert — NOT idempotent. A retry after a server-committed-but-client-timed-out
     * response would create a duplicate. Fails fast on the first error; the caller's single-record
     * fallback handles the record individually instead.
     */
    public void writeVertex(Vertex vertex) {
        executeNonIdempotentWrite(graph -> graph.addVertex(vertex));
    }

    /** Plain edge insert — NOT idempotent. See {@link #writeVertex}. */
    public void writeEdge(Edge edge, boolean checkVertex) {
        // Route through addEdges so the single-insert path honors checkVertex the same way the
        // batch path does (GraphManager.addEdge has no checkVertex overload).
        executeNonIdempotentWrite(
                graph -> graph.addEdges(Collections.singletonList(edge), checkVertex));
    }

    /** Single-vertex property-merge upsert; idempotent — see {@link #batchUpdateVertices}. */
    public void updateVertex(Vertex vertex, Map<String, UpdateStrategy> updateStrategies) {
        batchUpdateVertices(Collections.singletonList(vertex), updateStrategies);
    }

    /** Single-edge property-merge upsert; idempotent — see {@link #batchUpdateEdges}. */
    public void updateEdge(
            Edge edge, Map<String, UpdateStrategy> updateStrategies, boolean checkVertex) {
        batchUpdateEdges(Collections.singletonList(edge), updateStrategies, checkVertex);
    }

    /**
     * Upserts vertices with per-property merge strategies (OVERRIDE / APPEND / SUM / UNION / ...)
     * instead of overwriting. Existing vertices are merged; missing ones are created
     * (createIfNotExist). Idempotent — safe to retry. Chunked like {@link #batchWriteVertices}.
     */
    public void batchUpdateVertices(
            List<Vertex> buffer, Map<String, UpdateStrategy> updateStrategies) {
        for (int start = 0; start < buffer.size(); start += MAX_RECORDS_PER_BATCH_REQUEST) {
            List<Vertex> chunk =
                    buffer.subList(
                            start, Math.min(start + MAX_RECORDS_PER_BATCH_REQUEST, buffer.size()));
            BatchVertexRequest request =
                    new BatchVertexRequest.Builder()
                            .vertices(chunk)
                            .updatingStrategies(updateStrategies)
                            .createIfNotExist(true)
                            .build();
            executeIdempotentWrite(graph -> graph.updateVertices(request));
        }
    }

    /**
     * Upserts edges with per-property merge strategies. Idempotent. See {@link
     * #batchUpdateVertices}.
     */
    public void batchUpdateEdges(
            List<Edge> buffer, Map<String, UpdateStrategy> updateStrategies, boolean checkVertex) {
        for (int start = 0; start < buffer.size(); start += MAX_RECORDS_PER_BATCH_REQUEST) {
            List<Edge> chunk =
                    buffer.subList(
                            start, Math.min(start + MAX_RECORDS_PER_BATCH_REQUEST, buffer.size()));
            BatchEdgeRequest request =
                    new BatchEdgeRequest.Builder()
                            .edges(chunk)
                            .updatingStrategies(updateStrategies)
                            .checkVertex(checkVertex)
                            .createIfNotExist(true)
                            .build();
            executeIdempotentWrite(graph -> graph.updateEdges(request));
        }
    }

    /**
     * Writes vertices in chunks of at most {@link #MAX_RECORDS_PER_BATCH_REQUEST}. The HugeGraph
     * server rejects batch requests above its per-request cap (default 500, see server option
     * batch.max_vertices_per_batch), so a user-configured batch_size larger than the cap is split
     * client-side instead of failing wholesale.
     *
     * <p>NOT idempotent — a retry after a server-committed-but-client-timed-out response would
     * create duplicates. Fails fast; the caller's single-record fallback handles each record.
     */
    public void batchWriteVertices(List<Vertex> buffer) {
        for (int start = 0; start < buffer.size(); start += MAX_RECORDS_PER_BATCH_REQUEST) {
            List<Vertex> chunk =
                    buffer.subList(
                            start, Math.min(start + MAX_RECORDS_PER_BATCH_REQUEST, buffer.size()));
            executeNonIdempotentWrite(graph -> graph.addVertices(chunk));
        }
    }

    /**
     * Writes edges in server-cap-sized chunks. NOT idempotent. See {@link #batchWriteVertices} and
     * {@link #batchWriteEdges}. When {@code checkVertex} is true the server verifies that each
     * edge's source/target vertices exist, rejecting orphan edges instead of silently writing them
     * or auto-creating phantom vertices.
     */
    public void batchWriteEdges(List<Edge> buffer, boolean checkVertex) {
        for (int start = 0; start < buffer.size(); start += MAX_RECORDS_PER_BATCH_REQUEST) {
            List<Edge> chunk =
                    buffer.subList(
                            start, Math.min(start + MAX_RECORDS_PER_BATCH_REQUEST, buffer.size()));
            executeNonIdempotentWrite(graph -> graph.addEdges(chunk, checkVertex));
        }
    }

    // --- Graph read operations ---

    /**
     * Lists one page of vertices. HugeGraph only enters paged mode when the {@code page} query
     * parameter is present — a null first page must be sent as an empty string, otherwise the
     * server returns a single non-paged batch without a next-page marker and the scan silently
     * stops after {@code limit} records.
     *
     * <p>When {@code filter} is non-empty it is passed as the server-side property-equality
     * condition map, with {@code keepP=true} so the filtered properties are retained in the
     * returned vertices (they may be part of the output schema).
     */
    @SuppressWarnings("unchecked")
    @Override
    public PageResult<Vertex> listVertices(
            String label, Map<String, Object> filter, String page, int limit) {
        String effectivePage = page == null ? "" : page;
        boolean hasFilter = filter != null && !filter.isEmpty();
        Map<String, Object> conditions = hasFilter ? filter : null;
        return executeReadOperation(
                () -> {
                    Vertices vertices =
                            this.vertexAPI.list(
                                    label, conditions, hasFilter, 0, effectivePage, limit);
                    List<Vertex> records = (List<Vertex>) vertices.results();
                    return new PageResult<>(
                            records == null ? Collections.emptyList() : records, vertices.page());
                });
    }

    /** Lists one page of edges. See {@link #listVertices} for the empty-first-page contract. */
    @SuppressWarnings("unchecked")
    @Override
    public PageResult<Edge> listEdges(
            String label, Map<String, Object> filter, String page, int limit) {
        String effectivePage = page == null ? "" : page;
        boolean hasFilter = filter != null && !filter.isEmpty();
        Map<String, Object> conditions = hasFilter ? filter : null;
        return executeReadOperation(
                () -> {
                    Edges edges =
                            this.edgeAPI.list(
                                    null,
                                    null,
                                    label,
                                    conditions,
                                    hasFilter,
                                    0,
                                    effectivePage,
                                    limit);
                    List<Edge> records = (List<Edge>) edges.results();
                    return new PageResult<>(
                            records == null ? Collections.emptyList() : records, edges.page());
                });
    }

    /**
     * Splits the vertex keyspace into shards for parallel scanning. Delegates to the server's
     * {@code traverser().vertexShards} API. Requires a scan-capable backend (RocksDB / HBase /
     * Cassandra).
     */
    @Override
    public List<Shard> vertexShards(long splitSize) {
        return executeReadOperation(() -> this.client.traverser().vertexShards(splitSize));
    }

    /** Splits the edge keyspace into shards. See {@link #vertexShards}. */
    @Override
    public List<Shard> edgeShards(long splitSize) {
        return executeReadOperation(() -> this.client.traverser().edgeShards(splitSize));
    }

    /**
     * Scans one page of vertices within {@code shard}. The empty-first-page contract of {@link
     * #listVertices} applies: a null page is sent as the empty string so the server enters paged
     * mode. The scan returns vertices of all labels in the key range; label filtering is the
     * caller's responsibility.
     */
    @Override
    public PageResult<Vertex> scanVertices(Shard shard, String page, int limit) {
        String effectivePage = page == null ? "" : page;
        return executeReadOperation(
                () -> {
                    Vertices vertices =
                            this.client.traverser().vertices(shard, effectivePage, limit);
                    List<Vertex> records = vertices.results();
                    return new PageResult<>(
                            records == null ? Collections.emptyList() : records, vertices.page());
                });
    }

    /** Scans one page of edges within {@code shard}. See {@link #scanVertices}. */
    @Override
    public PageResult<Edge> scanEdges(Shard shard, String page, int limit) {
        String effectivePage = page == null ? "" : page;
        return executeReadOperation(
                () -> {
                    Edges edges = this.client.traverser().edges(shard, effectivePage, limit);
                    List<Edge> records = edges.results();
                    return new PageResult<>(
                            records == null ? Collections.emptyList() : records, edges.page());
                });
    }

    // --- Graph delete operations ---

    /** Delete vertex by id — idempotent (removing an already-deleted vertex is a no-op). */
    public void deleteVertex(Object vertexId) {
        executeIdempotentWrite(graph -> graph.removeVertex(vertexId));
    }

    /** Delete edge by id — idempotent. */
    public void deleteEdge(String edgeId) {
        executeIdempotentWrite(graph -> graph.removeEdge(edgeId));
    }

    /** Delete vertex with its incident edges — idempotent. */
    public void deleteVertexWithEdges(Object vertexId) {
        executeIdempotentWrite(
                graph -> {
                    List<Edge> edges = graph.getEdges(vertexId);
                    for (Edge edge : edges) {
                        graph.removeEdge(edge.id());
                    }
                    graph.removeVertex(vertexId);
                });
    }

    /**
     * Returns the names of every edge label whose source or target endpoint is {@code vertexLabel}.
     * These are the edge labels that would be cascade-deleted if every vertex of {@code
     * vertexLabel} is removed — used by the DROP_DATA pre-flight safety check.
     */
    public List<String> getConnectedEdgeLabels(String vertexLabel) {
        return executeReadOperation(
                () -> {
                    List<String> connected = new ArrayList<>();
                    for (EdgeLabel edgeLabel : getSchema().getEdgeLabels()) {
                        if (vertexLabel.equals(edgeLabel.sourceLabel())
                                || vertexLabel.equals(edgeLabel.targetLabel())) {
                            connected.add(edgeLabel.name());
                        }
                    }
                    return connected;
                });
    }

    /** Page size used when clearing a single label's data for data_save_mode=DROP_DATA. */
    private static final int DELETE_PAGE_SIZE = 500;

    /**
     * Deletes every vertex of {@code label} (data only — the VertexLabel schema is preserved), used
     * by data_save_mode=DROP_DATA to clear just the labels this job targets instead of wiping the
     * whole graph with {@code clearGraph}. Removing a vertex also removes its incident edges on the
     * server. Works by repeatedly deleting the first page until none remain, so it does not depend
     * on a paging cursor staying valid across deletes.
     */
    public void deleteVerticesByLabel(String label) {
        LOG.info("data_save_mode=DROP_DATA: deleting all vertices of label '{}'", label);
        long deleted = 0;
        while (true) {
            List<Vertex> records = listVertices(label, null, "", DELETE_PAGE_SIZE).getRecords();
            if (records.isEmpty()) {
                break;
            }
            for (Vertex vertex : records) {
                deleteVertex(vertex.id());
                deleted++;
            }
        }
        LOG.info("Deleted {} vertices of label '{}'", deleted, label);
    }

    /**
     * Deletes every edge of {@code label} (data only — the EdgeLabel schema is preserved). See
     * {@link #deleteVerticesByLabel} for the paging strategy; run before vertex deletion so
     * edge-only mappings are handled even when their endpoints are out of this job's scope.
     */
    public void deleteEdgesByLabel(String label) {
        LOG.info("data_save_mode=DROP_DATA: deleting all edges of label '{}'", label);
        long deleted = 0;
        while (true) {
            List<Edge> records = listEdges(label, null, "", DELETE_PAGE_SIZE).getRecords();
            if (records.isEmpty()) {
                break;
            }
            for (Edge edge : records) {
                deleteEdge(edge.id());
                deleted++;
            }
        }
        LOG.info("Deleted {} edges of label '{}'", deleted, label);
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
