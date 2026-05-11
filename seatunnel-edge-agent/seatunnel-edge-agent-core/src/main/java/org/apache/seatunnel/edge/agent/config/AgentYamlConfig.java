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

package org.apache.seatunnel.edge.agent.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Typed representation of edge-agent {@code agent.yaml}.
 *
 * <p>Expected sections mirror shipped samples under {@code conf/agent.yaml}:
 *
 * <ul>
 *   <li>{@code inputs}: NDJSON collectors ({@code file}, {@code log}, {@code event}) validated
 *       per-type.
 *   <li>{@code output}: Hazelcast bootstrap addresses plus EdgeSocket fields ({@code
 *       cluster-addresses}, {@code job-id}, {@code auth-token}, {@code port}); ingress hosts are
 *       resolved via {@code getJobTaskGroupAddresses}.
 *   <li>{@code queue}: durable SQLite WAL path ({@code sqlite-path}) and {@code poll-batch-size}.
 *   <li>{@code batch}: in-memory accumulator limits ({@code bulk-max-size}, {@code
 *       flush-interval-ms}) applied before WAL enqueue.
 *   <li>{@code retry}: transport backoff ({@code backoff-ms}) and WAL row attempt ceiling ({@code
 *       max-attempts}) for failed sends.
 * </ul>
 *
 * @see AgentYamlLoader
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AgentYamlConfig {

    private List<InputDefinition> inputs = Collections.emptyList();
    private OutputDefinition output = new OutputDefinition();
    private QueueDefinition queue = new QueueDefinition();
    private BatchDefinition batch = new BatchDefinition();
    private RetryDefinition retry = new RetryDefinition();

    public List<InputDefinition> getInputs() {
        return inputs;
    }

    public void setInputs(List<InputDefinition> inputs) {
        this.inputs = inputs == null ? Collections.emptyList() : inputs;
    }

    public OutputDefinition getOutput() {
        return output;
    }

    public void setOutput(OutputDefinition output) {
        this.output = output == null ? new OutputDefinition() : output;
    }

    public QueueDefinition getQueue() {
        return queue;
    }

    public void setQueue(QueueDefinition queue) {
        this.queue = queue == null ? new QueueDefinition() : queue;
    }

    public BatchDefinition getBatch() {
        return batch;
    }

    public void setBatch(BatchDefinition batch) {
        this.batch = batch == null ? new BatchDefinition() : batch;
    }

    public RetryDefinition getRetry() {
        return retry;
    }

    public void setRetry(RetryDefinition retry) {
        this.retry = retry == null ? new RetryDefinition() : retry;
    }

    /**
     * Single logical input source listed under YAML {@code inputs}.
     *
     * <p>Type-dependent keys must satisfy {@link #validate(AgentYamlConfig)}:
     *
     * <ul>
     *   <li>{@code file} → required non-empty {@link #paths} (NDJSON line-wise in path order).
     *   <li>{@code log} → required {@link #path}; optional {@link #readFromBeginning}: {@code true}
     *       reads from file start on startup, {@code false} or omitted tails new bytes after EOF
     *       seek.
     *   <li>{@code event} → optional {@link #paths}: non-empty enables file-backed preload at
     *       {@link org.apache.seatunnel.edge.agent.connector.AgentInput#open()}; empty/omit selects
     *       memory-only mode for programmatic enqueue (YAML-only deployments normally specify
     *       paths).
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class InputDefinition {

        private String id;
        private String type;

        /**
         * {@code file} / {@code event}: YAML {@code paths} list (never blank strings when present).
         */
        private List<String> paths = Collections.emptyList();

        /** {@code log}: YAML {@code path} (single NDJSON log file). */
        private String path;

        /**
         * {@code log} only: YAML {@code read-from-beginning}; omit/false ⇒ tail-follow from EOF.
         */
        @JsonProperty("read-from-beginning")
        private Boolean readFromBeginning;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<String> getPaths() {
            return paths;
        }

        public void setPaths(List<String> paths) {
            this.paths = paths == null ? Collections.emptyList() : paths;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public Boolean getReadFromBeginning() {
            return readFromBeginning;
        }

        public void setReadFromBeginning(Boolean readFromBeginning) {
            this.readFromBeginning = readFromBeginning;
        }
    }

    /**
     * SeaTunnel client bootstrap plus EdgeSocket transport correlation.
     *
     * <p>{@code cluster-name} / {@code cluster-addresses} configure {@link
     * org.apache.seatunnel.engine.client.SeaTunnelClient} (Hazelcast cluster members). Actual Edge
     * ingress hosts are discovered at runtime via {@link
     * org.apache.seatunnel.engine.client.SeaTunnelClient#getJobTaskGroupAddresses(Long)} using
     * {@code job-id}.
     *
     * <p>{@code port} is the EdgeSocket ingress TCP port combined with each discovered {@code
     * host}. Line payloads follow {@link
     * org.apache.seatunnel.edge.agent.transport.EdgeSocketProtocol}. {@code auth-token} must match
     * the EdgeSocket source token on the engine side.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class OutputDefinition {

        @JsonProperty("cluster-name")
        private String clusterName = "seatunnel";

        @JsonProperty("cluster-addresses")
        private List<String> clusterAddresses = Collections.emptyList();

        private Long jobId;

        @JsonProperty("auth-token")
        private String authToken;

        /**
         * EdgeSocket ingress TCP port paired with hosts returned by job discovery for {@link
         * #jobId}.
         */
        private Integer port;

        @JsonProperty("connect-timeout-ms")
        private Integer connectTimeoutMs;

        @JsonProperty("read-timeout-ms")
        private Integer readTimeoutMs;

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public List<String> getClusterAddresses() {
            return clusterAddresses;
        }

        public void setClusterAddresses(List<String> clusterAddresses) {
            this.clusterAddresses =
                    clusterAddresses == null ? Collections.emptyList() : clusterAddresses;
        }

        /**
         * Accepts YAML scalars or strings for {@code job-id} so configs remain readable when IDs
         * are written quoted.
         */
        @JsonProperty("job-id")
        public void setJobIdFromYaml(Object raw) {
            if (raw == null) {
                this.jobId = null;
                return;
            }
            if (raw instanceof Number) {
                this.jobId = ((Number) raw).longValue();
                return;
            }
            String s = raw.toString().trim();
            if (s.isEmpty()) {
                this.jobId = null;
                return;
            }
            this.jobId = Long.parseLong(s);
        }

        public Long getJobId() {
            return jobId;
        }

        /** Programmatic override (same semantics as YAML {@code job-id}). */
        public void setJobId(Long jobId) {
            this.jobId = jobId;
        }

        public String getAuthToken() {
            return authToken;
        }

        public void setAuthToken(String authToken) {
            this.authToken = authToken;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public Integer getConnectTimeoutMs() {
            return connectTimeoutMs;
        }

        public void setConnectTimeoutMs(Integer connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
        }

        public Integer getReadTimeoutMs() {
            return readTimeoutMs;
        }

        public void setReadTimeoutMs(Integer readTimeoutMs) {
            this.readTimeoutMs = readTimeoutMs;
        }
    }

    /**
     * Local durable outbound queue backed by SQLite (WAL journal).
     *
     * <p>{@code sqlite-path} is required for persistence; {@code poll-batch-size} drives both input
     * {@link org.apache.seatunnel.edge.agent.connector.AgentInput#poll poll()} sizing and WAL batch
     * claiming on each agent loop iteration (default {@code 128}).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class QueueDefinition {

        @JsonProperty("sqlite-path")
        private String sqlitePath;

        @JsonProperty("poll-batch-size")
        private Integer pollBatchSize;

        public String getSqlitePath() {
            return sqlitePath;
        }

        public void setSqlitePath(String sqlitePath) {
            this.sqlitePath = sqlitePath;
        }

        public Integer getPollBatchSize() {
            return pollBatchSize;
        }

        public void setPollBatchSize(Integer pollBatchSize) {
            this.pollBatchSize = pollBatchSize;
        }
    }

    /**
     * In-memory batch accumulator tuning ({@link
     * org.apache.seatunnel.edge.agent.batch.RecordBatchAccumulator}) before records become WAL
     * rows.
     *
     * <p>{@code bulk-max-size} and {@code flush-interval-ms} flush whichever threshold is hit
     * first; the time window resets when the buffer transitions empty→non-empty after the previous
     * flush (defaults {@code 256} / {@code 1000} ms when YAML omits keys).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class BatchDefinition {

        @JsonProperty("bulk-max-size")
        private Integer bulkMaxSize;

        @JsonProperty("flush-interval-ms")
        private Long flushIntervalMs;

        public Integer getBulkMaxSize() {
            return bulkMaxSize;
        }

        public void setBulkMaxSize(Integer bulkMaxSize) {
            this.bulkMaxSize = bulkMaxSize;
        }

        public Long getFlushIntervalMs() {
            return flushIntervalMs;
        }

        public void setFlushIntervalMs(Long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
        }
    }

    /**
     * WAL-backed EdgeSocket send policy after transient failures.
     *
     * <p>{@code max-attempts} bounds SQLite {@code attempts} per row before {@link
     * org.apache.seatunnel.edge.agent.wal.SqliteOutboundWal#claimSendingBatch claimSendingBatch}
     * skips it (defaults {@code 16}). {@code backoff-ms} sleeps the agent loop after a failed batch
     * send (defaults {@code 250}; zero allowed).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class RetryDefinition {

        @JsonProperty("max-attempts")
        private Integer maxAttempts;

        @JsonProperty("backoff-ms")
        private Long backoffMs;

        public Integer getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Long getBackoffMs() {
            return backoffMs;
        }

        public void setBackoffMs(Long backoffMs) {
            this.backoffMs = backoffMs;
        }
    }

    /** Validates semantic constraints beyond Jackson deserialization. */
    public static void validate(AgentYamlConfig cfg) {
        Objects.requireNonNull(cfg, "cfg");
        if (cfg.getInputs().isEmpty()) {
            throw new IllegalArgumentException("inputs must define at least one entry.");
        }
        for (InputDefinition def : cfg.getInputs()) {
            validateInput(def);
        }
        OutputDefinition out = cfg.getOutput();
        String clusterName = out.getClusterName() == null ? "" : out.getClusterName().trim();
        if (clusterName.isEmpty()) {
            throw new IllegalArgumentException("output.cluster-name must be non-empty.");
        }
        out.setClusterName(clusterName);
        List<String> hosts = normalizeClusterAddresses(out.getClusterAddresses());
        if (hosts.isEmpty()) {
            throw new IllegalArgumentException(
                    "output.cluster-addresses must list at least one non-empty host/IP.");
        }
        out.setClusterAddresses(hosts);
        if (out.getJobId() == null || out.getJobId() <= 0L) {
            throw new IllegalArgumentException("output.job-id must be a positive integer.");
        }
        if (out.getAuthToken() == null || out.getAuthToken().trim().isEmpty()) {
            throw new IllegalArgumentException("output.auth-token is required.");
        }
        out.setAuthToken(out.getAuthToken().trim());
        if (out.getPort() == null || out.getPort() < 1 || out.getPort() > 65535) {
            throw new IllegalArgumentException(
                    "output.port must be a valid TCP port (1-65535) for EdgeSocket ingress.");
        }
        QueueDefinition q = cfg.getQueue();
        if (q.getSqlitePath() == null || q.getSqlitePath().trim().isEmpty()) {
            throw new IllegalArgumentException("queue.sqlite-path is required.");
        }
        int pollBatch = q.getPollBatchSize() != null ? q.getPollBatchSize() : 128;
        if (pollBatch < 1) {
            throw new IllegalArgumentException("queue.poll-batch-size must be >= 1.");
        }
        BatchDefinition b = cfg.getBatch();
        int bulkMax = b.getBulkMaxSize() != null ? b.getBulkMaxSize() : 256;
        if (bulkMax < 1) {
            throw new IllegalArgumentException("batch.bulk-max-size must be >= 1.");
        }
        long flushMs = b.getFlushIntervalMs() != null ? b.getFlushIntervalMs() : 1000L;
        if (flushMs < 1L) {
            throw new IllegalArgumentException("batch.flush-interval-ms must be >= 1.");
        }
        RetryDefinition r = cfg.getRetry();
        int maxAttempts = r.getMaxAttempts() != null ? r.getMaxAttempts() : 16;
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("retry.max-attempts must be >= 1.");
        }
        long backoff = r.getBackoffMs() != null ? r.getBackoffMs() : 250L;
        if (backoff < 0L) {
            throw new IllegalArgumentException("retry.backoff-ms must be >= 0.");
        }
    }

    private static void validateInput(InputDefinition def) {
        Objects.requireNonNull(def.getType(), "inputs[].type");
        Objects.requireNonNull(def.getId(), "inputs[].id");
        if (def.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("inputs[].id must be non-empty.");
        }
        String t = def.getType().trim().toLowerCase();
        switch (t) {
            case "file":
                if (def.getPaths() == null || def.getPaths().isEmpty()) {
                    throw new IllegalArgumentException(
                            "inputs type=file requires non-empty paths for input id="
                                    + def.getId());
                }
                for (String path : def.getPaths()) {
                    if (path == null || path.trim().isEmpty()) {
                        throw new IllegalArgumentException(
                                "inputs type=file has blank paths item for input id="
                                        + def.getId());
                    }
                }
                break;
            case "log":
                if (def.getPath() == null || def.getPath().trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "inputs type=log requires path for input id=" + def.getId());
                }
                break;
            case "event":
                if (def.getPaths() != null) {
                    for (String path : def.getPaths()) {
                        if (path == null || path.trim().isEmpty()) {
                            throw new IllegalArgumentException(
                                    "inputs type=event has blank paths item for input id="
                                            + def.getId());
                        }
                    }
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown inputs type '" + def.getType() + "' for input id=" + def.getId());
        }
    }

    private static List<String> normalizeClusterAddresses(List<String> raw) {
        List<String> out = new ArrayList<>();
        if (raw == null) {
            return out;
        }
        for (String h : raw) {
            if (h == null) {
                continue;
            }
            String t = h.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }
}
