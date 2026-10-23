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

package org.apache.seatunnel.edge.agent.starter.yaml;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class AgentYamlConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private AgentSection agent;

    private ReaderDefinition input;
    private QueueDefinition queue;
    private RetryDefinition retry = new RetryDefinition();
    private OutputDefinition output = new OutputDefinition();

    public void ensureDefaults() {
        if (retry == null) {
            retry = new RetryDefinition();
        }
        if (output == null) {
            output = new OutputDefinition();
        }
        if (queue == null) {
            queue = new QueueDefinition();
        }
        if (agent == null) {
            agent = new AgentSection();
        }
        if (input != null) {
            input.ensureDefaults();
            input.rejectLegacyQueue();
        }
    }

    @Setter
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class AgentSection implements Serializable {

        private static final long serialVersionUID = 1L;

        private String id;

        @JsonProperty("delivery-guarantee")
        private String deliveryGuarantee;

        @JsonProperty("idle-sleep-ms")
        private Long idleSleepMs;

        @JsonProperty("bulk-max-size")
        private Integer bulkMaxSize;

        @JsonProperty("flush-interval-ms")
        private Long flushIntervalMs;
    }

    /**
     * Single logical input source listed under YAML {@code input}.
     *
     * <p>Collectors use non-empty {@code paths} (after normalizing legacy {@code path} for tail
     * logs). Optional {@code type} selects legacy {@code file}/{@code log}/{@code event} semantics
     * when present; omit {@code type} for the flat file-collector layout.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @Setter
    @Getter
    public static final class ReaderDefinition implements Serializable {

        private static final long serialVersionUID = 1L;

        private String id;
        private String type;

        /**
         * {@code file} / {@code event}: YAML {@code paths} list (never blank strings when present).
         */
        private List<String> paths = Collections.emptyList();

        private String path;

        /**
         * {@code log} only: YAML {@code read-from-beginning}; omit/false ⇒ tail-follow from EOF.
         */
        @JsonProperty("read-from-beginning")
        private Boolean readFromBeginning;

        /**
         * Optional nested {@code file} block for {@code type: file} with extended glob, multiline,
         * batch, and poll settings.
         */
        private FileInputDefinition file;

        /** Flattened file-collect keys (same semantics as nested {@code file}). */
        private String encoding;

        @JsonProperty("glob-scan-interval-ms")
        private Long globScanIntervalMs;

        @JsonProperty("close-inactive-ms")
        private Long closeInactiveMs;

        @JsonProperty("on-error")
        private String onError;

        private MultilineDefinition multiline;

        @JsonProperty("output-format")
        private OutputFormatDefinition outputFormat;

        @JsonProperty("queue")
        private QueueDefinition legacyQueue;

        public void ensureDefaults() {
            if (paths == null) {
                paths = Collections.emptyList();
            }
        }

        public void rejectLegacyQueue() {
            if (legacyQueue != null) {
                throw new IllegalArgumentException(
                        "input.queue is no longer supported; configure top-level queue: instead.");
            }
        }

        public void normalizeLegacyPath() {
            if ((paths == null || paths.isEmpty()) && path != null && !path.trim().isEmpty()) {
                paths = Collections.singletonList(path.trim());
            }
        }

        public FileInputDefinition toFileInputDefinition() {
            return FileInputDefinition.fromReader(this);
        }
    }

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class FileInputDefinition implements Serializable {

        private static final long serialVersionUID = 1L;

        private List<String> paths;
        private String encoding;

        @JsonProperty("read-from-beginning")
        private Boolean readFromBeginning;

        @JsonProperty("glob-scan-interval-ms")
        private Long globScanIntervalMs;

        @JsonProperty("close-inactive-ms")
        private Long closeInactiveMs;

        @JsonProperty("on-error")
        private String onError;

        private MultilineDefinition multiline;

        @JsonProperty("output-format")
        private OutputFormatDefinition outputFormat;

        private static FileInputDefinition fromReader(ReaderDefinition reader) {
            FileInputDefinition merged = new FileInputDefinition();
            if (reader.paths != null && !reader.paths.isEmpty()) {
                merged.paths = new ArrayList<>(reader.paths);
            }
            merged.encoding = reader.encoding;
            merged.readFromBeginning = reader.readFromBeginning;
            merged.globScanIntervalMs = reader.globScanIntervalMs;
            merged.closeInactiveMs = reader.closeInactiveMs;
            merged.onError = reader.onError;
            merged.multiline = reader.multiline;
            merged.outputFormat = reader.outputFormat;

            FileInputDefinition nested = reader.file;
            if (nested != null) {
                if (nested.paths != null && !nested.paths.isEmpty()) {
                    merged.paths = new ArrayList<>(nested.paths);
                }
                if (nested.encoding != null) {
                    merged.encoding = nested.encoding;
                }
                if (nested.readFromBeginning != null) {
                    merged.readFromBeginning = nested.readFromBeginning;
                }
                if (nested.globScanIntervalMs != null) {
                    merged.globScanIntervalMs = nested.globScanIntervalMs;
                }
                if (nested.closeInactiveMs != null) {
                    merged.closeInactiveMs = nested.closeInactiveMs;
                }
                if (nested.onError != null) {
                    merged.onError = nested.onError;
                }
                if (nested.multiline != null) {
                    merged.multiline = nested.multiline;
                }
                if (nested.outputFormat != null) {
                    merged.outputFormat = nested.outputFormat;
                }
            }
            return merged;
        }
    }

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class MultilineDefinition implements Serializable {

        private static final long serialVersionUID = 1L;

        private String pattern;
        private String match;

        private Boolean negate;

        @JsonProperty("max-lines")
        private Integer maxLines;
    }

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class OutputFormatDefinition implements Serializable {

        private static final long serialVersionUID = 1L;

        private String type;
    }

    @Setter
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class OutputDefinition implements Serializable {

        private static final long serialVersionUID = 1L;

        private String id;

        private String type;

        private String endpoint;

        private String token;

        @JsonProperty("connect-timeout-ms")
        private Integer connectTimeoutMs;

        @JsonProperty("read-timeout-ms")
        private Integer readTimeoutMs;

        @JsonProperty("auth-type")
        private String authType;

        @JsonProperty("initial-backoff-ms")
        private Long initialBackoffMs;

        @JsonProperty("max-backoff-ms")
        private Long maxBackoffMs;

        @JsonProperty("max-reconnect-cycles")
        private Integer maxReconnectCycles;

        @JsonProperty("max-batch-send-attempts")
        private Integer maxBatchSendAttempts;

        @JsonProperty("packet-mode")
        private String packetMode;

        private String compression;

        private String encryption;

        @JsonProperty("aes-secret-key-base64")
        private String aesSecretKeyBase64;
    }

    /**
     * Local durable outbound queue under top-level {@code queue} (SQLite WAL journal).
     *
     * <p>{@code sqlite-path} is required for persistence; {@code poll-batch-size} drives both input
     * reader {@code poll()} sizing and WAL batch claiming on each agent loop iteration (default
     * {@code 128}).
     */
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class QueueDefinition implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("sqlite-path")
        private String sqlitePath;

        @JsonProperty("poll-batch-size")
        @JsonAlias("poll-batch")
        private Integer pollBatchSize;

        @JsonProperty("acked-retention-ms")
        private Long ackedRetentionMs;

        @JsonProperty("cleanup-batch-size")
        private Integer cleanupBatchSize;

        @JsonProperty("resurrect-batch-size")
        private Integer resurrectBatchSize;

        @JsonProperty("resurrect-interval-ms")
        private Long resurrectIntervalMs;
    }

    /**
     * WAL-backed EdgeSocket send policy after transient failures.
     *
     * <p>{@code max-attempts} bounds SQLite {@code attempts} per row before the starter-owned WAL
     * skips it (defaults {@code 16}). {@code backoff-ms} sleeps the agent loop after a failed batch
     * send (defaults {@code 250}; zero allowed).
     */
    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class RetryDefinition implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("max-attempts")
        private Integer maxAttempts;

        @JsonProperty("backoff-ms")
        private Long backoffMs;

        @JsonProperty("backoff-max-ms")
        private Long backoffMaxMs;
    }
}
