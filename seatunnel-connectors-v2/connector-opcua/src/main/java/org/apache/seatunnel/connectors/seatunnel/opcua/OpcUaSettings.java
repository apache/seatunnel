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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.ALLOW_INSECURE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.ALLOW_LEGACY_SECURITY;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.APPLICATION_URI;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.BROWSE_PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.BROWSE_ROOT;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.CLIENT_KEY_ALIAS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.CLIENT_KEY_STORE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_ARRAY_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_CHUNK_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_CHUNK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_DEPTH;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_MESSAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_NODES;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_RECORDS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_STRING_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MESSAGE_SECURITY_MODE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.NODES;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.NODE_FETCH_MODE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.NODE_FILE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.QUEUE_CAPACITY;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.READ_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.READ_MODE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.REFRESH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SECURITY_POLICY;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SERVER_APPLICATION_URI;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SERVER_CERTIFICATE_SHA256;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SESSION_TIMEOUT_MS;

/** Holds only serializable job settings. SDK objects are created on a worker. */
final class OpcUaSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_NODE_FILE = 1024 * 1024;
    private static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Set<String> POLICIES =
            Set.of("None", "Basic128Rsa15", "Basic256", "Basic256Sha256");

    enum ReadMode {
        POLLING,
        SUBSCRIBE,
        BROWSE_NODES
    }

    enum FetchMode {
        MANUAL,
        FILE,
        BROWSE
    }

    final URI endpoint;
    final ReadMode mode;
    final FetchMode fetch;
    final Map<String, String> nodes;
    final String root;
    final int pageSize;
    final int maxNodes;
    final int maxDepth;
    final int interval;
    final int refreshInterval;
    final int batchSize;
    final int queueCapacity;
    final long maxRecords;
    final int requestTimeout;
    final int sessionTimeout;
    final String policy;
    final String messageMode;
    final String applicationUri;
    final String serverUri;
    final String fingerprint;
    final String keyStore;
    final String keyAlias;
    final String password;
    final int maxChunkSize;
    final int maxChunkCount;
    final int maxMessageSize;
    final int maxArrayLength;
    final int maxStringBytes;
    final String stateKey;

    OpcUaSettings(ReadonlyConfig config) {
        endpoint = checkedEndpoint(config.get(ENDPOINT));
        mode = parseMode(ReadMode.class, config.get(READ_MODE), "read_mode");
        FetchMode selection =
                parseMode(FetchMode.class, config.get(NODE_FETCH_MODE), "node_fetch_mode");
        fetch = mode == ReadMode.BROWSE_NODES ? FetchMode.BROWSE : selection;
        root = config.get(BROWSE_ROOT);
        pageSize = checked(config.get(BROWSE_PAGE_SIZE), 1, 1000, "browse_page_size");
        maxNodes = checked(config.get(MAX_NODES), 1, 8192, "max_nodes");
        maxDepth = checked(config.get(MAX_DEPTH), 1, 64, "max_depth");
        interval = checked(config.get(INTERVAL_MS), 1, 3600000, "interval_ms");
        refreshInterval =
                checked(config.get(REFRESH_INTERVAL_MS), 1, 86400000, "refresh_interval_ms");
        batchSize = checked(config.get(READ_BATCH_SIZE), 1, 1000, "read_batch_size");
        queueCapacity = checked(config.get(QUEUE_CAPACITY), 1, 8192, "queue_capacity");
        maxRecords = config.get(MAX_RECORDS);
        if (maxRecords < 0L || (mode == ReadMode.BROWSE_NODES && maxRecords != 0L)) {
            throw invalid("max_records applies only to value modes and must be nonnegative");
        }
        requestTimeout = checked(config.get(REQUEST_TIMEOUT_MS), 100, 300000, "request_timeout_ms");
        sessionTimeout =
                checked(
                        config.get(SESSION_TIMEOUT_MS),
                        requestTimeout,
                        900000,
                        "session_timeout_ms");
        maxChunkSize = checked(config.get(MAX_CHUNK_SIZE), 8196, 1048576, "max_chunk_size");
        maxChunkCount = checked(config.get(MAX_CHUNK_COUNT), 1, 256, "max_chunk_count");
        maxMessageSize =
                checked(config.get(MAX_MESSAGE_SIZE), maxChunkSize, 8388608, "max_message_size");
        maxArrayLength = checked(config.get(MAX_ARRAY_LENGTH), 1, 65536, "max_array_length");
        maxStringBytes = checked(config.get(MAX_STRING_BYTES), 1, 1048576, "max_string_bytes");

        policy = config.get(SECURITY_POLICY);
        if (!POLICIES.contains(policy)) {
            throw invalid("unsupported security_policy");
        }
        boolean noSecurity = "None".equals(policy);
        messageMode =
                config.getOptional(MESSAGE_SECURITY_MODE)
                        .orElse(noSecurity ? "None" : "SignAndEncrypt");
        if (noSecurity && (!config.get(ALLOW_INSECURE) || !"None".equals(messageMode))) {
            throw invalid("None requires allow_insecure and message_security_mode None");
        }
        if (!noSecurity && !"Sign".equals(messageMode) && !"SignAndEncrypt".equals(messageMode)) {
            throw invalid("secured policies require Sign or SignAndEncrypt");
        }
        if (("Basic128Rsa15".equals(policy) || "Basic256".equals(policy))
                && !config.get(ALLOW_LEGACY_SECURITY)) {
            throw invalid("legacy policy requires allow_legacy_security");
        }
        applicationUri = checkedApplicationUri(config.get(APPLICATION_URI), "application_uri");
        serverUri = config.getOptional(SERVER_APPLICATION_URI).orElse(null);
        fingerprint =
                config.getOptional(SERVER_CERTIFICATE_SHA256)
                        .map(s -> s.replace(":", "").toLowerCase(Locale.ROOT))
                        .orElse(null);
        keyStore = config.getOptional(CLIENT_KEY_STORE).orElse(null);
        keyAlias = config.get(CLIENT_KEY_ALIAS);
        password = config.getOptional(PASSWORD).orElse(null);
        if (!noSecurity) {
            checkedApplicationUri(serverUri, "server_application_uri");
            if (!present(keyStore)
                    || !present(keyAlias)
                    || !present(password)
                    || fingerprint == null
                    || !fingerprint.matches("[0-9a-f]{64}")) {
                throw invalid(
                        "secured policies require client_key_store, client_key_alias, password and server_certificate_sha256");
            }
        } else if (serverUri != null
                || fingerprint != null
                || keyStore != null
                || password != null) {
            throw invalid(
                    "server certificate and client key options apply only to secured policies");
        }

        Map<String, String> configNodes = config.getOptional(NODES).orElse(null);
        String nodeFile = config.getOptional(NODE_FILE).orElse(null);
        Map<String, String> selected;
        switch (fetch) {
            case MANUAL:
                if (configNodes == null || nodeFile != null) {
                    throw invalid("MANUAL requires nodes and excludes node_file");
                }
                selected = configNodes;
                break;
            case FILE:
                if (!present(nodeFile) || configNodes != null) {
                    throw invalid("FILE requires node_file and excludes nodes");
                }
                selected = readNodeFile(nodeFile);
                break;
            default:
                if (configNodes != null || nodeFile != null) {
                    throw invalid("browse selection excludes nodes and node_file");
                }
                OpcUaNodeIds.parse(root);
                selected = Collections.emptyMap();
                break;
        }
        if (fetch != FetchMode.BROWSE && (selected.isEmpty() || selected.size() > maxNodes)) {
            throw invalid("node selection must contain between 1 and max_nodes entries");
        }
        Map<String, String> sorted = new TreeMap<>();
        selected.forEach(
                (alias, id) -> {
                    if (!present(alias) || alias.length() > 256 || alias.indexOf('\0') != -1) {
                        throw invalid("node aliases must contain 1 to 256 characters");
                    }
                    OpcUaNodeIds.parse(id);
                    sorted.put(alias, id);
                });
        nodes = Collections.unmodifiableMap(sorted);
        stateKey =
                JSON.valueToTree(
                                Arrays.asList(
                                        endpoint.toASCIIString(),
                                        mode.name(),
                                        fetch.name(),
                                        nodes,
                                        root,
                                        policy,
                                        messageMode,
                                        applicationUri,
                                        serverUri,
                                        fingerprint,
                                        maxRecords,
                                        maxNodes,
                                        pageSize,
                                        maxDepth))
                        .toString();
    }

    boolean bounded() {
        return mode == ReadMode.BROWSE_NODES || maxRecords > 0;
    }

    private static Map<String, String> readNodeFile(String file) {
        try {
            Path path = Path.of(file);
            if (!Files.isRegularFile(path)) {
                throw invalid("node_file must be a regular file");
            }
            try (InputStream stream = Files.newInputStream(path)) {
                byte[] bytes = stream.readNBytes(MAX_NODE_FILE + 1);
                if (bytes.length > MAX_NODE_FILE) {
                    throw invalid("node_file exceeds 1 MiB");
                }
                JsonNode values = JSON.readTree(bytes);
                if (values == null || !values.isObject()) {
                    throw invalid("node_file must contain a JSON object");
                }
                Map<String, String> result = new TreeMap<>();
                values.fields()
                        .forEachRemaining(
                                entry -> {
                                    if (!entry.getValue().isTextual()) {
                                        throw invalid(
                                                "node_file values must contain textual NodeIds");
                                    }
                                    result.put(entry.getKey(), entry.getValue().textValue());
                                });
                return result;
            }
        } catch (IOException | SecurityException e) {
            throw invalid("could not read node_file as JSON");
        } catch (InvalidPathException e) {
            throw invalid("node_file path is invalid");
        }
    }

    private static URI checkedEndpoint(String text) {
        try {
            URI uri = URI.create(text);
            if ("opc.tcp".equals(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getPort() > 0
                    && uri.getPort() <= 65535
                    && uri.getUserInfo() == null
                    && uri.getQuery() == null
                    && uri.getFragment() == null
                    && text.length() <= 2048) {
                return uri;
            }
        } catch (IllegalArgumentException ignored) {
            // A URI parser error can include sensitive input.
        }
        throw invalid(
                "endpoint must be an opc.tcp URL with an explicit port and no credentials, query or fragment");
    }

    private static String checkedApplicationUri(String text, String name) {
        if (present(text) && text.length() <= 2048) {
            try {
                if (URI.create(text).isAbsolute()) {
                    return text;
                }
            } catch (IllegalArgumentException ignored) {
                // The option name is enough for the diagnostic.
            }
        }
        throw invalid(name + " must be an absolute application URI");
    }

    private static boolean present(String value) {
        return value != null && !value.isBlank();
    }

    private static int checked(int value, int min, int max, String key) {
        if (value < min || value > max) {
            throw invalid(key + " must be between " + min + " and " + max);
        }
        return value;
    }

    private static <M extends Enum<M>> M parseMode(Class<M> type, String value, String key) {
        try {
            return Enum.valueOf(type, value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw invalid(key + " has an unsupported value");
        }
    }

    static IllegalArgumentException invalid(String text) {
        return new IllegalArgumentException("OpcUa Source " + text);
    }
}
