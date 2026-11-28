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

package org.apache.seatunnel.connectors.seatunnel.astra;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Holds the validated options for one Astra vector writer and its embedding provider. The
 * checkpoint identity binds write scope and document generation without credential material.
 */
final class AstraConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String ASTRA_TOKEN = "Token";
    static final String BEARER = "Authorization";
    static final String AZURE_KEY = "api-key";
    private static final Pattern COLLECTION_NAME = Pattern.compile("[A-Za-z0-9_]{1,48}");
    private static final Pattern TOKEN_HEADER = Pattern.compile("[!-~]{1,4096}");
    private static final Pattern DEPLOYMENT = Pattern.compile("[A-Za-z0-9_.-]{1,128}");
    private static final String ADA = "text-embedding-ada-002";
    static final Option<String> ENDPOINT =
            required("endpoint", "Astra DB Serverless HTTPS origin.");
    static final Option<String> TOKEN = required("token", "Astra DB application token.");
    static final Option<String> KEYSPACE = required("keyspace", "Astra DB Data API keyspace.");
    static final Option<String> COLLECTION = required("collection", "Astra DB vector collection.");
    static final Option<String> MODE =
            setting("mode", "append", "Write mode for the selected scope.");
    static final Option<String> NAMESPACE =
            setting("namespace", "", "Namespace for the document scope.");
    static final Option<String> STREAM = setting("stream", "", "Stream for the document scope.");
    static final Option<String> NAMESPACE_FIELD =
            setting("namespace_field", "", "Row path for a namespace.");
    static final Option<String> STREAM_FIELD =
            setting("stream_field", "", "Row path for a stream.");
    static final Option<List<String>> PRIMARY_KEYS =
            list("primary_keys", "Ordered field paths for record identity.");
    static final Option<List<String>> TEXT_FIELDS =
            list("text_fields", "Row paths included in embedding text.");
    static final Option<List<String>> METADATA_FIELDS =
            list("metadata_fields", "Row paths included as metadata.");
    static final Option<Map<String, String>> FIELD_NAME_MAPPINGS =
            Options.key("field_name_mappings")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Renames selected text and metadata fields.");
    static final Option<Boolean> OMIT_RAW_TEXT =
            Options.key("omit_raw_text")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Omit generated chunk text from stored documents.");
    static final Option<String> SPLITTER =
            setting("splitter", "separator", "Text splitting strategy.");
    static final Option<List<String>> SEPARATORS =
            Options.key("separators")
                    .listType()
                    .defaultValue(List.of("\n\n", "\n", " ", ""))
                    .withDescription("Ordered literal splitter separators.");
    static final Option<Boolean> KEEP_SEPARATOR =
            Options.key("keep_separator")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Keep the literal separator at the start of the next section.");
    static final Option<Integer> MARKDOWN_LEVEL =
            integer("markdown_level", 1, "Maximum split heading level.");
    static final Option<String> CODE_LANGUAGE =
            setting("code_language", "", "Language for code splitting.");
    static final Option<Integer> CHUNK_SIZE =
            integer("chunk_size", 400, "Maximum GPT-2 tokens per chunk.");
    static final Option<Integer> CHUNK_OVERLAP =
            integer("chunk_overlap", 0, "Maximum GPT-2 tokens of repeated context.");
    static final Option<String> TOKENIZER_DATA_FILE =
            setting("tokenizer_data_file", "", "Optional verified GPT-2 rank file.");
    static final Option<String> EMBEDDING_MODE =
            required("embedding_mode", "Embedding provider or local mode.");
    static final Option<String> EMBEDDING_TOKEN =
            setting("embedding_secret_key", "", "Credential for the embedding service.");
    static final Option<String> EMBEDDING_URL =
            setting("embedding_url", "", "HTTPS API base for remote embeddings.");
    static final Option<String> EMBEDDING_MODEL =
            setting("embedding_model", ADA, "Model for OpenAI-compatible service.");
    static final Option<String> EMBEDDING_DEPLOYMENT =
            setting("embedding_deployment", "", "Azure model deployment name.");
    static final Option<String> EMBEDDING_FIELD =
            setting("embedding_field", "", "Row path holding an existing vector.");
    static final Option<Integer> EMBEDDING_DIMENSIONS =
            integer(
                    "embedding_dimensions",
                    0,
                    "Dimensions for existing or compatible embedding vectors.");
    static final Option<Integer> MAX_ROW_BYTES =
            integer("max_row_bytes", 262144, "Maximum serialized input bytes.");
    static final Option<Integer> MAX_BUFFER_BYTES =
            integer("max_buffer_bytes", 4194304, "Estimated pending document budget in bytes.");
    static final Option<Integer> MAX_BODY_BYTES =
            integer("max_body_bytes", 4194304, "Maximum JSON request or response bytes.");
    static final Option<Integer> MAX_CHUNKS_PER_ROW =
            integer("max_chunks_per_row", 100, "Maximum chunks per input row.");
    static final Option<Integer> MAX_DELETE_REQUESTS =
            integer("max_delete_requests", 500, "Maximum requests for one filtered cleanup.");
    static final Option<Integer> CONNECT_TIMEOUT =
            integer("connect_timeout_ms", 10000, "HTTPS connection timeout.");
    static final Option<Integer> READ_TIMEOUT =
            integer("read_timeout_ms", 15000, "HTTPS inactivity timeout.");
    static final Option<Integer> REQUEST_TIMEOUT =
            integer("request_timeout_ms", 30000, "Absolute operation timeout including retries.");
    static final Option<Integer> FLUSH_TIMEOUT =
            integer("flush_timeout_ms", 120000, "Total HTTPS time for one buffered flush.");
    static final Option<Integer> MAX_ATTEMPTS =
            integer("max_attempts", 3, "Maximum attempts for eligible retries.");

    final URI keyspaceUrl;
    final URI documentsUrl;
    final String token;
    final Mode mode;
    final String namespace;
    final String stream;
    final String namespaceField;
    final String streamField;
    final List<String> primaryKeys;
    final List<String> textFields;
    final List<String> metadataFields;
    final Map<String, String> rename;
    final boolean omitText;
    final Splitter splitter;
    final List<String> separators;
    final boolean keepSeparator;
    final int markdownLevel;
    final String codeLanguage;
    final int chunkSize;
    final int overlap;
    final String tokenizerFile;
    final Embedding embedding;
    final URI embeddingUrl;
    final String embeddingToken;
    final String embeddingModel;
    final String embeddingField;
    final int dimensions;
    final int maxRow;
    final int maxBuffer;
    final int maxBody;
    final int maxChunks;
    final int maxDelete;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int flushTimeout;
    final int maxAttempts;
    final String collection;
    final String fingerprint;

    AstraConfig(ReadonlyConfig input, TablePath table) {
        URI astra = endpoint(input.get(ENDPOINT), false, "endpoint");
        token = checkToken(input.get(TOKEN), false, "token");
        String keyspace = name(input.get(KEYSPACE), "keyspace");
        collection = name(input.get(COLLECTION), "collection");
        keyspaceUrl = astra.resolve("/api/json/v1/" + keyspace);
        documentsUrl = astra.resolve("/api/json/v1/" + keyspace + "/" + collection);
        mode = parse(input.get(MODE), Mode::valueOf, "mode");
        namespace =
                scope(
                        input.get(NAMESPACE).isEmpty() && table != null
                                ? coalesce(table.getDatabaseName())
                                : input.get(NAMESPACE),
                        true);
        stream =
                scope(
                        input.get(STREAM).isEmpty() && table != null
                                ? table.getTableName()
                                : input.get(STREAM),
                        false);
        namespaceField = optionalPath(input.get(NAMESPACE_FIELD));
        streamField = optionalPath(input.get(STREAM_FIELD));
        if (mode == Mode.OVERWRITE && (!namespaceField.isEmpty() || !streamField.isEmpty())) {
            throw invalid("overwrite requires a configured static namespace and stream");
        }
        primaryKeys = paths(input.get(PRIMARY_KEYS));
        if (mode == Mode.APPEND_DEDUP && primaryKeys.isEmpty()) {
            throw invalid("append_dedup requires nonempty primary_keys");
        }
        if (primaryKeys.stream().anyMatch(AstraPaths::wildcard)) {
            throw invalid("primary_keys must use literal paths");
        }
        textFields = paths(input.get(TEXT_FIELDS));
        metadataFields = paths(input.get(METADATA_FIELDS));
        Map<String, String> mappings = new TreeMap<>(input.get(FIELD_NAME_MAPPINGS));
        if (mappings.size() > 64
                || new LinkedHashSet<>(mappings.values()).size() != mappings.size()
                || mappings.entrySet().stream()
                        .anyMatch(entry -> !field(entry.getKey()) || !field(entry.getValue()))) {
            throw invalid(
                    "field_name_mappings must rename at most 64 valid fields to distinct destinations");
        }
        rename = Collections.unmodifiableMap(mappings);
        omitText = input.get(OMIT_RAW_TEXT);
        splitter = parse(input.get(SPLITTER), Splitter::valueOf, "splitter");
        separators = List.copyOf(input.get(SEPARATORS));
        if (separators.size() > 16
                || new LinkedHashSet<>(separators).size() != separators.size()
                || separators.stream().anyMatch(s -> s.length() > 64)
                || (separators.contains("") && !separators.get(separators.size() - 1).isEmpty())) {
            throw invalid(
                    "separators must have at most 16 distinct values with an optional trailing empty separator");
        }
        keepSeparator = input.get(KEEP_SEPARATOR);
        markdownLevel = number(input, MARKDOWN_LEVEL, 1, 6);
        codeLanguage = input.get(CODE_LANGUAGE);
        if (splitter == Splitter.CODE && !AstraSplitter.supportedLanguage(codeLanguage)) {
            throw invalid("code_language is required for code splitting");
        }
        chunkSize = number(input, CHUNK_SIZE, 1, 8191);
        overlap = number(input, CHUNK_OVERLAP, 0, chunkSize - 1);
        tokenizerFile = input.get(TOKENIZER_DATA_FILE);
        embedding = parse(input.get(EMBEDDING_MODE), Embedding::valueOf, "embedding_mode");
        embeddingField = optionalPath(input.get(EMBEDDING_FIELD));
        if (embedding == Embedding.FROM_FIELD && embeddingField.isEmpty()) {
            throw invalid("from_field requires embedding_field");
        }
        dimensions =
                embedding.dimension == 0
                        ? number(input, EMBEDDING_DIMENSIONS, 1, 4096)
                        : embedding.dimension;
        if (embedding.dimension != 0
                && input.get(EMBEDDING_DIMENSIONS) != 0
                && input.get(EMBEDDING_DIMENSIONS) != embedding.dimension) {
            throw invalid("embedding dimensions disagree with the selected fixed model");
        }
        embeddingToken =
                embedding.remote
                        ? checkToken(
                                input.get(EMBEDDING_TOKEN),
                                embedding == Embedding.OPENAI_COMPATIBLE,
                                "embedding_secret_key")
                        : "";
        String base = input.get(EMBEDDING_URL);
        switch (embedding) {
            case OPENAI:
                embeddingUrl = provider(base, "api.openai.com").resolve("/v1/embeddings");
                embeddingModel = ADA;
                break;
            case COHERE:
                embeddingUrl = provider(base, "api.cohere.com").resolve("/v2/embed");
                embeddingModel = "embed-english-v3.0";
                break;
            case AZURE_OPENAI:
                embeddingUrl =
                        endpoint(base, false, "embedding_url").resolve("/openai/v1/embeddings");
                embeddingModel = input.get(EMBEDDING_DEPLOYMENT);
                if (!DEPLOYMENT.matcher(embeddingModel).matches()) {
                    throw invalid("azure_openai requires a valid embedding_deployment");
                }
                break;
            case OPENAI_COMPATIBLE:
                URI compatible = endpoint(base, true, "embedding_url");
                String path = compatible.getRawPath().replaceFirst("/+$", "");
                embeddingUrl = compatible.resolve(path + "/embeddings");
                embeddingModel = input.get(EMBEDDING_MODEL);
                if (embeddingModel.isBlank() || embeddingModel.length() > 256) {
                    throw invalid("openai_compatible requires an embedding_model");
                }
                break;
            default:
                embeddingUrl = null;
                embeddingModel = "";
        }
        maxRow = number(input, MAX_ROW_BYTES, 256, 4194304);
        maxBuffer = number(input, MAX_BUFFER_BYTES, 1024, 33554432);
        maxBody = number(input, MAX_BODY_BYTES, 1024, 8388608);
        maxChunks = number(input, MAX_CHUNKS_PER_ROW, 1, 500);
        maxDelete = number(input, MAX_DELETE_REQUESTS, 1, 100000);
        connectTimeout = number(input, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = number(input, READ_TIMEOUT, 1, 300000);
        requestTimeout = number(input, REQUEST_TIMEOUT, 1, 300000);
        flushTimeout = number(input, FLUSH_TIMEOUT, 1, 600000);
        maxAttempts = number(input, MAX_ATTEMPTS, 1, 10);
        fingerprint =
                AstraJson.identity(
                        Arrays.asList(
                                documentsUrl.toASCIIString(),
                                mode.name(),
                                namespace,
                                stream,
                                namespaceField,
                                streamField,
                                primaryKeys,
                                textFields,
                                metadataFields,
                                rename,
                                omitText,
                                splitter.name(),
                                separators,
                                keepSeparator,
                                markdownLevel,
                                codeLanguage,
                                chunkSize,
                                overlap,
                                embedding.name(),
                                embeddingUrl == null ? "" : embeddingUrl.toASCIIString(),
                                embeddingModel,
                                embeddingField,
                                dimensions));
    }

    private static URI provider(String value, String officialHost) {
        URI result =
                endpoint(
                        value.isEmpty() ? "https://" + officialHost : value,
                        false,
                        "embedding_url");
        if (!officialHost.equalsIgnoreCase(result.getHost())) {
            throw invalid("embedding_url must use the selected provider hostname");
        }
        return result;
    }

    private static URI endpoint(String value, boolean allowPath, String option) {
        try {
            URI result = URI.create(value);
            String path = result.getRawPath();
            if ("https".equalsIgnoreCase(result.getScheme())
                    && result.getHost() != null
                    && result.getPort() != 0
                    && result.getPort() <= 65535
                    && result.getRawUserInfo() == null
                    && result.getRawQuery() == null
                    && result.getRawFragment() == null
                    && (path.isEmpty()
                            || "/".equals(path)
                            || allowPath
                                    && path.matches("(/[A-Za-z0-9._~-]+)+/?")
                                    && !path.contains("/../")
                                    && !path.endsWith("/..")
                                    && !path.contains("/./")
                                    && !path.endsWith("/."))) {
                return result;
            }
        } catch (IllegalArgumentException | NullPointerException ignored) {
            // URLs can contain credentials. Only a known option name is reported.
        }
        throw invalid(option + " must be an HTTPS origin or supported API base");
    }

    static String scope(String value, boolean canBeEmpty) {
        if (value == null
                || (!canBeEmpty && value.isBlank())
                || value.length() > 256
                || value.codePoints().anyMatch(Character::isISOControl)) {
            throw invalid("invalid namespace or stream");
        }
        return value;
    }

    private static String name(String value, String option) {
        if (value == null || !COLLECTION_NAME.matcher(value).matches()) {
            throw invalid(option + " must be an Astra DB identifier");
        }
        return value;
    }

    private static String checkToken(String value, boolean canBeEmpty, String name) {
        if (value == null
                || !(canBeEmpty && value.isEmpty() || TOKEN_HEADER.matcher(value).matches())) {
            throw invalid(name + " must be an accepted HTTPS credential value");
        }
        return value;
    }

    private static String optionalPath(String input) {
        if (!input.isEmpty() && (!AstraPaths.valid(input) || AstraPaths.wildcard(input))) {
            throw invalid("routing and embedding fields require literal row paths");
        }
        return input;
    }

    private static List<String> paths(List<String> input) {
        if (input.size() > 64
                || new LinkedHashSet<>(input).size() != input.size()
                || input.stream().anyMatch(path -> !AstraPaths.valid(path))) {
            throw invalid("row selectors must contain at most 64 distinct valid paths");
        }
        return List.copyOf(input);
    }

    static boolean field(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= 256
                && !value.startsWith("$")
                && !"*".equals(value)
                && value.codePoints().noneMatch(Character::isISOControl);
    }

    private static String coalesce(String value) {
        return value == null ? "" : value;
    }

    private static <T> T parse(String value, Function<String, T> parser, String name) {
        try {
            return parser.apply(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException ignored) {
            throw invalid("unsupported " + name);
        }
    }

    private static int number(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int n = config.get(option);
        if (n < min || n > max) {
            throw invalid(option.key() + " must be between " + min + " and " + max);
        }
        return n;
    }

    private static Option<String> required(String name, String description) {
        return Options.key(name).stringType().noDefaultValue().withDescription(description);
    }

    private static Option<String> setting(String name, String defaultValue, String description) {
        return Options.key(name)
                .stringType()
                .defaultValue(defaultValue)
                .withDescription(description);
    }

    private static Option<Integer> integer(String name, int defaultValue, String description) {
        return Options.key(name).intType().defaultValue(defaultValue).withDescription(description);
    }

    private static Option<List<String>> list(String name, String description) {
        return Options.key(name)
                .listType()
                .defaultValue(Collections.emptyList())
                .withDescription(description);
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Astra sink: " + message);
    }

    /**
     * Indicates how acknowledged records replace or preserve documents in the selected scope.
     * Scoped deletion only accompanies overwrite or primary-key deduplication.
     */
    enum Mode {
        APPEND,
        OVERWRITE,
        APPEND_DEDUP
    }

    /**
     * Selects structural boundaries before enforcing the common GPT-2 token budget. All strategies
     * keep Unicode characters whole.
     */
    enum Splitter {
        SEPARATOR,
        MARKDOWN,
        CODE
    }

    /**
     * Defines the expected dimensions and whether a strategy sends text to another service. Custom
     * models and already embedded fields require explicit dimensions.
     */
    enum Embedding {
        OPENAI(1536, true),
        AZURE_OPENAI(1536, true),
        COHERE(1024, true),
        OPENAI_COMPATIBLE(0, true),
        FROM_FIELD(0, false),
        FAKE(1536, false);

        final int dimension;
        final boolean remote;

        Embedding(int dimension, boolean remote) {
            this.dimension = dimension;
            this.remote = remote;
        }
    }
}
