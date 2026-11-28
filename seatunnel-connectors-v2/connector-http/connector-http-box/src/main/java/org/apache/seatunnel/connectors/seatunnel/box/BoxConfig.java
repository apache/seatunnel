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
package org.apache.seatunnel.connectors.seatunnel.box;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Validated Box routes, credentials, extraction options and checkpoint identity.
 *
 * <p>Read positions retain a one-way scope digest. Authentication credentials, file content and
 * extraction prompts are not placed in source split state.
 */
final class BoxConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Set<String> FIELD_TEXT =
            Set.of("key", "description", "displayName", "display_name", "prompt", "type");

    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Box outputs: " + BoxResource.names());
    static final Option<String> AUTH_MODE =
            text(
                    "auth_mode",
                    "bearer",
                    "Use a supplied bearer token or obtain one via client_credentials.");
    static final Option<String> TOKEN = text("token", null, "Box API bearer token.");
    static final Option<String> CLIENT_ID =
            text("client_id", null, "Box client credentials app ID.");
    static final Option<String> SECRET_KEY =
            text("secret_key", null, "Box client secret sent only to the token grant endpoint.");
    static final Option<String> SUBJECT_TYPE =
            text(
                    "box_subject_type",
                    null,
                    "Box client credentials subject type: user or enterprise.");
    static final Option<String> SUBJECT_ID =
            text("box_subject_id", null, "Decimal Box ID for the chosen subject.");
    static final Option<String> ENDPOINT = text("endpoint", null, "Custom HTTPS Box API origin.");
    static final Option<String> DOWNLOAD_ENDPOINT =
            text(
                    "download_endpoint",
                    null,
                    "Custom HTTPS content origin permitted with a custom API origin and trust.");
    static final Option<String> TRUST_CERTIFICATE =
            text("trust_certificate", null, "Worker-local PEM trust anchor for custom origins.");
    static final Option<Integer> PAGE_SIZE = number("page_size", 100, "Requested list limit.");
    static final Option<Integer> MAX_PAGES =
            number(
                    "max_pages_per_parent",
                    1000,
                    "Maximum successful pages per resource and parent.");
    static final Option<Integer> MAX_REQUESTS =
            number(
                    "max_requests",
                    50000,
                    "Maximum logical API or content operations for the scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            number("max_attempts", 4, "Total attempts per safely retryable HTTPS call.");
    static final Option<Integer> TIMEOUT =
            number(
                    "request_timeout_ms",
                    30000,
                    "Absolute acquisition deadline in milliseconds, including retries and polling.");
    static final Option<Integer> MAX_BYTES =
            number("max_response_bytes", 8388608, "Maximum decoded bytes per successful response.");
    static final Option<String> EVENT_POSITION =
            text("events_start_position", "0", "Initial provider stream position for user events.");
    static final Option<String> ADMIN_POSITION =
            text(
                    "admin_start_position",
                    "0",
                    "Initial provider stream position for enterprise event history.");
    static final Option<String> ADMIN_AFTER =
            text(
                    "admin_created_after",
                    null,
                    "Optional RFC 3339 lower bound for enterprise event history.");
    static final Option<String> ADMIN_BEFORE =
            text(
                    "admin_created_before",
                    null,
                    "Optional RFC 3339 upper bound for enterprise event history.");
    static final Option<String> FOLDER_ID =
            text("folder_id", "0", "Root folder for file extraction.");
    static final Option<Boolean> RECURSIVE =
            Options.key("recursive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Visit descendant folders for selected extraction.");
    static final Option<Integer> MAX_DEPTH =
            number("max_depth", 32, "Maximum descendant depth with the configured folder at zero.");
    static final Option<Integer> MAX_ITEMS =
            number("max_items", 10000, "Maximum raw folder entries read while discovering files.");
    static final Option<Integer> MAX_POLLS =
            number(
                    "max_representation_polls",
                    8,
                    "Maximum status polls for an extracted text representation.");
    static final Option<String> ASK_PROMPT =
            text("ask_prompt", null, "Single-file Box AI question.");
    static final Option<String> EXTRACT_PROMPT =
            text("extract_prompt", null, "Single-file freeform Box AI extraction prompt.");
    static final Option<String> STRUCTURED_FIELDS =
            text("structured_fields", null, "JSON list of Box AI extraction field definitions.");
    static final Option<List<String>> FILE_FIELDS =
            Options.key("file_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional extraction file detail fields, including metadata or permissions.");

    final Set<BoxResource> selected;
    final boolean clientCredentials;
    final String token;
    final String clientId;
    final String secretKey;
    final String subjectType;
    final String subjectId;
    final String origin;
    final String downloadOrigin;
    final String trustCertificate;
    final int pageSize;
    final int maxPages;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String eventPosition;
    final String adminPosition;
    final String adminAfter;
    final String adminBefore;
    final String folderId;
    final boolean recursive;
    final int maxDepth;
    final int maxItems;
    final int maxPolls;
    final String ask;
    final String extract;
    final String structured;
    final String fileFields;
    final String scope;

    BoxConfig(ReadonlyConfig options) {
        EnumSet<BoxResource> resources = readResources(options.get(RESOURCES));
        selected = Set.copyOf(resources);
        String mode = options.get(AUTH_MODE);
        if (!"bearer".equals(mode) && !"client_credentials".equals(mode)) {
            throw invalid("auth_mode", "expected bearer or client_credentials");
        }
        clientCredentials = "client_credentials".equals(mode);
        token = clientCredentials ? null : requiredHeader(options.get(TOKEN), TOKEN);
        clientId = clientCredentials ? requiredHeader(options.get(CLIENT_ID), CLIENT_ID) : null;
        secretKey = clientCredentials ? requiredHeader(options.get(SECRET_KEY), SECRET_KEY) : null;
        subjectType = clientCredentials ? options.get(SUBJECT_TYPE) : null;
        if (clientCredentials && !"user".equals(subjectType) && !"enterprise".equals(subjectType)) {
            throw invalid("box_subject_type", "expected user or enterprise");
        }
        subjectId = clientCredentials ? decimalId(options.get(SUBJECT_ID), SUBJECT_ID) : null;

        String custom = options.get(ENDPOINT);
        String download = options.get(DOWNLOAD_ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((trustCertificate == null) != (custom == null)
                || (trustCertificate != null && trustCertificate.isBlank())
                || (download != null && custom == null)) {
            throw invalid("endpoint", "custom API and content origins require trust_certificate");
        }
        origin = custom == null ? "https://api.box.com" : httpsOrigin(custom, ENDPOINT);
        downloadOrigin = download == null ? null : httpsOrigin(download, DOWNLOAD_ENDPOINT);
        pageSize = range(options, PAGE_SIZE, 1, 500);
        maxPages = range(options, MAX_PAGES, 1, 10000);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);

        eventPosition = position(options.get(EVENT_POSITION), EVENT_POSITION);
        adminPosition = position(options.get(ADMIN_POSITION), ADMIN_POSITION);
        Instant after = date(options.get(ADMIN_AFTER), ADMIN_AFTER);
        Instant before = date(options.get(ADMIN_BEFORE), ADMIN_BEFORE);
        if (after != null && before != null && !after.isBefore(before)) {
            throw invalid("admin_created_after", "must be less than admin_created_before");
        }
        adminAfter = after == null ? null : after.toString();
        adminBefore = before == null ? null : before.toString();

        folderId = decimalId(options.get(FOLDER_ID), FOLDER_ID);
        recursive = options.get(RECURSIVE);
        maxDepth = range(options, MAX_DEPTH, 0, 64);
        maxItems = range(options, MAX_ITEMS, 1, 100000);
        maxPolls = range(options, MAX_POLLS, 1, 100);
        ask = prompt(options.get(ASK_PROMPT), ASK_PROMPT, includes(BoxResource.AI_ASK));
        extract =
                prompt(
                        options.get(EXTRACT_PROMPT),
                        EXTRACT_PROMPT,
                        includes(BoxResource.AI_EXTRACT));
        structured =
                includes(BoxResource.AI_EXTRACT_STRUCTURED)
                        ? structuredFields(options.get(STRUCTURED_FIELDS))
                        : null;
        fileFields = fileFields(options.get(FILE_FIELDS));

        scope =
                digest(
                        String.join(
                                "\n",
                                resources.toString(),
                                mode,
                                optional(token),
                                optional(clientId),
                                optional(secretKey),
                                optional(subjectType),
                                optional(subjectId),
                                origin,
                                optional(downloadOrigin),
                                optional(trustCertificate),
                                Integer.toString(pageSize),
                                eventPosition,
                                adminPosition,
                                optional(adminAfter),
                                optional(adminBefore),
                                folderId,
                                Boolean.toString(recursive),
                                optional(ask),
                                optional(extract),
                                optional(structured),
                                optional(fileFields)));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(RESOURCES)
                .optional(
                        AUTH_MODE,
                        TOKEN,
                        CLIENT_ID,
                        SECRET_KEY,
                        SUBJECT_TYPE,
                        SUBJECT_ID,
                        ENDPOINT,
                        DOWNLOAD_ENDPOINT,
                        TRUST_CERTIFICATE,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        EVENT_POSITION,
                        ADMIN_POSITION,
                        ADMIN_AFTER,
                        ADMIN_BEFORE,
                        FOLDER_ID,
                        RECURSIVE,
                        MAX_DEPTH,
                        MAX_ITEMS,
                        MAX_POLLS,
                        ASK_PROMPT,
                        EXTRACT_PROMPT,
                        STRUCTURED_FIELDS,
                        FILE_FIELDS)
                .build();
    }

    boolean includes(BoxResource resource) {
        return selected.contains(resource);
    }

    private static EnumSet<BoxResource> readResources(List<String> input) {
        if (input == null || input.isEmpty() || input.size() > BoxResource.values().length) {
            throw invalid("resources", "select at least one Box resource");
        }
        EnumSet<BoxResource> resources = EnumSet.noneOf(BoxResource.class);
        for (String name : input) {
            if (!resources.add(BoxResource.from(name))) {
                throw invalid("resources", "select each resource only once");
            }
        }
        return resources;
    }

    private static String requiredHeader(String value, Option<String> option) {
        if (value == null
                || value.isEmpty()
                || value.length() > 2048
                || value.chars().anyMatch(c -> c < 33 || c > 126)) {
            throw invalid(
                    option.key(), "expected a nonempty value without spaces or control characters");
        }
        return value;
    }

    private static String decimalId(String value, Option<String> option) {
        if (value == null || !value.matches("[0-9]{1,64}")) {
            throw invalid(option.key(), "expected a decimal Box ID");
        }
        return value;
    }

    private static String position(String value, Option<String> option) {
        if (value == null || !value.matches("(?:[0-9]{1,128}|now)")) {
            throw invalid(option.key(), "expected a decimal stream position or now");
        }
        return value;
    }

    private static Instant date(String value, Option<String> option) {
        if (value != null) {
            try {
                if (value.length() <= 40 && value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T.+")) {
                    return OffsetDateTime.parse(value).toInstant();
                }
            } catch (DateTimeException ignored) {
                // User-provided values are excluded from diagnostics.
            }
            throw invalid(option.key(), "expected RFC 3339 with an offset");
        }
        return null;
    }

    private static String prompt(String value, Option<String> option, boolean needed) {
        if (!needed) {
            return null;
        }
        if (value == null || value.isBlank() || value.codePointCount(0, value.length()) > 10000) {
            throw invalid(option.key(), "expected a nonempty prompt up to 10000 characters");
        }
        return value;
    }

    private static String fileFields(List<String> fields) {
        if (fields == null) {
            return null;
        }
        Set<String> output = new LinkedHashSet<>(List.of("id", "type", "name"));
        if (fields.size() > 128) {
            throw invalid("file_fields", "too many field selectors");
        }
        for (String selector : fields) {
            if (selector == null || !selector.matches("[A-Za-z][A-Za-z0-9_.-]{0,127}")) {
                throw invalid("file_fields", "expected individual Box field selectors");
            }
            output.add(selector);
        }
        return String.join(",", output);
    }

    private static String structuredFields(String value) {
        if (value == null || value.length() > 65536) {
            throw invalid("structured_fields", "expected a bounded JSON field array");
        }
        try {
            JsonNode input =
                    BoxJson.read(value.getBytes(StandardCharsets.UTF_8), "structured_fields");
            return BoxJson.write(normalizeFields(input, 0), "structured_fields");
        } catch (BoxReadException failure) {
            throw invalid("structured_fields", "expected valid JSON field definitions");
        }
    }

    /**
     * Rebuilds each field with its own options list and optional nested fields.
     *
     * <p>Aliases are normalized to the wire field at this boundary, so unrelated fields never
     * inherit an option from one another.
     */
    private static ArrayNode normalizeFields(JsonNode value, int depth) {
        if (value == null
                || !value.isArray()
                || value.isEmpty()
                || value.size() > 100
                || depth > 4) {
            throw invalid(
                    "structured_fields",
                    "expected 1 to 100 fields per level and at most four nested levels");
        }
        Set<String> keys = new HashSet<>();
        ArrayNode result = BoxJson.MAPPER.createArrayNode();
        for (JsonNode field : value) {
            JsonNode key = field.path("key");
            if (!field.isObject()
                    || !key.isTextual()
                    || key.textValue().isBlank()
                    || key.textValue().length() > 256
                    || !keys.add(key.textValue())) {
                throw invalid(
                        "structured_fields", "field keys must be nonempty and unique in each list");
            }
            ObjectNode output = result.addObject();
            Iterator<String> names = field.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                JsonNode property = field.path(name);
                if (FIELD_TEXT.contains(name)
                        && property.isTextual()
                        && property.textValue().length() <= 10000) {
                    String wire = "display_name".equals(name) ? "displayName" : name;
                    if (output.has(wire)) {
                        throw invalid("structured_fields", "duplicate display name option");
                    }
                    output.set(wire, property);
                } else if ("options".equals(name)) {
                    output.set(name, normalizeOptions(property));
                } else if ("fields".equals(name)) {
                    output.set(name, normalizeFields(property, depth + 1));
                } else {
                    throw invalid("structured_fields", "unsupported field property or value");
                }
            }
        }
        return result;
    }

    private static ArrayNode normalizeOptions(JsonNode value) {
        if (!value.isArray() || value.size() > 100) {
            throw invalid("structured_fields", "expected at most 100 options per field");
        }
        Set<String> keys = new HashSet<>();
        ArrayNode result = BoxJson.MAPPER.createArrayNode();
        for (JsonNode option : value) {
            JsonNode key = option.path("key");
            if (!option.isObject()
                    || option.size() != 1
                    || !key.isTextual()
                    || key.textValue().isBlank()
                    || key.textValue().length() > 256
                    || !keys.add(key.textValue())) {
                throw invalid(
                        "structured_fields", "option keys must be nonempty and unique per field");
            }
            result.addObject().set("key", key);
        }
        return result;
    }

    private static String httpsOrigin(String value, Option<String> option) {
        try {
            URI uri = URI.create(value);
            int port = uri.getPort();
            if ("https".equals(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getUserInfo() == null
                    && uri.getQuery() == null
                    && uri.getFragment() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (port == -1 || (port > 0 && port <= 65535))) {
                String host = uri.getHost().toLowerCase(Locale.ROOT);
                return "https://" + host + (port == -1 || port == 443 ? "" : ":" + port);
            }
        } catch (IllegalArgumentException ignored) {
            // An invalid origin may contain user credentials.
        }
        throw invalid(option.key(), "expected an HTTPS origin without path, query or credentials");
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value between " + min + " and " + max);
        }
        return value;
    }

    private static Option<String> text(String key, String defaultValue, String description) {
        return defaultValue == null
                ? Options.key(key).stringType().noDefaultValue().withDescription(description)
                : Options.key(key)
                        .stringType()
                        .defaultValue(defaultValue)
                        .withDescription(description);
    }

    private static Option<Integer> number(String key, int defaultValue, String description) {
        return Options.key(key).intType().defaultValue(defaultValue).withDescription(description);
    }

    private static String optional(String value) {
        return value == null ? "" : digest(value);
    }

    static String digest(String value) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String option, String reason) {
        return new IllegalArgumentException("Box option " + option + ": " + reason);
    }
}
