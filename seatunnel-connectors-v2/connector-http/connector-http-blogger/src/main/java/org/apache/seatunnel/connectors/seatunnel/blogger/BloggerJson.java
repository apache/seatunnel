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

package org.apache.seatunnel.connectors.seatunnel.blogger;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.connectors.seatunnel.blogger.BloggerConfig.Stage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Decodes Google JSON while preserving the original data values and graph associations.
 *
 * <p>Parser exceptions and response messages can contain potential credentials or user data.
 */
final class BloggerJson {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(8 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final Pattern BEARER = Pattern.compile("[A-Za-z0-9._~+/-]+={0,8}");
    private static final Set<String> THROTTLING_REASONS =
            Set.of("rateLimitExceeded", "userRateLimitExceeded", "RATE_LIMIT_EXCEEDED");

    private BloggerJson() {}

    static JsonNode parse(byte[] bytes) throws IOException {
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
        } catch (CharacterCodingException e) {
            throw failure("response is not valid UTF-8 JSON");
        }
        try {
            JsonNode root = JSON.readTree(bytes);
            if (root != null && root.isObject()) {
                return root;
            }
        } catch (IOException | RuntimeException ignored) {
            // Discard the parser's payload-bearing diagnostic.
        }
        throw failure("response is not a valid JSON object");
    }

    /**
     * Validates the active provider envelope before emitting anything or traversing a child.
     *
     * <p>A typed empty list without an items property is valid. A detail object is a single record.
     */
    static Page page(
            JsonNode root, Stage stage, int limit, String blog, String post, boolean discovery)
            throws IOException {
        if (root.has("error") || !stage.kind.equals(root.path("kind").asText(""))) {
            throw failure("unexpected Google response kind or error envelope");
        }
        JsonNode rawNext = root.path("nextPageToken");
        String next = null;
        if (!rawNext.isMissingNode() && !rawNext.isNull()) {
            if (!stage.paginated
                    || !rawNext.isTextual()
                    || !BloggerConfig.validPageToken(rawNext.textValue())) {
                throw failure("invalid nextPageToken for this Blogger response");
            }
            next = rawNext.textValue();
        }

        List<Row> rows;
        MessageDigest fingerprint = sha256();
        update(fingerprint, next == null ? "terminal" : "next:" + next);
        if (stage == Stage.POST_DETAIL) {
            Row detail = row(root, stage, blog, post, false);
            rows = List.of(detail);
            update(fingerprint, detail.json);
        } else {
            JsonNode items = root.path("items");
            if (!items.isMissingNode() && (!items.isArray() || items.size() > limit)) {
                throw failure("invalid or oversized items collection");
            }
            rows = new ArrayList<>(items.size());
            Set<String> parentIds = discovery ? new HashSet<>() : null;
            for (JsonNode item : items) {
                Row value = row(item, stage, blog, post, discovery);
                if (discovery
                        && !parentIds.add(stage == Stage.BLOG_LIST ? value.blog : value.post)) {
                    throw failure("duplicate Blogger parent ID in a discovery page");
                }
                rows.add(value);
                update(fingerprint, discovery ? value.blog + ":" + value.post : value.json);
            }
        }
        return new Page(rows, next, hex(fingerprint.digest()));
    }

    private static Row row(JsonNode value, Stage stage, String blog, String post, boolean discovery)
            throws IOException {
        JsonNode raw = value.path("id");
        if (!value.isObject() || !raw.isTextual() || !BloggerConfig.validId(raw.textValue())) {
            throw failure("invalid record ID in Blogger response");
        }
        String id = raw.textValue();
        if (stage == Stage.BLOG_LIST) {
            blog = id;
        } else {
            verifyParent(value.get("blog"), blog, "blog");
        }
        if (stage == Stage.POST_LIST) {
            post = id;
        } else if (stage == Stage.POST_DETAIL && !id.equals(post)) {
            throw failure("post detail does not match its requested post ID");
        } else if (stage == Stage.COMMENT_LIST) {
            verifyParent(value.get("post"), post, "post");
        }
        return new Row(blog, post, discovery ? null : encode(value));
    }

    private static void verifyParent(JsonNode member, String expected, String field)
            throws IOException {
        if (member != null && !member.isNull()) {
            JsonNode id = member.path("id");
            if (!member.isObject() || !id.isTextual() || !expected.equals(id.textValue())) {
                throw failure("record " + field + " association differs from its requested parent");
            }
        }
    }

    /**
     * Accepts only a usable Bearer token, lifetime and disclosed read grant.
     *
     * <p>Google may omit scope when the grant is unchanged.
     */
    static Access access(JsonNode root) throws IOException {
        JsonNode value = root.path("access_token");
        JsonNode type = root.path("token_type");
        JsonNode life = root.path("expires_in");
        if (root.has("error")
                || !value.isTextual()
                || value.textValue().length() > 8192
                || !BEARER.matcher(value.textValue()).matches()
                || !type.isTextual()
                || !"Bearer".equalsIgnoreCase(type.textValue())
                || !life.isIntegralNumber()
                || !life.canConvertToInt()
                || life.intValue() < 1
                || life.intValue() > 604800) {
            throw failure("invalid OAuth token response");
        }
        JsonNode scope = root.get("scope");
        if (scope != null
                && (!scope.isTextual()
                        || Arrays.stream(scope.textValue().split(" +"))
                                .noneMatch(BloggerConfig.READONLY_SCOPE::equals))) {
            throw failure("OAuth token does not have blogger.readonly scope");
        }
        return new Access(value.textValue(), life.intValue());
    }

    static boolean throttled(JsonNode root) {
        JsonNode error = root.path("error");
        for (String name : List.of("errors", "details")) {
            for (JsonNode entry : error.path(name)) {
                if (THROTTLING_REASONS.contains(entry.path("reason").asText(""))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String encode(JsonNode node) throws IOException {
        try {
            return JSON.writeValueAsString(node);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a Blogger record");
        }
    }

    static String digest(String text) {
        return hex(sha256().digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    private static void update(MessageDigest input, String value) {
        input.update(value.getBytes(StandardCharsets.UTF_8));
        input.update((byte) '\n');
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] & 0xff) >>> 4];
            result[i * 2 + 1] = digits[bytes[i] & 0xf];
        }
        return new String(result);
    }

    static Failure failure(String reason) {
        return new Failure("Blogger: " + reason);
    }

    /**
     * Contains an internally defined, non-payload diagnostic.
     *
     * <p>Only fixed operation details or numeric status codes are exposed.
     */
    static final class Failure extends IOException {
        Failure(String reason) {
            super(reason);
        }
    }

    /**
     * One resource record or parent key, associated with its containing blog and post.
     *
     * <p>Parent discovery creates these without retaining content.
     */
    static final class Row {
        final String blog;
        final String post;
        final String json;

        Row(String blog, String post, String json) {
            this.blog = blog;
            this.post = post;
            this.json = json;
        }
    }

    /**
     * A validated response page and its ordered replay identity.
     *
     * <p>For discovery, the fingerprint includes only identities and the continuation.
     */
    static final class Page {
        final List<Row> rows;
        final String next;
        final String digest;

        Page(List<Row> rows, String next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Transient OAuth credentials and their lifetime.
     *
     * <p>The access token is used only inside the HTTPS client's reader lifetime.
     */
    static final class Access {
        final String token;
        final int lifetimeSeconds;

        Access(String token, int lifetimeSeconds) {
            this.token = token;
            this.lifetimeSeconds = lifetimeSeconds;
        }
    }
}
