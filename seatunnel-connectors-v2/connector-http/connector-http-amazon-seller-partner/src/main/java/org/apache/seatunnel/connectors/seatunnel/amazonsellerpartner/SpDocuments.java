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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Bounded document decoding for generated and requested reports.
 *
 * <p>The decoded byte fingerprint is independent of a renewed URL or a different gzip wrapper.
 * Restore compares it before skipping any previously emitted report lines.
 */
final class SpDocuments {
    private static final Pattern CHARSET =
            Pattern.compile("(?i)(?:^|;)\\s*charset\\s*=\\s*\\\"?([A-Za-z0-9_-]+)");
    private final SpSettings settings;

    SpDocuments(SpSettings settings) {
        this.settings = settings;
    }

    Decoded parse(byte[] wire, String encoding, String media, String documentId) throws SpFailure {
        byte[] bytes = expand(wire, encoding);
        List<ObjectNode> records;
        if (settings.resource.format == SpResource.Format.JSON) {
            records = jsonRows(bytes, media);
        } else {
            String decoded = text(bytes, media);
            records =
                    settings.resource.format == SpResource.Format.TSV
                                    || settings.resource.format == SpResource.Format.FEEDBACK
                            ? new SpDelimited(settings, decoded).rows()
                            : new SpXml(settings).rows(decoded);
        }
        return new Decoded(documentId, digest(bytes), records);
    }

    byte[] expand(byte[] wire, String encoding) throws SpFailure {
        boolean magic = wire.length >= 2 && wire[0] == 0x1f && (wire[1] & 0xff) == 0x8b;
        if (encoding != null && !"GZIP".equals(encoding)) {
            throw invalid("unsupported report compression");
        }
        if (encoding != null && !magic) {
            throw invalid("report advertised gzip but did not contain a gzip document");
        }
        if (!magic) {
            return wire;
        }
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(wire))) {
            byte[] bytes = in.readNBytes(settings.maxResponseBytes + 1);
            if (bytes.length > settings.maxResponseBytes) {
                throw invalid("decompressed report exceeds its byte limit");
            }
            return bytes;
        } catch (SpFailure e) {
            throw e;
        } catch (IOException e) {
            throw invalid("invalid gzip report");
        }
    }

    private List<ObjectNode> jsonRows(byte[] bytes, String media) throws SpFailure {
        String input = text(bytes, media);
        if (input.isBlank()) {
            return List.of();
        }
        if (input.startsWith("\ufeff")) {
            input = input.substring(1);
        }
        JsonNode document =
                SpJson.read(input.getBytes(StandardCharsets.UTF_8), settings.resource.name());
        JsonNode errors = document.path("errors");
        if (!document.isObject()
                || !(errors.isMissingNode()
                        || errors.isNull()
                        || (errors.isArray() && errors.isEmpty()))) {
            throw invalid("invalid JSON report envelope");
        }
        JsonNode selected = document.path(settings.resource.jsonArray);
        if (!selected.isArray() || selected.size() > settings.maxRows) {
            throw invalid("JSON report array is missing, invalid, or over its row limit");
        }
        List<ObjectNode> rows = new ArrayList<>(selected.size());
        for (JsonNode record : selected) {
            if (!record.isObject()) {
                throw invalid("expected object entries in the JSON report array");
            }
            rows.add((ObjectNode) record);
        }
        return rows;
    }

    private String text(byte[] bytes, String media) throws SpFailure {
        try {
            Matcher matcher = CHARSET.matcher(media == null ? "" : media);
            if (matcher.find()) {
                String charset = matcher.group(1).toLowerCase(Locale.ROOT);
                if (!List.of(
                                "utf-8",
                                "utf8",
                                "iso-8859-1",
                                "windows-1252",
                                "utf-16",
                                "utf-16le",
                                "utf-16be")
                        .contains(charset)) {
                    throw invalid("unsupported report charset");
                }
                return decode(bytes, Charset.forName(charset));
            }
            if (bytes.length >= 2
                    && (((bytes[0] & 255) == 254 && (bytes[1] & 255) == 255)
                            || ((bytes[0] & 255) == 255 && (bytes[1] & 255) == 254))) {
                return decode(bytes, StandardCharsets.UTF_16);
            }
            try {
                return decode(bytes, StandardCharsets.UTF_8);
            } catch (CharacterCodingException e) {
                if (settings.resource.format == SpResource.Format.JSON) {
                    throw e;
                }
                // Some flat-file and XML reports are published in a legacy European encoding.
                return decode(bytes, StandardCharsets.ISO_8859_1);
            }
        } catch (CharacterCodingException e) {
            throw invalid("report characters do not match the declared encoding");
        }
    }

    private static String decode(byte[] bytes, Charset charset) throws CharacterCodingException {
        return charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }

    private static String digest(byte[] bytes) {
        try {
            return Base64.getEncoder()
                    .encodeToString(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    private SpFailure invalid(String reason) {
        return new SpFailure(settings.resource.name(), reason);
    }

    /**
     * In-memory rows from one bounded document response.
     *
     * <p>Only its document ID, digest, and current offset are eligible for checkpoint state.
     */
    static final class Decoded {
        final String id;
        final String digest;
        final List<ObjectNode> rows;

        private Decoded(String id, String digest, List<ObjectNode> rows) {
            this.id = id;
            this.digest = digest;
            this.rows = rows;
        }
    }
}
