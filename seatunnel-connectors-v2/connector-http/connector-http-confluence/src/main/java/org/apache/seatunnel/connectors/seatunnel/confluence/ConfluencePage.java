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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.http.Header;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

final class ConfluencePage {
    private static final int MAX_JSON_TOKENS = 200000;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .maxStringLength(8388608)
                                                    .maxNumberLength(256)
                                                    .build())
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    final List<Row> rows;
    final String next;

    private ConfluencePage(List<Row> rows, String next) {
        this.rows = rows;
        this.next = next;
    }

    static ConfluencePage decode(
            ConfluenceConfig config, byte[] bytes, Header[] links, String current) {
        JsonNode envelope;
        try (JsonParser parser = JSON.getFactory().createParser(bytes)) {
            int tokens = 0;
            while (parser.nextToken() != null) {
                if (++tokens > MAX_JSON_TOKENS) {
                    throw config.failure("response JSON token limit exceeded");
                }
            }
            envelope = JSON.readTree(bytes);
        } catch (IOException e) {
            throw config.failure("malformed JSON response (body withheld)");
        }
        if (envelope == null || !envelope.isObject()) {
            throw config.failure("response must be a JSON object");
        }
        JsonNode results = envelope.get("results");
        if (results == null || !results.isArray() || results.size() > config.pageSize) {
            throw config.failure("response must have a bounded results array");
        }
        String next =
                new ConfluencePagination(config).next(envelope, links, current, results.size());
        List<Row> rows = new ArrayList<>(results.size());
        byte[] prefix = new byte[0];
        for (JsonNode object : results) {
            if (!object.isObject()) {
                throw config.failure("results contain a non-object record");
            }
            String id = id(config, object.path("id"));
            Long created = auditDate(config, object.path("creationDate"));
            MessageDigest digest = ConfluenceConfig.newDigest();
            digest.update(prefix);
            prefix = digest.digest(canonical(object).toString().getBytes(StandardCharsets.UTF_8));
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {config.resource.name, id, created, object.toString()});
            rows.add(new Row(row, Base64.getEncoder().encodeToString(prefix)));
        }
        return new ConfluencePage(rows, next);
    }

    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            Map<String, JsonNode> fields = new TreeMap<>();
            node.fields().forEachRemaining(entry -> fields.put(entry.getKey(), entry.getValue()));
            ObjectNode sorted = JSON.createObjectNode();
            fields.forEach((key, value) -> sorted.set(key, canonical(value)));
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode sorted = JSON.createArrayNode();
            node.forEach(value -> sorted.add(canonical(value)));
            return sorted;
        }
        return node;
    }

    private static String id(ConfluenceConfig config, JsonNode id) {
        if (id.isNull() || id.isMissingNode()) {
            return null;
        }
        if (!id.isTextual() && !id.isIntegralNumber()) {
            throw config.failure("record id must be a string or integer");
        }
        return id.asText();
    }

    private static Long auditDate(ConfluenceConfig config, JsonNode date) {
        if (config.resource != ConfluenceConfig.Resource.AUDIT
                || date.isNull()
                || date.isMissingNode()) {
            return null;
        }
        if (!date.isIntegralNumber() || !date.canConvertToLong()) {
            throw config.failure("audit creationDate must be a 64-bit integer");
        }
        return date.longValue();
    }

    static final class Row {
        final SeaTunnelRow value;
        final String prefix;

        private Row(SeaTunnelRow value, String prefix) {
            this.value = value;
            this.prefix = prefix;
        }
    }
}
