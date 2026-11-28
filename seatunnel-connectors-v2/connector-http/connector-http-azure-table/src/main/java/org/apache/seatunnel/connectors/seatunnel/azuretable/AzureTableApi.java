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
package org.apache.seatunnel.connectors.seatunnel.azuretable;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the two Azure Table REST queries and validates their response envelopes.
 *
 * <p>Only typed provider continuation headers can advance a scan. Links in the payload stay as data
 * and cannot affect the next signed target.
 */
final class AzureTableApi {
    static final String DISCOVERY = "table discovery";
    private static final String TYPE_SUFFIX = "@odata.type";
    private static final Set<String> NON_FINITE = Set.of("NaN", "Infinity", "-Infinity");

    private final AzureTableConfig config;
    private final AzureTableHttp http;

    AzureTableApi(AzureTableConfig config, AzureTableHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Lists one page of table names through the /Tables collection.
     *
     * <p>Azure can return zero tables with an opaque continuation token.
     */
    DiscoveryPage discover(Cursor cursor, long deadline) throws InterruptedException {
        String query = "?$top=" + config.pageSize;
        if (cursor != null) {
            query += "&NextTableName=" + encoded(cursor.first);
        }
        AzureTableHttp.Reply reply = http.request(DISCOVERY, "/Tables", query, deadline);
        if (reply.nextPartition != null || reply.nextRow != null) {
            throw new Failure(DISCOVERY, "unexpected entity continuation headers");
        }
        JsonNode value = values(DISCOVERY, reply);
        List<String> names = new ArrayList<>(value.size());
        for (JsonNode row : value) {
            String name = row.isObject() ? text(row.get("TableName")) : null;
            if (!AzureTableConfig.validTable(name)) {
                throw new Failure(DISCOVERY, "invalid table name in discovery response");
            }
            names.add(name);
        }
        return new DiscoveryPage(
                names, reply.nextTable == null ? null : new Cursor(reply.nextTable, null));
    }

    /**
     * Restores the original entity query and adds only the service's continuation tuple.
     *
     * <p>Configured partition and row values are OData literals. Continuation values are opaque
     * protocol strings and receive only URI encoding.
     */
    EntityPage entities(String table, Cursor cursor, long deadline) throws InterruptedException {
        String path = "/" + table + "()";
        AzureTableHttp.Reply reply =
                http.request(table, path, entityQuery(table, cursor), deadline);
        if (reply.nextTable != null || reply.nextPartition == null && reply.nextRow != null) {
            throw new Failure(table, "inconsistent entity continuation headers");
        }
        Cursor next =
                reply.nextPartition == null ? null : new Cursor(reply.nextPartition, reply.nextRow);
        JsonNode value = values(table, reply);
        List<Entity> entities = new ArrayList<>(value.size());
        Set<String> identities = new HashSet<>();
        for (JsonNode row : value) {
            Entity entity = entity(table, row);
            if (!identities.add(entity.identity)) {
                throw new Failure(table, "duplicate entity keys in one page");
            }
            entities.add(entity);
        }
        String continuation = next == null ? "terminal" : next.fingerprint();
        return new EntityPage(
                entities, next, AzureTableConfig.hash(value.toString() + "\n" + continuation));
    }

    String entityQuery(String table, Cursor cursor) {
        String query = "?$top=" + config.pageSize;
        AzureTableConfig.Key start = config.boundaries.get(AzureTableConfig.normalized(table));
        if (start != null) {
            String partition = literal(start.partition);
            String row = literal(start.row);
            String filter =
                    "(PartitionKey gt "
                            + partition
                            + ") or (PartitionKey eq "
                            + partition
                            + " and RowKey gt "
                            + row
                            + ")";
            query += "&$filter=" + encoded(filter);
        }
        if (cursor != null) {
            query += "&NextPartitionKey=" + encoded(cursor.first);
            if (cursor.second != null) {
                query += "&NextRowKey=" + encoded(cursor.second);
            }
        }
        return query;
    }

    private JsonNode values(String collection, AzureTableHttp.Reply reply) {
        JsonNode content = reply.json.get("value");
        if (content == null || !content.isArray() || content.size() > config.pageSize) {
            throw new Failure(collection, "invalid or oversized value array");
        }
        return content;
    }

    private static Entity entity(String table, JsonNode row) {
        if (!row.isObject()) {
            throw new Failure(table, "entity must be a JSON object");
        }
        String partition = text(row.get("PartitionKey"));
        String key = text(row.get("RowKey"));
        String timestamp = text(row.get("Timestamp"));
        JsonNode etagValue = row.get("odata.etag");
        String etag = text(etagValue);
        if (!AzureTableConfig.validKey(partition)
                || !AzureTableConfig.validKey(key)
                || !date(timestamp)
                || etagValue != null && !etagValue.isNull() && etag == null) {
            throw new Failure(table, "invalid entity keys, Timestamp or ETag");
        }
        for (Iterator<Map.Entry<String, JsonNode>> fields = row.fields(); fields.hasNext(); ) {
            Map.Entry<String, JsonNode> field = fields.next();
            String name = field.getKey();
            if (name.endsWith(TYPE_SUFFIX)) {
                JsonNode property =
                        row.get(name.substring(0, name.length() - TYPE_SUFFIX.length()));
                String type = text(field.getValue());
                if (type == null || property == null || !validEdm(type, property)) {
                    throw new Failure(table, "invalid EDM-annotated property");
                }
            }
        }
        return new Entity(partition, key, timestamp, etag, row);
    }

    /**
     * Validates the Table service's eight EDM representations without rewriting them.
     *
     * <p>In particular, the JSON string representation of Int64 and non-finite doubles must remain
     * associated with their annotations.
     */
    private static boolean validEdm(String type, JsonNode value) {
        String raw = text(value);
        try {
            switch (type) {
                case "Edm.Binary":
                    return raw != null && Base64.getDecoder().decode(raw) != null;
                case "Edm.Boolean":
                    return value.isBoolean();
                case "Edm.DateTime":
                    return date(raw);
                case "Edm.Double":
                    return value.isNumber() && Double.isFinite(value.doubleValue())
                            || raw != null && NON_FINITE.contains(raw);
                case "Edm.Guid":
                    return raw != null
                            && raw.matches("(?i)[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}");
                case "Edm.Int32":
                    return value.isIntegralNumber() && value.canConvertToInt();
                case "Edm.Int64":
                    if (raw == null || raw.length() > 20) {
                        return false;
                    }
                    Long.parseLong(raw);
                    return true;
                case "Edm.String":
                    return raw != null;
                default:
                    return false;
            }
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private static boolean date(String value) {
        if (value == null || value.length() > 80) {
            return false;
        }
        try {
            OffsetDateTime.parse(value);
            return true;
        } catch (DateTimeException ex) {
            return false;
        }
    }

    private static String text(JsonNode value) {
        return value != null && value.isTextual() ? value.textValue() : null;
    }

    private static String literal(String raw) {
        return "'" + raw.replace("'", "''") + "'";
    }

    private static String encoded(String raw) {
        return URLEncoder.encode(raw, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * One Azure-defined continuation, with a nullable second member for entity scans.
     *
     * <p>The caller knows whether the first field is a table or partition token. Fingerprints
     * encode the absent row distinctly from a present opaque string.
     */
    static final class Cursor implements Serializable {
        private static final long serialVersionUID = 1L;
        final String first;
        final String second;

        Cursor(String first, String second) {
            this.first = first;
            this.second = second;
        }

        String fingerprint() {
            return AzureTableConfig.hash(first + "\n" + (second == null ? "0" : "1" + second));
        }
    }

    /**
     * Verified table names and the continuation of the account listing.
     *
     * <p>No listing response document is retained after this page is committed.
     */
    static final class DiscoveryPage {
        final List<String> names;
        final Cursor next;

        DiscoveryPage(List<String> names, Cursor next) {
            this.names = names;
            this.next = next;
        }
    }

    /**
     * Verified Azure entity rows and their replay identity.
     *
     * <p>The digest binds both the exact page data and the service's next position.
     */
    static final class EntityPage {
        final List<Entity> rows;
        final Cursor next;
        final String digest;

        EntityPage(List<Entity> rows, Cursor next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * One unmodified provider entity and its stable public identity columns.
     *
     * <p>The composite hash is collision-resistant and cannot confuse keys containing punctuation.
     */
    static final class Entity {
        final String partition;
        final String row;
        final String timestamp;
        final String etag;
        final JsonNode record;
        final String identity;

        Entity(String partition, String row, String timestamp, String etag, JsonNode record) {
            this.partition = partition;
            this.row = row;
            this.timestamp = timestamp;
            this.etag = etag;
            this.record = record;
            this.identity = AzureTableConfig.hash(partition + "\n" + row);
        }
    }

    /**
     * Safe collection-specific protocol failure propagated from the request worker.
     *
     * <p>Only fixed diagnostics and validated table names reach the job.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String collection, String reason) {
            super("AzureTableStorage " + collection + ": " + reason);
        }
    }
}
