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

package org.apache.seatunnel.connectors.seatunnel.solr.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.solr.sink.SolrSinkOptions.CommitMode;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class SolrSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private final CatalogTable catalog;
    private final URI origin;
    private final String corePath;
    private final String authorization;
    private final String[] fieldNames;
    private final int keyIndex;
    private final boolean enableDelete;
    private final int batchSize;
    private final CommitMode commitMode;
    private final int timeoutMillis;

    SolrSink(ReadonlyConfig config, CatalogTable catalog) {
        this.catalog = catalog;
        this.origin =
                origin(
                        config.get(SolrSinkOptions.BASE_URL),
                        config.get(SolrSinkOptions.ALLOW_INSECURE_HTTP));
        String collection = config.get(SolrSinkOptions.COLLECTION);
        if (!collection.matches("[A-Za-z0-9_.-]{1,128}")
                || ".".equals(collection)
                || "..".equals(collection)) {
            throw invalid("collection must be a valid core or collection name");
        }
        this.corePath = "/solr/" + collection;
        this.authorization =
                authorization(
                        config.get(SolrSinkOptions.USERNAME), config.get(SolrSinkOptions.PASSWORD));
        this.enableDelete = config.get(SolrSinkOptions.ENABLE_DELETE);
        this.batchSize = range(config.get(SolrSinkOptions.BATCH_SIZE), 1, 500, "batch_size");
        this.commitMode = config.get(SolrSinkOptions.COMMIT_MODE);
        this.timeoutMillis =
                range(
                        config.get(SolrSinkOptions.REQUEST_TIMEOUT_MS),
                        1,
                        300000,
                        "request_timeout_ms");

        String key = config.get(SolrSinkOptions.UNIQUE_KEY);
        SeaTunnelRowType rowType = catalog.getSeaTunnelRowType();
        String[] source = rowType.getFieldNames();
        Map<String, String> mapping = config.get(SolrSinkOptions.FIELD_MAPPING);
        Set<String> sourceNames = new HashSet<>();
        Set<String> targets = new HashSet<>();
        this.fieldNames = new String[source.length];
        int index = -1;
        for (int i = 0; i < source.length; i++) {
            sourceNames.add(source[i]);
            String target = mapping.getOrDefault(source[i], source[i]);
            if (!validTargetField(target) || !targets.add(target)) {
                throw invalid("field_mapping has an invalid, reserved or duplicate target field");
            }
            this.fieldNames[i] = target;
            if (!supported(rowType.getFieldType(i), true)) {
                throw invalid(
                        "input columns must contain supported scalar values or one-dimensional arrays");
            }
            if (target.equals(key)) {
                index = i;
            }
        }
        if (!sourceNames.containsAll(mapping.keySet())) {
            throw invalid("field_mapping names an input column that does not exist");
        }
        if (index < 0 || !identityType(rowType.getFieldType(index).getSqlType())) {
            throw invalid("the unique_key must map to a STRING or integral input column");
        }
        this.keyIndex = index;
    }

    private static boolean identityType(SqlType type) {
        return type == SqlType.STRING
                || type == SqlType.TINYINT
                || type == SqlType.SMALLINT
                || type == SqlType.INT
                || type == SqlType.BIGINT;
    }

    private static boolean supported(SeaTunnelDataType<?> type, boolean allowArray) {
        switch (type.getSqlType()) {
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case NULL:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case BYTES:
                return true;
            case ARRAY:
                return allowArray && supported(((ArrayType<?, ?>) type).getElementType(), false);
            default:
                return false;
        }
    }

    private static boolean validTargetField(String field) {
        return field != null
                && field.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
                && !"_version_".equals(field)
                && !"_root_".equals(field)
                && !"_childDocuments_".equals(field)
                && !"_nest_path_".equals(field)
                && !"_route_".equals(field);
    }

    private static URI origin(String value, boolean allowHttp) {
        try {
            URI uri = new URI(value);
            boolean scheme =
                    "https".equalsIgnoreCase(uri.getScheme())
                            || (allowHttp && "http".equalsIgnoreCase(uri.getScheme()));
            if (scheme
                    && uri.getHost() != null
                    && !uri.getHost().contains("%")
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && !uri.getRawAuthority().endsWith(":")
                    && uri.getPort() != 0
                    && uri.getPort() <= 65535
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))) {
                return uri;
            }
        } catch (URISyntaxException ignored) {
            // An invalid URI can contain user information.
        }
        throw invalid(
                "base_url must be a trusted origin without path, user info, query or fragment");
    }

    private static String authorization(String username, String password) {
        if (username == null && password == null) {
            return null;
        }
        if (!credential(username) || !credential(password) || username.indexOf(':') >= 0) {
            throw invalid("provide a valid username and password pair");
        }
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString(
                                (username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean credential(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= 4096
                && value.codePoints().noneMatch(Character::isISOControl)
                && StandardCharsets.UTF_8.newEncoder().canEncode(value);
    }

    private static int range(int value, int min, int max, String option) {
        if (value < min || value > max) {
            throw invalid(option + " must be in [" + min + ", " + max + "]");
        }
        return value;
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Solr Sink " + reason);
    }

    @Override
    public String getPluginName() {
        return "Solr";
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalog);
    }

    @Override
    public SolrSinkWriter createWriter(SinkWriter.Context context) {
        SolrClient client = new SolrClient(origin, corePath, authorization, timeoutMillis);
        SolrSinkWriter writer =
                new SolrSinkWriter(
                        client,
                        catalog.getSeaTunnelRowType(),
                        fieldNames,
                        keyIndex,
                        enableDelete,
                        batchSize,
                        commitMode);
        context.enableDeferredTerminalWriteOutcomes();
        context.registerFlushAction(writer::flushAndCommit);
        return writer;
    }
}
