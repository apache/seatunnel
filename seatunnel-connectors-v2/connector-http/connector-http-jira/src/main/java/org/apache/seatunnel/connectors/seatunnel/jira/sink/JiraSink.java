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

package org.apache.seatunnel.connectors.seatunnel.jira.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.jira.sink.config.JiraSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jira.sink.config.JiraSinkOptions.Operation;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/**
 * Validates the command schema and trusted Jira origin before creating isolated synchronous
 * writers. Mutations are acknowledged individually and do not participate in a checkpoint
 * transaction.
 */
final class JiraSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final CatalogTable table;
    private final URI origin;
    private final String authorization;
    private final String issuePath;
    private final Operation operation;
    private final int timeoutMillis;
    private final int maxRetries;
    private final int identityIndex;
    private final int fieldsIndex;

    JiraSink(ReadonlyConfig options, CatalogTable table) {
        this.table = table;
        this.operation = options.get(JiraSinkOptions.OPERATION);
        int apiVersion = options.get(JiraSinkOptions.API_VERSION);
        if (apiVersion != 3) {
            throw new IllegalArgumentException("Jira Sink api_version must be 3");
        }
        this.issuePath = "/rest/api/" + apiVersion + "/issue";
        this.origin =
                validateOrigin(
                        options.get(JiraSinkOptions.BASE_URL),
                        options.get(JiraSinkOptions.ALLOW_INSECURE_HTTP));
        String username = validateCredential(options.get(JiraSinkOptions.USERNAME), "username");
        if (username.indexOf(':') >= 0) {
            throw new IllegalArgumentException("Jira Sink username must not contain a colon");
        }
        String token = validateCredential(options.get(JiraSinkOptions.TOKEN), "token");
        this.authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + token).getBytes(StandardCharsets.UTF_8));
        this.timeoutMillis = options.get(JiraSinkOptions.REQUEST_TIMEOUT_MS);
        if (timeoutMillis < 1 || timeoutMillis > 300000) {
            throw new IllegalArgumentException(
                    "Jira Sink request_timeout_ms must be between 1 and 300000");
        }
        this.maxRetries = options.get(JiraSinkOptions.MAX_RETRIES);
        if (maxRetries < 0 || maxRetries > 10) {
            throw new IllegalArgumentException("Jira Sink max_retries must be between 0 and 10");
        }

        SeaTunnelRowType type = table.getSeaTunnelRowType();
        int expectedFields = operation == Operation.UPDATE ? 2 : 1;
        if (type.getTotalFields() != expectedFields) {
            throw new IllegalArgumentException(
                    "Jira Sink "
                            + operation
                            + " requires exactly "
                            + expectedFields
                            + " schema column(s): fields for CREATE, issue_id_or_key and fields for UPDATE,"
                            + " or issue_id_or_key for DELETE");
        }
        this.identityIndex =
                operation == Operation.CREATE ? -1 : requireString(type, "issue_id_or_key");
        this.fieldsIndex = operation == Operation.DELETE ? -1 : requireString(type, "fields");
    }

    private static int requireString(SeaTunnelRowType type, String name) {
        String[] names = type.getFieldNames();
        for (int i = 0; i < names.length; i++) {
            if (name.equals(names[i]) && BasicType.STRING_TYPE.equals(type.getFieldType(i))) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Jira Sink schema requires a STRING column named " + name);
    }

    /* Reject paths and embedded credentials so command paths cannot escape the configured origin. */
    private static URI validateOrigin(String value, boolean allowInsecureHttp) {
        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            throw invalidOrigin();
        }
        boolean validScheme =
                "https".equalsIgnoreCase(uri.getScheme())
                        || (allowInsecureHttp && "http".equalsIgnoreCase(uri.getScheme()));
        if (!validScheme
                || uri.getHost() == null
                || uri.getRawUserInfo() != null
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null
                || uri.getRawAuthority().endsWith(":")
                || uri.getHost().contains("%")
                || uri.getPort() == 0
                || uri.getPort() > 65535
                || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))) {
            throw invalidOrigin();
        }
        return uri;
    }

    private static IllegalArgumentException invalidOrigin() {
        return new IllegalArgumentException(
                "Jira Sink base_url must be a trusted HTTPS origin without user info, path, query or fragment");
    }

    private static String validateCredential(String value, String option) {
        if (value.trim().isEmpty()
                || !value.equals(value.trim())
                || value.codePoints().anyMatch(Character::isISOControl)
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new IllegalArgumentException("Jira Sink " + option + " is invalid");
        }
        return value;
    }

    @Override
    public String getPluginName() {
        return "Jira";
    }

    @Override
    public JiraSinkWriter createWriter(SinkWriter.Context context) {
        return new JiraSinkWriter(
                operation,
                issuePath,
                identityIndex,
                fieldsIndex,
                new JiraClient(origin, authorization, operation, timeoutMillis, maxRetries));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(table);
    }
}
