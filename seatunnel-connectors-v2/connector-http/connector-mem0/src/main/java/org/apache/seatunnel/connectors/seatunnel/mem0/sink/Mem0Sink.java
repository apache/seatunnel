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
package org.apache.seatunnel.connectors.seatunnel.mem0.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.mem0.config.Mem0SinkOptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Mem0Sink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {
    private final CatalogTable catalogTable;
    private final SeaTunnelRowType rowType;
    private final HttpParameter httpParameter;
    private final String messagesField;
    private final String userIdField;
    private final String agentIdField;
    private final String appIdField;
    private final String runIdField;
    private final String metadataField;

    public Mem0Sink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.messagesField = config.get(Mem0SinkOptions.MESSAGES_FIELD);
        this.userIdField = config.get(Mem0SinkOptions.USER_ID_FIELD);
        this.agentIdField = config.getOptional(Mem0SinkOptions.AGENT_ID_FIELD).orElse(null);
        this.appIdField = config.getOptional(Mem0SinkOptions.APP_ID_FIELD).orElse(null);
        this.runIdField = config.getOptional(Mem0SinkOptions.RUN_ID_FIELD).orElse(null);
        this.metadataField = config.getOptional(Mem0SinkOptions.METADATA_FIELD).orElse(null);
        String baseUrl = config.get(Mem0SinkOptions.API_BASE_URL);
        this.httpParameter = new HttpParameter();
        this.httpParameter.setUrl(normalizeBaseUrl(baseUrl) + Mem0SinkOptions.ADD_PATH);
        Map<String, String> headers = new HashMap<>();
        headers.put(Mem0SinkOptions.AUTHORIZATION, "Token " + config.get(Mem0SinkOptions.API_KEY));
        headers.put(Mem0SinkOptions.CONTENT_TYPE, Mem0SinkOptions.APPLICATION_JSON);
        headers.put(Mem0SinkOptions.ACCEPT, Mem0SinkOptions.APPLICATION_JSON);
        this.httpParameter.setHeaders(headers);
        this.httpParameter.setRetry(config.getOptional(Mem0SinkOptions.RETRY).orElse(0));
        this.httpParameter.setRetryBackoffMultiplierMillis(
                config.get(Mem0SinkOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        this.httpParameter.setRetryBackoffMaxMillis(
                config.get(Mem0SinkOptions.RETRY_BACKOFF_MAX_MS));
    }

    private static String normalizeBaseUrl(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    @Override
    public String getPluginName() {
        return "Mem0";
    }

    @Override
    public Mem0SinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new Mem0SinkWriter(
                rowType,
                httpParameter,
                messagesField,
                userIdField,
                agentIdField,
                appIdField,
                runIdField,
                metadataField);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
