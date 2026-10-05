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

package org.apache.seatunnel.connectors.seatunnel.airtable.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.airtable.config.AirtableConfig;
import org.apache.seatunnel.connectors.seatunnel.airtable.sink.config.AirtableSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import java.io.IOException;
import java.util.Optional;

public class AirtableSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private final CatalogTable catalogTable;
    private final SeaTunnelRowType seaTunnelRowType;
    private final HttpParameter httpParameter;
    private final int batchSize;
    private final boolean typecast;
    private final int requestIntervalMs;
    private final int rateLimitBackoffMs;
    private final int rateLimitMaxRetries;

    public AirtableSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();

        String baseId = pluginConfig.get(AirtableConfig.BASE_ID);
        String table = pluginConfig.get(AirtableConfig.TABLE);
        String token = pluginConfig.get(AirtableConfig.TOKEN);
        String apiBaseUrl =
                pluginConfig
                        .getOptional(AirtableConfig.API_BASE_URL)
                        .orElse(AirtableConfig.DEFAULT_API_BASE_URL);

        this.httpParameter = new HttpParameter();
        this.httpParameter.setUrl(AirtableConfig.buildBaseUrl(apiBaseUrl, baseId, table));
        this.httpParameter.setHeaders(AirtableConfig.buildAuthHeaders(token, null));

        this.batchSize = pluginConfig.get(AirtableSinkOptions.BATCH_SIZE);
        this.typecast = pluginConfig.get(AirtableSinkOptions.TYPECAST);
        this.requestIntervalMs = pluginConfig.get(AirtableConfig.REQUEST_INTERVAL_MS);
        this.rateLimitBackoffMs = pluginConfig.get(AirtableConfig.RATE_LIMIT_BACKOFF_MS);
        this.rateLimitMaxRetries = pluginConfig.get(AirtableConfig.RATE_LIMIT_MAX_RETRIES);
    }

    @Override
    public String getPluginName() {
        return "Airtable";
    }

    @Override
    public AirtableSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new AirtableSinkWriter(
                seaTunnelRowType,
                httpParameter,
                batchSize,
                typecast,
                requestIntervalMs,
                rateLimitBackoffMs,
                rateLimitMaxRetries);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
