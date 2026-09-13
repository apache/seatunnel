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

package org.apache.seatunnel.connectors.seatunnel.zendesk.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.zendesk.config.ZendeskConfig;
import org.apache.seatunnel.connectors.seatunnel.zendesk.sink.config.ZendeskSinkOptions;

import java.io.IOException;
import java.util.Optional;

public class ZendeskSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private final CatalogTable catalogTable;
    private final SeaTunnelRowType seaTunnelRowType;
    private final HttpParameter httpParameter;
    private final String resourceKey;
    private final int requestIntervalMs;
    private final int rateLimitBackoffMs;
    private final int rateLimitMaxRetries;

    public ZendeskSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();

        String url = pluginConfig.get(ZendeskConfig.URL);
        String email = pluginConfig.get(ZendeskConfig.EMAIL);
        String apiToken = pluginConfig.get(ZendeskConfig.API_TOKEN);

        this.httpParameter = new HttpParameter();
        this.httpParameter.setUrl(url);
        this.httpParameter.setHeaders(ZendeskConfig.buildAuthHeaders(email, apiToken, null));

        this.resourceKey = pluginConfig.getOptional(ZendeskSinkOptions.RESOURCE_KEY).orElse(null);
        this.requestIntervalMs = pluginConfig.get(ZendeskConfig.REQUEST_INTERVAL_MS);
        this.rateLimitBackoffMs = pluginConfig.get(ZendeskConfig.RATE_LIMIT_BACKOFF_MS);
        this.rateLimitMaxRetries = pluginConfig.get(ZendeskConfig.RATE_LIMIT_MAX_RETRIES);
    }

    @Override
    public String getPluginName() {
        return "Zendesk";
    }

    @Override
    public ZendeskSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new ZendeskSinkWriter(
                seaTunnelRowType,
                httpParameter,
                resourceKey,
                requestIntervalMs,
                rateLimitBackoffMs,
                rateLimitMaxRetries,
                context.getNumberOfParallelSubtasks());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
