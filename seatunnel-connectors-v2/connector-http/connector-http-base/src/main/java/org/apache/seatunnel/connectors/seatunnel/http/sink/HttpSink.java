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

package org.apache.seatunnel.connectors.seatunnel.http.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSinkOptions;

import java.io.IOException;
import java.util.Optional;

public class HttpSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {
    protected final HttpParameter httpParameter = new HttpParameter();
    protected CatalogTable catalogTable;
    protected SeaTunnelRowType seaTunnelRowType;
    protected ReadonlyConfig pluginConfig;

    public HttpSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.pluginConfig = pluginConfig;
        httpParameter.setUrl(pluginConfig.get(HttpSinkOptions.URL));
        if (pluginConfig.getOptional(HttpSinkOptions.HEADERS).isPresent()) {
            httpParameter.setHeaders(pluginConfig.get(HttpSinkOptions.HEADERS));
        }
        if (pluginConfig.getOptional(HttpSinkOptions.PARAMS).isPresent()) {
            httpParameter.setHeaders(pluginConfig.get(HttpSinkOptions.PARAMS));
        }
        this.catalogTable = catalogTable;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public String getPluginName() {
        return HttpConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public HttpSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new HttpSinkWriter(seaTunnelRowType, httpParameter);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
