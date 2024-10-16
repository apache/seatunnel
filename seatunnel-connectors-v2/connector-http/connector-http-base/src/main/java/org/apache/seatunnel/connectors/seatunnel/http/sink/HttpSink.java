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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class HttpSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {
    protected final HttpParameter httpParameter = new HttpParameter();
    protected CatalogTable catalogTable;
    protected SeaTunnelRowType seaTunnelRowType;
    protected Config pluginConfig;

    public HttpSink(Config pluginConfig, CatalogTable catalogTable) {
        this.pluginConfig = pluginConfig;
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HttpConfig.URL.key());
        if (!result.isSuccess()) {
            throw new HttpConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        httpParameter.setUrl(pluginConfig.getString(HttpConfig.URL.key()));
        if (pluginConfig.hasPath(HttpConfig.HEADERS.key())) {
            httpParameter.setHeaders(
                    pluginConfig.getConfig(HttpConfig.HEADERS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        if (pluginConfig.hasPath(HttpConfig.PARAMS.key())) {
            httpParameter.setHeaders(
                    pluginConfig.getConfig(HttpConfig.PARAMS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
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
