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

package org.apache.seatunnel.connectors.seatunnel.datahub.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter.Context;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions.ACCESS_ID;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions.RETRY_TIMES;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions.TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.datahub.config.DataHubSinkOptions.TOPIC;

public class DataHubSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final ReadonlyConfig pluginConfig;
    private final CatalogTable catalogTable;

    public DataHubSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.pluginConfig = pluginConfig;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return "DataHub";
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(Context context) throws IOException {
        return new DataHubWriter(
                catalogTable.getSeaTunnelRowType(),
                pluginConfig.get(ENDPOINT),
                pluginConfig.get(ACCESS_ID),
                pluginConfig.get(ACCESS_KEY),
                pluginConfig.get(PROJECT),
                pluginConfig.get(TOPIC),
                pluginConfig.get(TIMEOUT),
                pluginConfig.get(RETRY_TIMES));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
