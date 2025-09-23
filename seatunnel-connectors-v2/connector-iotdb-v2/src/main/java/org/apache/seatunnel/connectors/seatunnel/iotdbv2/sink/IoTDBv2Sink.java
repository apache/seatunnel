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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.constant.SinkConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.sink.relational.IoTDBv2RelationalSinkWriter;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class IoTDBv2Sink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final ReadonlyConfig pluginConfig;
    private final CatalogTable catalogTable;
    private final String sqlDialect;

    public IoTDBv2Sink(ReadonlyConfig pluginConfig, CatalogTable catalogTable, String sqlDialect) {
        this.pluginConfig = pluginConfig;
        this.catalogTable = catalogTable;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public String getPluginName() {
        return "IoTDBv2";
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        if (SinkConstants.TABLE.equalsIgnoreCase(sqlDialect)) {
            return new IoTDBv2RelationalSinkWriter(
                    pluginConfig, catalogTable.getSeaTunnelRowType());
        }
        return new IoTDBv2SinkWriter(pluginConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
