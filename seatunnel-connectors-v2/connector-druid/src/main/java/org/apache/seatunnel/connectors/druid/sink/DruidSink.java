/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.druid.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;

import java.io.IOException;
import java.util.Optional;

import static org.apache.seatunnel.connectors.druid.config.DruidSinkOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.druid.config.DruidSinkOptions.COORDINATOR_URL;
import static org.apache.seatunnel.connectors.druid.config.DruidSinkOptions.DATASOURCE;

public class DruidSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private ReadonlyConfig config;
    private CatalogTable catalogTable;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "Druid";
    }

    public DruidSink(ReadonlyConfig config, CatalogTable table) {
        this.config = config;
        this.catalogTable = table;
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public DruidWriter createWriter(SinkWriter.Context context) throws IOException {
        return new DruidWriter(
                seaTunnelRowType,
                config.get(COORDINATOR_URL),
                config.get(DATASOURCE),
                config.get(BATCH_SIZE));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
