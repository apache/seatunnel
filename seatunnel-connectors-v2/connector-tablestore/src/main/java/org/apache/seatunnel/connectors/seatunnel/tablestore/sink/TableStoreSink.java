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

package org.apache.seatunnel.connectors.seatunnel.tablestore.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreConfig;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreSinkOptions;

import java.io.IOException;
import java.util.Optional;

public class TableStoreSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final CatalogTable catalogTable;
    private final TableStoreConfig tableStoreConfig;

    public TableStoreSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.tableStoreConfig = new TableStoreConfig(pluginConfig);
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return TableStoreSinkOptions.identifier;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new TableStoreWriter(tableStoreConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
