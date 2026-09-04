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

package org.apache.seatunnel.connectors.seatunnel.deeplake.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkOptions;

import java.util.Optional;

public class DeepLakeSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private final CatalogTable catalogTable;
    private final DeepLakeSinkConfig config;

    public DeepLakeSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.config = new DeepLakeSinkConfig(readonlyConfig, catalogTable);
    }

    @Override
    public String getPluginName() {
        return DeepLakeSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public DeepLakeSinkWriter createWriter(SinkWriter.Context context) {
        return new DeepLakeSinkWriter(catalogTable, config);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
