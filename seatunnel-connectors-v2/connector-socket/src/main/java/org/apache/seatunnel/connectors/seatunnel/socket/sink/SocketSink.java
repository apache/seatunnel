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

package org.apache.seatunnel.connectors.seatunnel.socket.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketConfig;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkOptions;

import java.io.IOException;
import java.util.Optional;

/**
 * Socket Sink for writing data to a network socket.
 *
 * <p>This sink implements {@link SupportMultiTableSink} to support multi-table routing scenarios.
 * In multi-table mode, multiple source tables can write to the same socket instance without data
 * shuffling, which is useful for CDC and debugging scenarios.
 *
 * <p><b>Current Implementation:</b> Uses a shared schema for all incoming rows (the schema of the
 * first table). This works correctly for single-table jobs and multi-table jobs where all tables
 * share the same schema.
 *
 * @see org.apache.seatunnel.api.sink.SupportMultiTableSink
 * @since 2.3.13
 */
public class SocketSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private final SocketConfig socketConfig;
    private final CatalogTable catalogTable;

    public SocketSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.socketConfig = new SocketConfig(pluginConfig);
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return SocketSinkOptions.identifier;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new SocketSinkWriter(socketConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
