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

package org.apache.seatunnel.connectors.seatunnel.websocket.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.Optional;

/**
 * Sends insert rows over one lazy connection per writer, without transactional delivery state.
 *
 * @see WebSocketSinkWriter
 */
public final class WebSocketSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
    private static final long serialVersionUID = 1L;

    private final WebSocketSinkConfig config;
    // Retain the complete upstream contract; row type alone discards catalog identity/metadata.
    private final CatalogTable catalogTable;
    private final SeaTunnelRowType rowType;

    WebSocketSink(ReadonlyConfig options, CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.config = new WebSocketSinkConfig(options, rowType);
    }

    @Override
    public String getPluginName() {
        return WebSocketSinkFactory.IDENTIFIER;
    }

    /**
     * Declares the same complete table supplied by the factory, so planning retains its schema,
     * identity and metadata while writer serialization derives the unchanged row type.
     */
    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    /*
     * Registers periodic flush through the existing engine hook; checkpoints and final close also drain.
     */
    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        WebSocketSinkWriter writer = new WebSocketSinkWriter(config, rowType);
        context.registerFlushAction(writer::flush);
        return writer;
    }
}
