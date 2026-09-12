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

package org.apache.seatunnel.connectors.seatunnel.couchbase.sink;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.couchbase.config.CouchbaseSinkOptions;

import java.io.IOException;
import java.util.Optional;

/**
 * SeaTunnel sink connector for Couchbase.
 *
 * <p>This is a single-table sink: one fixed bucket/scope/collection per job configuration.
 * Multi-table sink support is intentionally out of scope for the initial implementation.
 *
 * <p>Writes {@link SeaTunnelRow} data to a Couchbase collection using a {@link CouchbaseWriter}
 * that buffers records and flushes them in batches.
 */
public class CouchbaseSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

    private final CouchbaseWriterOptions options;
    private final CatalogTable catalogTable;

    public CouchbaseSink(CouchbaseWriterOptions options, CatalogTable catalogTable) {
        this.options = options;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return CouchbaseSinkOptions.CONNECTOR_IDENTITY;
    }

    /**
     * Creates a new writer for a single parallel sub-task.
     *
     * @param context writer context supplied by the engine
     * @return a configured {@link CouchbaseWriter}
     */
    @Override
    public CouchbaseWriter createWriter(SinkWriter.Context context) throws IOException {
        return new CouchbaseWriter(options, catalogTable, context);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
