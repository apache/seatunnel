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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.catalog.MongodbCatalog;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataDocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataToBsonConverters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit.MongodbSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.savemode.MongodbSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import java.util.Optional;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

public class MongodbSink
        implements SeaTunnelSink<
                        SeaTunnelRow, DocumentBulk, MongodbCommitInfo, MongodbAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {

    private final MongodbWriterOptions options;

    private final CatalogTable catalogTable;

    public MongodbSink(MongodbWriterOptions options, CatalogTable catalogTable) {
        this.options = options;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return MongodbSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public MongodbWriter createWriter(SinkWriter.Context context) {
        return new MongodbWriter(
                new RowDataDocumentSerializer(
                        RowDataToBsonConverters.createConverter(catalogTable.getSeaTunnelRowType()),
                        options,
                        new MongoKeyExtractor(options)),
                options,
                context);
    }

    @Override
    public Optional<Serializer<DocumentBulk>> getWriterStateSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public Optional<SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return options.transaction
                ? Optional.of(new MongodbSinkAggregatedCommitter(options))
                : Optional.empty();
    }

    @Override
    public Optional<Serializer<MongodbAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public Optional<Serializer<MongodbCommitInfo>> getCommitInfoSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        String url = options.getConnectString();
        String database = options.getDatabase();
        if (catalogTable != null) {
            Optional<Catalog> catalogOptional =
                    Optional.of(
                            new MongodbCatalog(
                                    MongodbSinkOptions.CONNECTOR_IDENTITY, url, database));
            try {
                DataSaveMode dataSaveMode = options.getDataSaveMode();
                Catalog catalog = catalogOptional.get();
                return Optional.of(
                        new MongodbSaveModeHandler(
                                SchemaSaveMode.IGNORE, dataSaveMode, catalog, catalogTable));
            } catch (Exception e) {
                throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
            }
        }
        return Optional.empty();
    }
}
