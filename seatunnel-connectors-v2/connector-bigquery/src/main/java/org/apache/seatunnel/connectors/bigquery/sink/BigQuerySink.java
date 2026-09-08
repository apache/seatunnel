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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.bigquery.catalog.BigQueryCatalog;
import org.apache.seatunnel.connectors.bigquery.convert.BigQuerySerializer;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfo;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitInfoSerializer;
import org.apache.seatunnel.connectors.bigquery.sink.committer.BigQueryCommitter;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class BigQuerySink
        implements SeaTunnelSink<
                        SeaTunnelRow, BigQuerySinkState, BigQueryCommitInfo, BigQueryCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {

    private final ReadonlyConfig config;
    private final boolean isBatch;
    private final CatalogTable catalogTable;

    public BigQuerySink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        if (BigQuerySinkBatchWriter.BATCH.equals(config.get(BigQuerySinkOptions.WRITE_MODE))) {
            this.isBatch = true;
        } else if (BigQuerySinkStreamWriter.STREAMING.equals(
                config.get(BigQuerySinkOptions.WRITE_MODE))) {
            this.isBatch = false;
        } else {
            throw CommonError.illegalArgument(
                    config.get(BigQuerySinkOptions.WRITE_MODE),
                    BigQuerySinkOptions.WRITE_MODE.key());
        }
        this.catalogTable = catalogTable;
    }

    @Override
    public AbstractBigQuerySinkWriter createWriter(SinkWriter.Context context) {
        if (isBatch) {
            return new BigQuerySinkBatchWriter(
                    config, new BigQuerySerializer(catalogTable, config));
        } else {
            return new BigQuerySinkStreamWriter(
                    config, new BigQuerySerializer(catalogTable, config));
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, BigQueryCommitInfo, BigQuerySinkState> restoreWriter(
            SinkWriter.Context context, List<BigQuerySinkState> states) {
        if (isBatch) {
            if (states != null && !states.isEmpty()) {
                BigQuerySinkState latestState = getLatestState(states);
                return new BigQuerySinkBatchWriter(
                        config,
                        new BigQuerySerializer(catalogTable, config),
                        latestState.getStreamName(),
                        latestState.getNextOffset());
            } else {
                return new BigQuerySinkBatchWriter(
                        config, new BigQuerySerializer(catalogTable, config));
            }
        } else {
            return new BigQuerySinkStreamWriter(
                    config, new BigQuerySerializer(catalogTable, config));
        }
    }

    @Override
    public Optional<SinkCommitter<BigQueryCommitInfo>> createCommitter() {
        return Optional.of(new BigQueryCommitter(config));
    }

    @Override
    public Optional<Serializer<BigQueryCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new BigQueryCommitInfoSerializer());
    }

    @Override
    public Optional<Serializer<BigQuerySinkState>> getWriterStateSerializer() {
        return Optional.of(new BigQuerySinkStateSerializer());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public String getPluginName() {
        return BigQuerySinkOptions.IDENTIFIER;
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (catalogTable == null) {
            return Optional.empty();
        }
        Catalog catalog = new BigQueryCatalog(catalogTable.getCatalogName(), config);
        SchemaSaveMode schemaSaveMode = config.get(BigQuerySinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(BigQuerySinkOptions.DATA_SAVE_MODE);
        TablePath tablePath = catalogTable.getTableId().toTablePath();
        String configuredTableId = config.get(BigQuerySinkOptions.TABLE_ID);
        // Note: config option TABLE_ID is already placeholder-resolved per target table
        // by TablePlaceholderProcessor during FactoryUtil.createAndPrepareSink()
        if (configuredTableId != null && !configuredTableId.contains("${table_name}")) {
            tablePath = TablePath.of(tablePath.getDatabaseName(), configuredTableId);
        }
        String customSql = config.get(BigQuerySinkOptions.CUSTOM_SQL);
        return Optional.of(
                new BigQuerySaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, tablePath, catalogTable, customSql));
    }

    static BigQuerySinkState getLatestState(List<BigQuerySinkState> states) {
        return states.stream()
                .max(Comparator.comparingLong(BigQuerySinkState::getCheckpointId))
                .get();
    }
}
