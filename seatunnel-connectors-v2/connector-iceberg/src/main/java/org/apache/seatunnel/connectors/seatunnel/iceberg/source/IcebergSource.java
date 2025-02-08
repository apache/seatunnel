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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.IcebergBatchSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.IcebergSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.IcebergStreamSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.reader.IcebergSourceReader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergSource
        implements SeaTunnelSource<
                        SeaTunnelRow, IcebergFileScanTaskSplit, IcebergSplitEnumeratorState>,
                SupportParallelism,
                SupportColumnProjection {

    private static final long serialVersionUID = 4343414808223919870L;

    private final SourceConfig sourceConfig;
    private final Map<TablePath, CatalogTable> catalogTables;
    private final Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections;
    private JobContext jobContext;

    public IcebergSource(SourceConfig config, List<CatalogTable> catalogTables) {
        this.sourceConfig = config;
        this.catalogTables =
                catalogTables.stream()
                        .collect(Collectors.toMap(CatalogTable::getTablePath, table -> table));
        this.tableSchemaProjections = loadIcebergSchemaProjections(config, this.catalogTables);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(catalogTables.values());
    }

    @Override
    public String getPluginName() {
        return "Iceberg";
    }

    @SneakyThrows
    private Map<TablePath, Pair<Schema, Schema>> loadIcebergSchemaProjections(
            SourceConfig config, Map<TablePath, CatalogTable> tables) {
        IcebergCatalogLoader catalogFactory = new IcebergCatalogLoader(config);
        Catalog catalog = catalogFactory.loadCatalog();

        Map<TablePath, Pair<Schema, Schema>> icebergTables = new HashMap<>();
        try {
            for (TablePath tablePath : tables.keySet()) {
                CatalogTable catalogTable = tables.get(tablePath);
                Table icebergTable =
                        catalog.loadTable(
                                TableIdentifier.of(
                                        tablePath.getDatabaseName(), tablePath.getTableName()));
                Schema icebergSchema = icebergTable.schema();
                Schema projectedSchema =
                        icebergSchema.select(catalogTable.getTableSchema().getFieldNames());
                icebergTables.put(tablePath, Pair.of(icebergSchema, projectedSchema));
            }
        } finally {
            if (catalog instanceof AutoCloseable) {
                ((AutoCloseable) catalog).close();
            }
        }
        return icebergTables;
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SourceReader<SeaTunnelRow, IcebergFileScanTaskSplit> createReader(
            SourceReader.Context readerContext) {
        return new IcebergSourceReader(
                readerContext, sourceConfig, catalogTables, tableSchemaProjections);
    }

    @Override
    public SourceSplitEnumerator<IcebergFileScanTaskSplit, IcebergSplitEnumeratorState>
            createEnumerator(
                    SourceSplitEnumerator.Context<IcebergFileScanTaskSplit> enumeratorContext) {
        if (Boundedness.BOUNDED.equals(getBoundedness())) {
            return new IcebergBatchSplitEnumerator(
                    enumeratorContext, sourceConfig, catalogTables, tableSchemaProjections);
        }
        return new IcebergStreamSplitEnumerator(
                enumeratorContext, sourceConfig, catalogTables, tableSchemaProjections);
    }

    @Override
    public SourceSplitEnumerator<IcebergFileScanTaskSplit, IcebergSplitEnumeratorState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<IcebergFileScanTaskSplit> enumeratorContext,
                    IcebergSplitEnumeratorState checkpointState) {
        if (Boundedness.BOUNDED.equals(getBoundedness())) {
            return new IcebergBatchSplitEnumerator(
                    enumeratorContext,
                    sourceConfig,
                    catalogTables,
                    tableSchemaProjections,
                    checkpointState);
        }
        return new IcebergStreamSplitEnumerator(
                enumeratorContext,
                sourceConfig,
                catalogTables,
                tableSchemaProjections,
                checkpointState);
    }
}
