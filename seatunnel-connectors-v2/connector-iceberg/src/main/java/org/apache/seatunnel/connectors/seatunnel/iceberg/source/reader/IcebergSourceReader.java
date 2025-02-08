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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.reader;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DefaultDeserializer;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.Deserializer;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.CloseableIterator;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class IcebergSourceReader implements SourceReader<SeaTunnelRow, IcebergFileScanTaskSplit> {

    private static final long POLL_WAIT_MS = 1000;

    private final Context context;
    private final SourceConfig sourceConfig;
    private final Map<TablePath, CatalogTable> tables;
    private final Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections;
    private final BlockingQueue<IcebergFileScanTaskSplit> pendingSplits;

    private volatile IcebergFileScanTaskSplit currentReadSplit;
    private volatile boolean noMoreSplitsAssignment;

    private Catalog catalog;
    private ConcurrentMap<TablePath, IcebergFileScanTaskSplitReader> tableReaders;

    public IcebergSourceReader(
            @NonNull SourceReader.Context context,
            @NonNull SourceConfig sourceConfig,
            @NonNull Map<TablePath, CatalogTable> tables,
            @NonNull Map<TablePath, Pair<Schema, Schema>> tableSchemaProjections) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.tables = tables;
        this.tableSchemaProjections = tableSchemaProjections;
        this.pendingSplits = new LinkedBlockingQueue<>();
        this.tableReaders = new ConcurrentHashMap<>();
    }

    @Override
    public void open() {
        IcebergCatalogLoader catalogFactory = new IcebergCatalogLoader(sourceConfig);
        catalog = catalogFactory.loadCatalog();
    }

    @Override
    public void close() throws IOException {
        if (catalog != null && catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }
        tableReaders.forEach((tablePath, reader) -> reader.close());
    }

    private IcebergFileScanTaskSplitReader getOrCreateTableReader(TablePath tablePath) {
        IcebergFileScanTaskSplitReader tableReader = tableReaders.get(tablePath);
        if (tableReader != null) {
            return tableReader;
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // clean up table readers if the source is bounded
            tableReaders.forEach((key, value) -> value.close());
            tableReaders.clear();
        }

        return tableReaders.computeIfAbsent(
                tablePath,
                key -> {
                    SourceTableConfig tableConfig = sourceConfig.getTableConfig(key);
                    CatalogTable catalogTable = tables.get(key);
                    Pair<Schema, Schema> pair = tableSchemaProjections.get(key);
                    Schema tableSchema = pair.getLeft();
                    Schema projectedSchema = pair.getRight();
                    Deserializer deserializer =
                            new DefaultDeserializer(
                                    catalogTable.getSeaTunnelRowType(), projectedSchema);

                    Table icebergTable = catalog.loadTable(tableConfig.getTableIdentifier());
                    return new IcebergFileScanTaskSplitReader(
                            deserializer,
                            IcebergFileScanTaskReader.builder()
                                    .fileIO(icebergTable.io())
                                    .tableSchema(tableSchema)
                                    .projectedSchema(projectedSchema)
                                    .caseSensitive(sourceConfig.isCaseSensitive())
                                    .reuseContainers(true)
                                    .build());
                });
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            currentReadSplit = pendingSplits.poll();
            if (currentReadSplit != null) {
                IcebergFileScanTaskSplitReader tableReader =
                        getOrCreateTableReader(currentReadSplit.getTablePath());
                try (CloseableIterator<SeaTunnelRow> rowIterator =
                        tableReader.open(currentReadSplit)) {
                    while (rowIterator.hasNext()) {
                        output.collect(rowIterator.next());
                    }
                }
                return;
            }
        }

        if (noMoreSplitsAssignment && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            context.signalNoMoreElement();
        } else {
            context.sendSplitRequest();
            if (pendingSplits.isEmpty()) {
                Thread.sleep(POLL_WAIT_MS);
            }
        }
    }

    @Override
    public List<IcebergFileScanTaskSplit> snapshotState(long checkpointId) {
        List<IcebergFileScanTaskSplit> readerState = new ArrayList<>();
        if (!pendingSplits.isEmpty()) {
            readerState.addAll(pendingSplits);
        }
        if (currentReadSplit != null) {
            readerState.add(currentReadSplit);
        }
        return readerState;
    }

    @Override
    public void addSplits(List<IcebergFileScanTaskSplit> splits) {
        log.info("Add {} splits to reader", splits.size());
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
