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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DefaultDeserializer;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.Deserializer;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterator;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Slf4j
public class IcebergSourceReader implements SourceReader<SeaTunnelRow, IcebergFileScanTaskSplit> {

    private static final long POLL_WAIT_MS = 1000;

    private final Context context;
    private final Queue<IcebergFileScanTaskSplit> pendingSplits;
    private final Deserializer deserializer;
    private final Schema tableSchema;
    private final Schema projectedSchema;
    private final SourceConfig sourceConfig;

    private IcebergTableLoader icebergTableLoader;
    private IcebergFileScanTaskSplitReader icebergFileScanTaskSplitReader;

    private IcebergFileScanTaskSplit currentReadSplit;
    private boolean noMoreSplitsAssignment;

    private CatalogTable catalogTable;

    public IcebergSourceReader(
            @NonNull SourceReader.Context context,
            @NonNull SeaTunnelRowType seaTunnelRowType,
            @NonNull Schema tableSchema,
            @NonNull Schema projectedSchema,
            @NonNull SourceConfig sourceConfig,
            CatalogTable catalogTable) {
        this.context = context;
        this.pendingSplits = new LinkedList<>();
        this.catalogTable = catalogTable;
        this.deserializer = new DefaultDeserializer(seaTunnelRowType, projectedSchema);
        this.tableSchema = tableSchema;
        this.projectedSchema = projectedSchema;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void open() {
        icebergTableLoader = IcebergTableLoader.create(sourceConfig, catalogTable);
        icebergTableLoader.open();

        icebergFileScanTaskSplitReader =
                new IcebergFileScanTaskSplitReader(
                        deserializer,
                        IcebergFileScanTaskReader.builder()
                                .fileIO(icebergTableLoader.loadTable().io())
                                .tableSchema(tableSchema)
                                .projectedSchema(projectedSchema)
                                .caseSensitive(sourceConfig.isCaseSensitive())
                                .reuseContainers(true)
                                .build());
    }

    @Override
    public void close() throws IOException {
        if (icebergFileScanTaskSplitReader != null) {
            icebergFileScanTaskSplitReader.close();
        }
        icebergTableLoader.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        for (IcebergFileScanTaskSplit pendingSplit = pendingSplits.poll();
                pendingSplit != null;
                pendingSplit = pendingSplits.poll()) {
            currentReadSplit = pendingSplit;
            try (CloseableIterator<SeaTunnelRow> rowIterator =
                    icebergFileScanTaskSplitReader.open(currentReadSplit)) {
                while (rowIterator.hasNext()) {
                    output.collect(rowIterator.next());
                }
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
