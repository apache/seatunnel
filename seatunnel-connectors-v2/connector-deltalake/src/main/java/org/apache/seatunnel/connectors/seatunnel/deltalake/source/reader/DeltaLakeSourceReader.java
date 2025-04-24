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

package org.apache.seatunnel.connectors.seatunnel.deltalake.source.reader;

import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.seatunnel.connectors.seatunnel.deltalake.DeltaLakeCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.SourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.deltalake.data.DefaultDeserializer;
import org.apache.seatunnel.connectors.seatunnel.deltalake.data.Deserializer;
import org.apache.seatunnel.connectors.seatunnel.deltalake.source.split.DeltaLakeFileScanTaskSplit;

@Slf4j
public class DeltaLakeSourceReader implements SourceReader<SeaTunnelRow, DeltaLakeFileScanTaskSplit> {

  private static final long POLL_WAIT_MS = 1000;

  private final Context context;
  private final DeltaLakeSourceConfig sourceConfig;
  private final Map<TablePath, CatalogTable> tables;
  private final LinkedBlockingQueue<DeltaLakeFileScanTaskSplit> pendingSplits;
  private final ConcurrentHashMap<TablePath, DeltaLakeFileScanTaskSplitReader> tableReaders;
  private final Map<TablePath, List<String>> filterColumns;
  private volatile DeltaLakeFileScanTaskSplit currentReadSplit;
  private volatile boolean noMoreSplitsAssignment;
  private Catalog catalog;

  public DeltaLakeSourceReader(
          @NonNull SourceReader.Context context,
          @NonNull DeltaLakeSourceConfig sourceConfig,
          @NonNull Map<TablePath, CatalogTable> tables,
          @NonNull Map<TablePath, List<String>> filterColumns) {
    this.context = context;
    this.sourceConfig = sourceConfig;
    this.tables = tables;
    this.filterColumns = filterColumns;
    this.pendingSplits = new LinkedBlockingQueue<>();
    this.tableReaders = new ConcurrentHashMap<>();
  }

  @Override
  public void open() throws Exception {
    DeltaLakeCatalogLoader catalogLoader =
            new DeltaLakeCatalogLoader(sourceConfig);
    this.catalog = catalogLoader.loadCatalog();
  }

  @Override
  public void close() throws IOException {
    if (catalog != null && catalog instanceof Closeable) {
      ((Closeable) catalog).close();
    }
    tableReaders.forEach((tablePath, reader) -> reader.close());
  }

  @Override
  public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
    synchronized (output.getCheckpointLock()) {
      currentReadSplit = pendingSplits.poll();
      if (currentReadSplit != null) {
        DeltaLakeFileScanTaskSplitReader tableReader =
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

  private DeltaLakeFileScanTaskSplitReader getOrCreateTableReader(TablePath tablePath) {
    DeltaLakeFileScanTaskSplitReader tableReader = tableReaders.get(tablePath);
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
              StructType deltaTableSchema = deltaTableSchemas.get(key);
              Deserializer deserializer =
                      new DefaultDeserializer(
                              catalogTable.getSeaTunnelRowType(), deltaTableSchema);

              return new DeltaLakeFileScanTaskSplitReader(
                      deserializer,
                      DeltaLakeFileScanTaskReader.builder()
                              .engine(engine)
                              .columnsOpt(filterColumns.get(key))
                              .build());
            });
  }

  @Override
  public List<DeltaLakeFileScanTaskSplit> snapshotState(long checkpointId) throws Exception {
    List<DeltaLakeFileScanTaskSplit> readerState = new ArrayList<>();
    if (!pendingSplits.isEmpty()) {
      readerState.addAll(pendingSplits);
    }
    if (currentReadSplit != null) {
      readerState.add(currentReadSplit);
    }
    return readerState;
  }

  @Override
  public void addSplits(List<DeltaLakeFileScanTaskSplit> splits) {
    log.info("Add {} splits to reader", splits.size());
    pendingSplits.addAll(splits);
  }

  @Override
  public void handleNoMoreSplits() {
    log.info("Reader received NoMoreSplits event.");
    noMoreSplitsAssignment = true;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {

  }
}
