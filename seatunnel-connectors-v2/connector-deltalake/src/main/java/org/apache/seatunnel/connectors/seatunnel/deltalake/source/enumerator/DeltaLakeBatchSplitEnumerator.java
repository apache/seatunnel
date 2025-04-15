package org.apache.seatunnel.connectors.seatunnel.deltalake.source.enumerator;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.deltalake.source.split.DeltaLakeFileScanTaskSplit;

import java.util.*;

@Slf4j
public class DeltaLakeBatchSplitEnumerator extends AbstractSplitEnumerator {

    private Engine engine;

    public DeltaLakeBatchSplitEnumerator(
            Context<DeltaLakeFileScanTaskSplit> context,
            DeltaLakeSourceConfig config,
            Map<TablePath, CatalogTable> catalogTables,
            Engine engine) {
        super(context, config, catalogTables, engine);
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            TablePath tablePath = pendingTables.poll();
            List<DeltaLakeFileScanTaskSplit> splits = loadSplits(tablePath);
            log.info("Loaded {} splits from table {}", splits.size(), tablePath);
            synchronized (stateLock) {
                addPendingSplits(splits);
                assignPendingSplits(readers);
            }
        }

        // Signal end of scan
        for (int reader : readers) {
            context.signalNoMoreSplits(reader);
        }
    }

    @Override
    public DeltaLakeSplitEnumeratorState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new DeltaLakeSplitEnumeratorState(
                    new ArrayList<>(pendingTables),
                    new HashMap<>(pendingSplits));
        }
    }

    private List<DeltaLakeFileScanTaskSplit> loadSplits(TablePath tablePath) {
        try {
            Table table = loadTable(tablePath);
            Snapshot snapshot = table.getLatestSnapshot(engine);
            ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);
            CloseableIterator<FilteredColumnarBatch> files = scanBuilder.build().getScanFiles(engine);

            List<DeltaLakeFileScanTaskSplit> splits = new ArrayList<>();
            while (files.hasNext()) {
                FilteredColumnarBatch file = files.next();
                splits.add(new DeltaLakeFileScanTaskSplit(
                        tablePath,
                        file.getPath(),
                        file.getSize(),
                        0,
                        file.getSize(),
                        null
                ));
            }

            return splits;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load splits for table: " + tablePath, e);
        }
    }

    @Override
    protected Table loadTable(TablePath path) {
        return sourceConfig.getCatalog().loadTable(path);
    }
}
