package org.apache.seatunnel.connectors.seatunnel.deltalake.source;


import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.types.StructType;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.deltalake.DeltaLakeCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.deltalake.source.enumerator.DeltaLakeSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.deltalake.source.reader.DeltaLakeFileScanTaskSplitReader;
import org.apache.seatunnel.connectors.seatunnel.deltalake.source.reader.DeltaLakeSourceReader;
import org.apache.seatunnel.connectors.seatunnel.deltalake.source.split.DeltaLakeFileScanTaskSplit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeltaLakeSource
        implements SeaTunnelSource<SeaTunnelRow, DeltaLakeFileScanTaskSplit, DeltaLakeSplitEnumeratorState>,
        SupportParallelism,
        SupportColumnProjection {

  private static final long serialVersionUID = 4343414808223919870L;
  private final DeltaLakeSourceConfig sourceConfig;

  private final Map<TablePath, CatalogTable> catalogTables;
  private final Map<TablePath, StructType> deltaTableSchemas;

  private JobContext jobContext;

  private DeltaLakeFileScanTaskSplit split;
  private DeltaLakeFileScanTaskSplitReader reader;
  private Collector<SeaTunnelRow> collector;

  public DeltaLakeSource(DeltaLakeSourceConfig sourceConfig, List<CatalogTable> catalogTables) {
    this.sourceConfig = sourceConfig;
    this.catalogTables =
            catalogTables.stream()
                    .collect(Collectors.toMap(CatalogTable::getTablePath, table -> table));
    this.deltaTableSchemas = loadDeltaLakeTableSchema(sourceConfig, this.catalogTables);
  }

  private Map<TablePath, StructType> loadDeltaLakeTableSchema(
          DeltaLakeSourceConfig sourceConfig, Map<TablePath, CatalogTable> catalogTables) {
    DeltaLakeCatalogLoader catalogFactory = new DeltaLakeCatalogLoader(sourceConfig);

    Map<TablePath, StructType> deltaTableSchemas = new HashMap<>();

    for (TablePath tablePath : catalogTables.keySet()) {
      CatalogTable catalogTable = catalogTables.get(tablePath);
      Table deltaTable = catalogFactory.loadTable(tablePath);
      Snapshot latestSnapshot = deltaTable.getLatestSnapshot(engine);
      StructType schema = latestSnapshot.getSchema(engine);
      deltaTableSchemas.put(tablePath, schema);
    }
    return deltaTableSchemas;
  }

  @Override
  public List<CatalogTable> getProducedCatalogTables() {
    return new ArrayList<>(catalogTables.values());
  }

  @Override
  public String getPluginName() {
    return "Deltalake";
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
  public SourceReader<SeaTunnelRow, DeltaLakeFileScanTaskSplit> createReader(SourceReader.Context readerContext) {
    return new DeltaLakeSourceReader(
            readerContext,
            sourceConfig,
            catalogTables,
            deltaTableSchemas
    );
  }

  @Override
  public SourceSplitEnumerator<DeltaLakeFileScanTaskSplit, DeltaLakeSplitEnumeratorState>
  restoreEnumerator(SourceSplitEnumerator.Context<DeltaLakeFileScanTaskSplit> enumeratorContext,
                    DeltaLakeSplitEnumeratorState checkpointState) throws Exception {
    return null;
  }

  @Override
  public SourceSplitEnumerator<DeltaLakeFileScanTaskSplit, DeltaLakeSplitEnumeratorState>
  createEnumerator(SourceSplitEnumerator.Context<DeltaLakeFileScanTaskSplit> enumeratorContext) throws Exception {
    return null;
  }
}
