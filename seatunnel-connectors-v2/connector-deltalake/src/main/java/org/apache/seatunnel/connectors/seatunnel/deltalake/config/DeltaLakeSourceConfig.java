package org.apache.seatunnel.connectors.seatunnel.deltalake.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.util.List;

public class DeltaLakeSourceConfig extends DeltaLakeCommonConfig {

  private static final long serialVersionUID = -1965861967575264253L;

  private long incrementScanInterval;
  private List<SourceTableConfig> tableList;

  public DeltaLakeSourceConfig(ReadonlyConfig pluginConfig) {
    super(pluginConfig);
  }

  public SourceTableConfig getTableConfig(TablePath tablePath) {
    return tableList.stream()
            .filter(tableConfig -> tableConfig.getTablePath().equals(tablePath))
            .findFirst()
            .get();
  }
}
