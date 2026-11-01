package org.apache.seatunnel.azuredataexplorer.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;

public class AzureDataExplorerSource
        implements SeaTunnelSource<
                SeaTunnelRow, AzureDataExplorerSourceSplit, AzureDataExplorerSourceState> {

    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;

    public AzureDataExplorerSource(ReadonlyConfig config) {
        this.config = config;
        this.catalogTable =
                config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()
                        ? CatalogTableUtil.buildWithConfig(config)
                        : null;
    }

    @Override
    public String getPluginName() {
        return AzureDataExplorerSourceFactory.IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        if (catalogTable == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, AzureDataExplorerSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new AzureDataExplorerSourceReader(
                readerContext,
                config,
                catalogTable == null ? null : catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<AzureDataExplorerSourceSplit, AzureDataExplorerSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<AzureDataExplorerSourceSplit> enumeratorContext) {
        return new AzureDataExplorerSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<AzureDataExplorerSourceSplit, AzureDataExplorerSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<AzureDataExplorerSourceSplit> enumeratorContext,
                    AzureDataExplorerSourceState checkpointState) {
        return new AzureDataExplorerSplitEnumerator(enumeratorContext, checkpointState);
    }
}
