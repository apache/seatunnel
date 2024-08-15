package org.apache.seatunnel.connectors.seatunnel.typesense.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class TypesenseSource
        implements SeaTunnelSource<SeaTunnelRow, TypesenseSourceSplit, TypesenseSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig config;

    private CatalogTable catalogTable;

    public TypesenseSource(ReadonlyConfig config) {
        this.config = config;
        if (config.getOptional(TableSchemaOptions.SCHEMA).isPresent()) {
            catalogTable = CatalogTableUtil.buildWithConfig(config);
        }
    }

    @Override
    public String getPluginName() {
        return "Typesense";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, TypesenseSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new TypesenseSourceReader(readerContext, config, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<TypesenseSourceSplit, TypesenseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<TypesenseSourceSplit> enumeratorContext) {
        return new TypesenseSourceSplitEnumerator(enumeratorContext, config);
    }

    @Override
    public SourceSplitEnumerator<TypesenseSourceSplit, TypesenseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<TypesenseSourceSplit> enumeratorContext,
            TypesenseSourceState checkpointState) {
        return new TypesenseSourceSplitEnumerator(enumeratorContext, config);
    }
}
