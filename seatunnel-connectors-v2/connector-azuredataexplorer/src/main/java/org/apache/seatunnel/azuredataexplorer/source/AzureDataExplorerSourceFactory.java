package org.apache.seatunnel.azuredataexplorer.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SourceConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.CLUSTER_URI;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.DATABASE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.QUERY;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.TENANT_ID;

@AutoService(Factory.class)
public class AzureDataExplorerSourceFactory implements TableSourceFactory {
    public static final String IDENTIFIER = "AzureDataExplorer";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(CLUSTER_URI, DATABASE, QUERY)
                .bundled(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
                .optional(SourceConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AzureDataExplorerSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new AzureDataExplorerSource(context.getOptions());
    }
}
