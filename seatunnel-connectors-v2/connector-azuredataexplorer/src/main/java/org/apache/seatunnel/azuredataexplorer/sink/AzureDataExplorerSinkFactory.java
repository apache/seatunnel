package org.apache.seatunnel.azuredataexplorer.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.BATCH_SIZE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.CLIENT_ID;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.CLIENT_SECRET;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.CLUSTER_URI;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.DATABASE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.FLUSH_INTERVAL_MS;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.INGESTION_MAPPING_REFERENCE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.INGESTION_TYPE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.TABLE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.TENANT_ID;

@AutoService(Factory.class)
public class AzureDataExplorerSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "AzureDataExplorer";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(CLUSTER_URI, DATABASE, TABLE)
                .bundled(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
                .optional(
                        INGESTION_MAPPING_REFERENCE, INGESTION_TYPE, BATCH_SIZE, FLUSH_INTERVAL_MS)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        return () -> new AzureDataExplorerSink(config);
    }
}
