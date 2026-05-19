package org.apache.seatunnel.azuredataexplorer.sink; // fix 1

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import java.util.Optional;

import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSinkOptions.*; // fix 1

@AutoService(Factory.class)
public class AzureDataExplorerSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "AzureDataExplorer";

    @Override
    public String factoryIdentifier() { return IDENTIFIER; }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(CLUSTER_URI, DATABASE, TABLE)
                .bundled(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
                .optional(INGESTION_MAPPING_REFERENCE, INGESTION_TYPE,
                        BATCH_SIZE, FLUSH_INTERVAL_MS)
                .build();
    }

    @Override
    public AzureDataExplorerSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        return new AzureDataExplorerSink(config);
    }
}