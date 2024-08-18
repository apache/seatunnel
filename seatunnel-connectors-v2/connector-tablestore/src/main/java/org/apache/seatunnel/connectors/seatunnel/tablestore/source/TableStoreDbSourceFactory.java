package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class TableStoreDbSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Tablestore";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        TablestoreConfig.END_POINT,
                        TablestoreConfig.INSTANCE_NAME,
                        TablestoreConfig.ACCESS_KEY_ID,
                        TablestoreConfig.ACCESS_KEY_SECRET,
                        TablestoreConfig.TABLE)
                .optional(TablestoreConfig.BATCH_SIZE)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return TableStoreDBSource.class;
    }
}
