package org.apache.seatunnel.connectors.seatunnel.sls.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.sls.config.Config;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SlsSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return Config.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Config.ENDPOINT,
                        Config.PROJECT,
                        Config.LOGSTORE,
                        Config.ACCESS_KEY_ID,
                        Config.ACCESS_KEY_SECRET)
                .optional(Config.SOURCE, Config.TOPIC)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () ->
                new SlsSink(
                        context.getOptions(),
                        context.getCatalogTable().getTableSchema().toPhysicalRowDataType());
    }
}
