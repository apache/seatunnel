package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;

@AutoService(Factory.class)
public class IcebergSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule
                .builder()
                .required(
                        SinkConfig.KEY_CATALOG_NAME,
                        SinkConfig.KEY_CATALOG_TYPE,
                        SinkConfig.KEY_FIELDS,
                        SinkConfig.KEY_NAMESPACE,
                        SinkConfig.KEY_TABLE,
                        SinkConfig.KEY_WAREHOUSE
                )
                .optional(SinkConfig.KEY_URI)
                .build();
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        return IcebergSink::new;
    }
}
