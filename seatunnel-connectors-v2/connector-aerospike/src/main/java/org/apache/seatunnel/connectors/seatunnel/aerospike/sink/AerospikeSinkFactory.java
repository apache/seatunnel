package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class AerospikeSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "aerospike";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AerospikeSinkOptions.HOST,
                        AerospikeSinkOptions.PORT,
                        AerospikeSinkOptions.NAMESPACE,
                        AerospikeSinkOptions.SET)
                .optional(
                        AerospikeSinkOptions.USERNAME,
                        AerospikeSinkOptions.PASSWORD,
                        AerospikeSinkOptions.KEY_FIELD,
                        AerospikeSinkOptions.BIN_NAME,
                        AerospikeSinkOptions.DATA_FORMAT,
                        AerospikeSinkOptions.WRITE_TIMEOUT)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new AerospikeSink(context.getOptions(), context.getCatalogTable());
    }
}
