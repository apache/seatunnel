package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeConfig;

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
                        AerospikeConfig.HOST,
                        AerospikeConfig.PORT,
                        AerospikeConfig.NAMESPACE,
                        AerospikeConfig.SET)
                .optional(
                        AerospikeConfig.USERNAME,
                        AerospikeConfig.PASSWORD,
                        AerospikeConfig.KEY_FIELD,
                        AerospikeConfig.BIN_NAME,
                        AerospikeConfig.DATA_FORMAT,
                        AerospikeConfig.WRITE_TIMEOUT)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new AerospikeSink(context.getOptions(), context.getCatalogTable());
    }
}
