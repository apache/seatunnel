package org.apache.seatunnel.format.json.kafka.connect;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.DeserializationFormat;
import org.apache.seatunnel.api.table.connector.SerializationFormat;
import org.apache.seatunnel.api.table.factory.DeserializationFormatFactory;
import org.apache.seatunnel.api.table.factory.SerializationFormatFactory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;

/**
 * kafka connect format factory
 */
public class KafkaConnectFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    @Override public DeserializationFormat createDeserializationFormat(TableFactoryContext context) {
        return null;
    }

    @Override public String factoryIdentifier() {
        return null;
    }

    @Override public OptionRule optionRule() {
        return null;
    }

    @Override public SerializationFormat createSerializationFormat(TableFactoryContext context) {
        return null;
    }
}
