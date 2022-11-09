package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.source.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

public interface KafkaDeserializationSchema<T> extends Serializable {

    T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception;

    default void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<T> out)
        throws Exception {
        T deserialized = deserialize(message);
        if (deserialized != null) {
            out.collect(deserialized);
        }
    }

}
