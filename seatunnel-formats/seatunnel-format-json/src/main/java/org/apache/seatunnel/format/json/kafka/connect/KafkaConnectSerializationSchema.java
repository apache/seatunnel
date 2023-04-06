package org.apache.seatunnel.format.json.kafka.connect;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;


public class KafkaConnectSerializationSchema implements SerializationSchema {
    @Override
    public byte[] serialize(SeaTunnelRow element) {
        return new byte[0];
    }
}
