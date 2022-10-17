package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public class AvroSerializationSchema implements SerializationSchema {
    @Override
    public byte[] serialize(SeaTunnelRow element) {
        return new byte[0];
    }
}
