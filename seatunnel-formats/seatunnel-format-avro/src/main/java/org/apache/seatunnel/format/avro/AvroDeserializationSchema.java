package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;

public class AvroDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        return null;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return null;
    }
}
