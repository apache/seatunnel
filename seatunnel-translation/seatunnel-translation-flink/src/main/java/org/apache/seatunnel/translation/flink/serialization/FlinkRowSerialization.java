package org.apache.seatunnel.translation.flink.serialization;

import org.apache.flink.types.Row;
import org.apache.seatunnel.translation.serialization.RowSerialization;

import java.io.IOException;

public class FlinkRowSerialization implements RowSerialization<Row> {

    @Override
    public Row serialize(org.apache.seatunnel.api.table.type.Row seaTunnelRow) throws IOException {
        return null;
    }

    @Override
    public org.apache.seatunnel.api.table.type.Row deserialize(Row engineRow) throws IOException {
        return null;
    }
}
