package org.apache.seatunnel.api.table.type;

import java.sql.Timestamp;

public class TimestampType implements SeaTunnelDataType<Timestamp> {

    private final int precision;

    public TimestampType(int precision) {
        this.precision = precision;
    }

    public int getPrecision() {
        return precision;
    }
}
