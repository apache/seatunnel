package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public class StarRocksBaseSerializer {

    protected static String convert(SeaTunnelDataType dataType,
                                  Object val) {
        if (val == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case STRING:
                return (String) val;
            case TINYINT:
            case SMALLINT:
                return String.valueOf(((Number) val).shortValue());
            case INT:
                return String.valueOf(((Number) val).intValue());
            case BIGINT:
                return String.valueOf(((Number) val).longValue());
            case FLOAT:
                return String.valueOf(((Number) val).floatValue());
            case DOUBLE:
                return String.valueOf(((Number) val).doubleValue());
            case BOOLEAN:
                return String.valueOf((Long) val);
            case BYTES:
                byte[] bts = (byte[]) val;
                long value = 0;
                for (int i = 0; i < bts.length; i++) {
                    value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
                }
                return String.valueOf(value);
            default:
                throw new UnsupportedOperationException("Unsupported dataType: " + dataType);
        }
    }
}