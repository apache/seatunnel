package org.apache.seatunnel.api.table.type;

public class BinaryType implements SeaTunnelDataType<BinaryObject> {

    public static final BinaryType INSTANCE = new BinaryType();

    @Override
    public Class<BinaryObject> getTypeClass() {
        return BinaryObject.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.BINARY;
    }
}
