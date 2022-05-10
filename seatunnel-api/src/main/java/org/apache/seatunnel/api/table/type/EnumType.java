package org.apache.seatunnel.api.table.type;

public class EnumType<T extends Enum<T>> implements SeaTunnelDataType<T> {
    private final Class<T> enumClass;

    public EnumType(Class<T> enumClass) {
        this.enumClass = enumClass;
    }

    public Class<T> getEnumClass() {
        return enumClass;
    }
}
