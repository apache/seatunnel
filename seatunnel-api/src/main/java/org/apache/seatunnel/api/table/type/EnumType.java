package org.apache.seatunnel.api.table.type;

public class EnumType<T> implements DataType<T> {
    private final Class<T> typeClass;

    public EnumType(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    public Class<T> getTypeClass() {
        return typeClass;
    }
}
