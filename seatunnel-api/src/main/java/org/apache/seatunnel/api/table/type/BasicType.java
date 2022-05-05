package org.apache.seatunnel.api.table.type;

public class BasicType<T> implements DataType<T> {

    private final Class<T> typeClass;

    public BasicType(Class<T> typeClass) {
        if (typeClass == null) {
            throw new IllegalArgumentException("typeClass cannot be null");
        }
        this.typeClass = typeClass;
    }

    @Override
    public boolean isBasicType() {
        return true;
    }

    @Override
    public Class<T> getTypeClass() {
        return this.typeClass;
    }
}
