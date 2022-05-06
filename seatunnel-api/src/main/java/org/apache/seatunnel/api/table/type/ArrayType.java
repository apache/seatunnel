package org.apache.seatunnel.api.table.type;

public class ArrayType<T> implements DataType<T> {

    private final DataType<T> elementType;

    public ArrayType(DataType<T> elementType) {
        this.elementType = elementType;
    }

    public DataType<T> getElementType() {
        return elementType;
    }

}
