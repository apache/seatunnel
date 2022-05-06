package org.apache.seatunnel.api.table.type;

public class ArrayType<T> implements DataType<T> {

    private final BasicType<T> elementType;

    public ArrayType(BasicType<T> elementType) {
        this.elementType = elementType;
    }

    public BasicType<T> getElementType() {
        return elementType;
    }

}
