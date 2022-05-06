package org.apache.seatunnel.api.table.type;

import java.util.Map;

public class MapType<K, V> implements DataType<Map<K, V>> {

    private final DataType<K> keyType;
    private final DataType<V> valueType;

    public MapType(DataType<K> keyType, DataType<V> valueType) {
        if (keyType == null) {
            throw new IllegalArgumentException("keyType cannot be null");
        }
        if (valueType == null) {
            throw new IllegalArgumentException("valueType cannot be null");
        }
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public DataType<K> getKeyType() {
        return keyType;
    }

    public DataType<V> getValueType() {
        return valueType;
    }

}
