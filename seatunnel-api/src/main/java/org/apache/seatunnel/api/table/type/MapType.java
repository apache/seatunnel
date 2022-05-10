package org.apache.seatunnel.api.table.type;

import java.util.Map;

public class MapType<K, V> implements SeaTunnelDataType<Map<K, V>> {

    private final SeaTunnelDataType<K> keyType;
    private final SeaTunnelDataType<V> valueType;

    public MapType(SeaTunnelDataType<K> keyType, SeaTunnelDataType<V> valueType) {
        if (keyType == null) {
            throw new IllegalArgumentException("keyType cannot be null");
        }
        if (valueType == null) {
            throw new IllegalArgumentException("valueType cannot be null");
        }
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public SeaTunnelDataType<K> getKeyType() {
        return keyType;
    }

    public SeaTunnelDataType<V> getValueType() {
        return valueType;
    }

}
