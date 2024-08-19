package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public class FixedValueCollectionSerializer implements CollectionSerializer {

    private final String index;

    public FixedValueCollectionSerializer(String index) {
        this.index = index;
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        return index;
    }
}
