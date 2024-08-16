package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.List;

public class CollectionSerializerFactory {

    public static CollectionSerializer getIndexSerializer(
            String index) {
        return new FixedValueCollectionSerializer(index);
    }

}
