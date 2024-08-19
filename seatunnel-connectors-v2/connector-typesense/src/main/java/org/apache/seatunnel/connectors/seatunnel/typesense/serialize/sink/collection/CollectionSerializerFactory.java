package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection;

public class CollectionSerializerFactory {

    public static CollectionSerializer getIndexSerializer(String index) {
        return new FixedValueCollectionSerializer(index);
    }
}
