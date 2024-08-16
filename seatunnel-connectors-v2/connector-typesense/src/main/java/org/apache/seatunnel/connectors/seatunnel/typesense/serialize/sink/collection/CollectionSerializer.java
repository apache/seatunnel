package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.collection;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public interface CollectionSerializer {
    String serialize(SeaTunnelRow row);
}
