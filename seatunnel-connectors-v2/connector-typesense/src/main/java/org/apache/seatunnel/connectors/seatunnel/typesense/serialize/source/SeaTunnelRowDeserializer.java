package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.source;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public interface SeaTunnelRowDeserializer {

    SeaTunnelRow deserialize(TypesenseRecord rowRecord);
}
