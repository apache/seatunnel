package org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public interface SeaTunnelRowSerializer {
    String serializeRow(SeaTunnelRow row);

}
