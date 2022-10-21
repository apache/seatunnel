package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.Serializable;

public interface StarRocksISerializer extends Serializable {

    String serialize(SeaTunnelRow seaTunnelRow);
    
}
