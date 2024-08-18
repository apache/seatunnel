package org.apache.seatunnel.connectors.seatunnel.tablestore.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.alicloud.openservices.tablestore.model.StreamRecord;

public interface SeaTunnelRowDeserializer {

    SeaTunnelRow deserialize(StreamRecord streamRecord);
}
