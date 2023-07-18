package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.iceberg.data.Record;

public interface DataConverter {

    SeaTunnelRow toSeaTunnelRowStruct(Record record);

    Record toIcebergStruct(SeaTunnelRow row);
}
