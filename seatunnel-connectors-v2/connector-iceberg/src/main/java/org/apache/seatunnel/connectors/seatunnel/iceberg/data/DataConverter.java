package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.iceberg.data.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public interface DataConverter {

    SeaTunnelRow toSeaTunnelRowStruct(Record record);

    Record toIcebergStruct(SeaTunnelRow row);

}
