package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.iceberg.data.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 *
 *
 * @author mustard
 * @version 1.0
 * Create by 2023-07-05
 */
public interface DataConverter {

    SeaTunnelRow toSeaTunnelRowStruct(Record record);

    Record toIcebergStruct(SeaTunnelRow row);

}
