package org.apache.seatunnel.connectors.seatunnel.deltalake.data;

import io.delta.kernel.data.FilteredColumnarBatch;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public interface Deserializer {

  SeaTunnelRow deserialize(FilteredColumnarBatch record);
}
