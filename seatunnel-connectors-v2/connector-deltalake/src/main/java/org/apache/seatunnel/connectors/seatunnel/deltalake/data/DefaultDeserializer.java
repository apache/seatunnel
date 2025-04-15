package org.apache.seatunnel.connectors.seatunnel.deltalake.data;

import io.delta.kernel.data.FilteredColumnarBatch;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;


@AllArgsConstructor
public class DefaultDeserializer implements Deserializer {
  @NonNull
  private final SeaTunnelRowType seaTunnelRowType;


  @Override
  public SeaTunnelRow deserialize(FilteredColumnarBatch record) {
    return null;
  }
}
