package org.apache.seatunnel.connectors.seatunnel.deltalake.data;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.iceberg.types.Types;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;


@AllArgsConstructor
public class DefaultDeserializer implements Deserializer {
  @NonNull
  private final SeaTunnelRowType seaTunnelRowType;

  @NonNull
  private final StructType deltaTableSchema;


  @Override
  public SeaTunnelRow deserialize(FilteredColumnarBatch record) {
    SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
    for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
      String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
      SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);
      StructField deltaField = deltaTableSchema.get(seaTunnelFieldName);
      Object deltaValue = record.getData(seaTunnelFieldName);

      seaTunnelRow.setField(
              i, convert(deltaField.getDataType(), deltaValue, seaTunnelFieldType));
    }
    return seaTunnelRow;
  }
}
