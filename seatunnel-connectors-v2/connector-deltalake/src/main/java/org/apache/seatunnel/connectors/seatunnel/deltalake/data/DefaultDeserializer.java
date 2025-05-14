package org.apache.seatunnel.connectors.seatunnel.deltalake.data;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;


@AllArgsConstructor
public class DefaultDeserializer implements Deserializer {
  @NonNull
  private final SeaTunnelRowType seaTunnelRowType;

  @NonNull
  private final StructType deltaTableSchema;


  @Override
  public SeaTunnelRow deserialize(Row deltaRow) {
    SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
    for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
      String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
      SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);
      StructField deltaField = deltaTableSchema.get(seaTunnelFieldName);
      Object deltaValue = deltaRow.getStruct(i);

      seaTunnelRow.setField(
              i, convert(deltaField.getDataType(), deltaValue, seaTunnelFieldType));
    }
    return seaTunnelRow;
  }
    private Object convert(
            @NonNull io.delta.kernel.types.DataType deltaType,
            Object deltaValue,
            @NonNull SeaTunnelDataType<?> seaTunnelType) {
        if (deltaValue == null) {
        return null;
        }
        if (deltaType instanceof BooleanType) {
            return deltaValue;
        } else if (deltaType instanceof ByteType) {
            return (Byte) deltaValue;
        } else if (deltaType instanceof ShortType) {
            return (Short) deltaValue;
        } else if (deltaType instanceof IntegerType) {
            return (Integer) deltaValue;
        } else if (deltaType instanceof LongType) {
            return (Long) deltaValue;
        } else if (deltaType instanceof FloatType) {
            return (Float) deltaValue;
        } else if (deltaType instanceof DoubleType) {
            return (Double) deltaValue;
        } else if (deltaType instanceof StringType) {
            return (String) deltaValue;
        } else if (deltaType instanceof BinaryType) {
            return (byte[]) deltaValue;
        } else if (deltaType instanceof DateType) {
            return (LocalDate) deltaValue;
        } else if (deltaType instanceof TimestampNTZType
                || deltaType instanceof TimestampType) {
            return (LocalDateTime) deltaValue;
        } else if (deltaType instanceof DecimalType) {
            return (BigDecimal) deltaValue;
        } else if (deltaType instanceof StructType) {
            return (SeaTunnelRow) deltaValue;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + deltaType);
        }
    }
}
