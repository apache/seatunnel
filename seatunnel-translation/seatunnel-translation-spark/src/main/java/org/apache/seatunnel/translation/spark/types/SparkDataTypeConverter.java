package org.apache.seatunnel.translation.spark.types;

import org.apache.seatunnel.api.table.type.Converter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.spark.sql.types.DataType;

public interface SparkDataTypeConverter<T1, T2> extends Converter<T1, T2> {

    /**
     * Convert SeaTunnel {@link SeaTunnelDataType} to flink {@link DataType}.
     *
     * @param seaTunnelDataType SeaTunnel {@link SeaTunnelDataType}
     * @return flink {@link DataType}
     */
    @Override
    T2 convert(T1 seaTunnelDataType);
}
