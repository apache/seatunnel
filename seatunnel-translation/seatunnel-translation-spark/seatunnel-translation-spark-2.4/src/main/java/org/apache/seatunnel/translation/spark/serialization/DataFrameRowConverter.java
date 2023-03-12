package org.apache.seatunnel.translation.spark.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.serialization.RowConverter;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class DataFrameRowConverter extends RowConverter<GenericRowWithSchema> {
    private final StructType structType;

    public DataFrameRowConverter(SeaTunnelDataType<?> dataType) {
        super(dataType);
        structType = (StructType) TypeConverterUtils.convert(dataType);
    }

    @Override
    public GenericRowWithSchema convert(SeaTunnelRow seaTunnelRow) throws IOException {
        return new GenericRowWithSchema(seaTunnelRow.getFields(), structType);
    }

    @Override
    public SeaTunnelRow reconvert(GenericRowWithSchema engineRow) throws IOException {
        return new SeaTunnelRow(engineRow.values());
    }
}
