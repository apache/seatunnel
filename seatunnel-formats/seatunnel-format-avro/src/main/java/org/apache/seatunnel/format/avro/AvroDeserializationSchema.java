package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Builder
public class AvroDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private final SeaTunnelRowType seaTunnelRowType;

    private final Schema schema;

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(message), datumReader);
        GenericRecord avroRecord = dataFileReader.next();
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);

            Schema.Field avroField = schema.getField(seaTunnelFieldName);
            Object avroValue = avroRecord.get(seaTunnelFieldName);

            seaTunnelRow.setField(i, convert(avroField.schema(), avroValue, seaTunnelFieldType));
        }
        return seaTunnelRow;

    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    private Object convert(@NonNull Schema schema,
                           Object avroValue,
                           @NonNull SeaTunnelDataType<?> seaTunnelType) {
        if (avroValue == null) {
            return null;
        }
        switch (schema.getType()) {
            case BOOLEAN:
                return Boolean.class.cast(avroValue);
            case INT:
                return Integer.class.cast(avroValue);
            case LONG:
                return Long.class.cast(avroValue);
            case FLOAT:
                return Float.class.cast(avroValue);
            case DOUBLE:
                return Double.class.cast(avroValue);
            case STRING:
                return String.class.cast(avroValue);
            case FIXED:
                return byte[].class.cast(avroValue);
            case BYTES:
                return ByteBuffer.class.cast(avroValue).array();
            case RECORD:
                GenericData.Record avroStruct = GenericData.Record.class.cast(avroValue);
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) seaTunnelType;
                SeaTunnelRow seatunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
                for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                    String seatunnelFieldName = seaTunnelRowType.getFieldName(i);
                    Object seatunnelFieldValue = convert(avroStruct.getSchema(),
                        avroStruct.getSchema().getField(seatunnelFieldName),
                        seaTunnelRowType.getFieldType(i));
                    seatunnelRow.setField(i, seatunnelFieldValue);
                }
                return seatunnelRow;
            case ARRAY:
                List avroList = Arrays.asList(avroValue);
                List seatunnelList = new ArrayList(avroList.size());
                ArrayType seatunnelListType = (ArrayType) seaTunnelType;
                for (int i = 0; i < avroList.size(); i++) {
                    seatunnelList.add(convert(schema.getElementType(),
                        avroList.get(i), seatunnelListType.getElementType()));
                }
                return seatunnelList.toArray();
            default:
                throw new UnsupportedOperationException("Unsupported avro type: " + schema);
        }
    }

}
