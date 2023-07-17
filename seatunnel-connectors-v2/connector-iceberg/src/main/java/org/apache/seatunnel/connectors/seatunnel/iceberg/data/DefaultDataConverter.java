package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;

import static org.apache.iceberg.types.Type.TypeID.*;

@RequiredArgsConstructor
@Slf4j
public class DefaultDataConverter implements DataConverter {

    @NonNull
    private final SeaTunnelRowType seaTunnelRowType;

    @NonNull
    private final Schema icebergSchema;

    private final Map<Type.TypeID, Object[]> arrayTypeMap = new HashMap<Type.TypeID, Object[]>() {
        {
            put(BOOLEAN, new Boolean[0]);
            put(INTEGER, new Integer[0]);
            put(LONG, new Long[0]);
            put(FLOAT, new Float[0]);
            put(DOUBLE, new Double[0]);
            put(STRING, new String[0]);
        }
    };

    @Override
    public SeaTunnelRow toSeaTunnelRowStruct(@NonNull Record record) {
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());

        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);
            Types.NestedField icebergField = icebergSchema.findField(seaTunnelFieldName);
            Object icebergValue = record.getField(seaTunnelFieldName);
            seaTunnelRow.setField(i, convertToSeaTunnel(icebergField.type(), icebergValue, seaTunnelFieldType));
        }

        return seaTunnelRow;
    }

    @Override
    public Record toIcebergStruct(SeaTunnelRow row) {
        GenericRecord genericRecord = GenericRecord.create(icebergSchema);

        for (int i = 0; i < row.getArity(); i++) {
            String seaTunnelFieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelFieldType = seaTunnelRowType.getFieldType(i);
            Types.NestedField icebergField = icebergSchema.findField(seaTunnelFieldName);
            Object value = row.getField(i);
            genericRecord.setField(seaTunnelFieldName, convertToIceberg(seaTunnelFieldType, icebergField.type(), value));
        }

        return genericRecord;
    }

    private Object convertToIceberg(@NonNull SeaTunnelDataType<?> seaTunnelType, @NonNull Type icebergType, Object seaTunnelValue) {
        if (Objects.isNull(seaTunnelValue)) {
            return null;
        }

        switch (seaTunnelType.getSqlType()) {
            case STRING:
                return (String) seaTunnelValue;
            case BOOLEAN:
                return (Boolean) seaTunnelValue;
            case INT:
                return (Integer) seaTunnelValue;
            case BIGINT:
                return (Long) seaTunnelValue;
            case FLOAT:
                return (Float) seaTunnelValue;
            case DOUBLE:
                return (Double) seaTunnelValue;
            case DECIMAL:
                return BigDecimal.class.cast(seaTunnelValue);
            case BYTES:
                if (icebergType.typeId() == FIXED) {
                    return (byte[]) seaTunnelValue;
                }
                return ByteBuffer.wrap((byte[]) seaTunnelValue);
            case DATE:
                return (LocalDate) seaTunnelValue;
            case TIME:
                return (LocalTime) seaTunnelValue;
            case TIMESTAMP:
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                if (timestampType.shouldAdjustToUTC()) {
                    ZoneOffset utc = ZoneOffset.UTC;
                    return ((LocalDateTime) seaTunnelValue).atOffset(utc);
                }
                return (LocalDateTime) seaTunnelValue;
            case ROW:
                SeaTunnelRow seaTunnelRow = SeaTunnelRow.class.cast(seaTunnelValue);
                Types.StructType icebergStructType = (Types.StructType) icebergType;
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) seaTunnelType;
                GenericRecord record = GenericRecord.create(icebergStructType);
                for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                    String fieldName = seaTunnelRowType.getFieldName(i);
                    Object fieldValue = convertToIceberg(seaTunnelRowType.getFieldType(i), icebergStructType.fieldType(fieldName), seaTunnelRow.getField(i));
                    record.setField(fieldName, fieldValue);
                }
                return record;
            case ARRAY:
                Object[] seaTunnelList = (Object[]) seaTunnelValue;
                Types.ListType icebergListType = (Types.ListType) icebergType;
                List icebergList = new ArrayList(seaTunnelList.length);
                ArrayType seatunnelListType = (ArrayType) seaTunnelType;
                for (int i = 0; i < seaTunnelList.length; i++) {
                    icebergList.add(convertToIceberg(seatunnelListType.getElementType(), icebergListType.elementType(), seaTunnelList[i]));
                }
                return icebergList;
            case MAP:
                Map<Object, Object> seaTunnelMap = Map.class.cast(seaTunnelValue);
                Types.MapType icebergMapType = (Types.MapType) icebergType;
                Map icebergMap = new HashMap();
                MapType seaTunnelMapType = (MapType) seaTunnelType;
                for (Map.Entry entry : seaTunnelMap.entrySet()) {
                    icebergMap.put(convertToIceberg(seaTunnelMapType.getKeyType(), icebergMapType.keyType(), entry.getKey()), convertToIceberg(seaTunnelMapType.getValueType(), icebergMapType.valueType(), entry.getValue()));
                }
                return icebergMap;
            default:
                throw new UnsupportedOperationException("Unsupported seatunnel type: " + seaTunnelType);
        }
    }

    private Object convertToSeaTunnel(@NonNull Type icebergType, Object icebergValue, @NonNull SeaTunnelDataType<?> seaTunnelType) {
        if (Objects.isNull(icebergValue)) {
            return null;
        }

        switch (icebergType.typeId()) {
            case BOOLEAN:
                return (Boolean) icebergValue;
            case INTEGER:
                return (Integer) icebergValue;
            case LONG:
                return (Long) icebergValue;
            case FLOAT:
                return (Float) icebergValue;
            case DOUBLE:
                return (Double) icebergValue;
            case DATE:
                return (LocalDate) icebergValue;
            case TIME:
                return (LocalTime) icebergValue;
            case TIMESTAMP:
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                if (timestampType.shouldAdjustToUTC()) {
                    return ((OffsetDateTime) icebergValue).toLocalDateTime();
                }
                return (LocalDateTime) icebergValue;
            case STRING:
                return (String) icebergValue;
            case FIXED:
                return (byte[]) icebergValue;
            case BINARY:
                return ((ByteBuffer) icebergValue).array();
            case DECIMAL:
                return (BigDecimal) icebergValue;
            case STRUCT:
                Record icebergStruct = (Record) icebergValue;
                Types.StructType icebergStructType = (Types.StructType) icebergType;
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) seaTunnelType;
                SeaTunnelRow seatunnelRow = new SeaTunnelRow(seaTunnelRowType.getTotalFields());
                for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                    String seatunnelFieldName = seaTunnelRowType.getFieldName(i);
                    Object seatunnelFieldValue = convertToSeaTunnel(icebergStructType.fieldType(seatunnelFieldName), icebergStruct.getField(seatunnelFieldName), seaTunnelRowType.getFieldType(i));
                    seatunnelRow.setField(i, seatunnelFieldValue);
                }
                return seatunnelRow;
            case LIST:
                List icebergList = (List) icebergValue;
                List seatunnelList = new ArrayList();
                Type.TypeID typeID = ((Types.ListType) icebergType).elementType().typeId();
                Types.ListType icebergListType = (Types.ListType) icebergType;
                ArrayType seatunnelListType = (ArrayType) seaTunnelType;
                for (int i = 0; i < icebergList.size(); i++) {
                    seatunnelList.add(convertToSeaTunnel(icebergListType.elementType(), icebergList.get(i), seatunnelListType.getElementType()));
                }
                return seatunnelList.toArray(arrayTypeMap.get(typeID));
            case MAP:
                Map<Object, Object> icebergMap = Map.class.cast(icebergValue);
                Types.MapType icebergMapType = (Types.MapType) icebergType;
                Map seatunnelMap = new HashMap();
                MapType seatunnelMapType = (MapType) seaTunnelType;
                for (Map.Entry entry : icebergMap.entrySet()) {
                    seatunnelMap.put(convertToSeaTunnel(icebergMapType.keyType(), entry.getKey(), seatunnelMapType.getKeyType()), convertToSeaTunnel(icebergMapType.valueType(), entry.getValue(), seatunnelMapType.getValueType()));
                }
                return seatunnelMap;
            default:
                throw new IcebergConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format("Unsupported iceberg type: %s", icebergType));
        }
    }

}
