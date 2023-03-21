package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.bson.Document;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;


public class BsonToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    public BsonToRowDataConverters() {}

    public BsonToRowDataConverter createConverter(SeaTunnelDataType type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private BsonToRowDataConverter createNotNullConverter(SeaTunnelDataType type) {
        switch (type.getSqlType()) {
            case NULL:
                return (reuse, value) -> null;
            case BOOLEAN:
            case DOUBLE:
            case INT:
            case BIGINT:
                return (reuse, value) -> value;
            case TINYINT:
                return (reuse, value) -> ((Integer) value).byteValue();
            case SMALLINT:
                return (reuse, value) -> ((Integer) value).shortValue();
            case FLOAT:
                return (reuse, value) -> ((Double) value).floatValue();
            case STRING:
                return (reuse, value) -> value.toString();
            case DATE:
                return this.createDateConverter();
            case DECIMAL:
                return (reuse, value) -> ((BigDecimal) value);
            case ARRAY:
                return null;
            case MAP:
                return null;
            case ROW:
                return this.createRowConverter((SeaTunnelRowType) type);
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private BsonToRowDataConverter createDateConverter() {
        return (reuse, value) -> value;
    }


    private BsonToRowDataConverter createRowConverter(SeaTunnelRowType type) {
        String[] fieldNames = type.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = type.getFieldTypes();
        BsonToRowDataConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(BsonToRowDataConverter[]::new);
        int fieldCount = type.getTotalFields();

        return (reuse, value) -> {
            SeaTunnelRow containerRow;
            if (reuse != null) {
                containerRow = (SeaTunnelRow) reuse;
            } else {
                containerRow = new SeaTunnelRow(fieldCount);
            }

            Document document = (Document) value;
            for (int i = 0; i < fieldCount; ++i) {
                String fieldName = fieldNames[i];
                Object fieldValue = document.get(fieldName);
                containerRow.setField(i, fieldConverters[i].convert(null, fieldValue));
            }
            return containerRow;
        };
    }

    private BsonToRowDataConverter wrapIntoNullableConverter(BsonToRowDataConverter converter) {
        return (reuse, object) -> object == null ? null : converter.convert(reuse, object);
    }

    public interface BsonToRowDataConverter extends Serializable {
        Object convert(Object reusedContainer, Object value);
    }
}
