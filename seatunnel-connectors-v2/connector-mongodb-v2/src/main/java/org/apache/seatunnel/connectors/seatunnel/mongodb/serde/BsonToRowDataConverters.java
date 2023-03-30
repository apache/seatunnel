package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.bson.Document;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

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
                return createArrayConverter((ArrayType<?, ?>) type);
            case MAP:
                return createMapConverter((MapType<?, ?>) type);
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
            Object[] fieldValue = document.values().toArray();
            for (int i = 0; i < fieldCount; ++i) {
                Object o = fieldValue[i];
                containerRow.setField(i, fieldConverters[i].convert(null, o));
            }
            return containerRow;
        };
    }

    private BsonToRowDataConverter createArrayConverter(ArrayType<?, ?> type) {
        BsonToRowDataConverter valueConverter = createConverter(type.getElementType());
        return (reuse, value) -> {
            JsonNode jsonNode = (JsonNode)value;
                Object arr =
                        Array.newInstance(type.getElementType().getTypeClass(), jsonNode.size());
                for (int i = 0; i < jsonNode.size(); i++) {
                    Array.set(arr, i, valueConverter.convert(null, jsonNode.get(i)));
                }
                return arr;
            };
        }

    private BsonToRowDataConverter createMapConverter(MapType<?, ?> type) {
        BsonToRowDataConverter valueConverter = createConverter(type.getValueType());
        return (reuse, value) -> {
                Map<Object, Object> map = new HashMap<>();
                Document document = (Document) value;
                for (String key : document.keySet()) {
                    map.put(
                            key,
                            valueConverter.convert(null, document.get(key)));
                }
                return map;
            };
    }

    private BsonToRowDataConverter wrapIntoNullableConverter(BsonToRowDataConverter converter) {
        return (reuse, object) -> object == null ? null : converter.convert(reuse, object);
    }

    public interface BsonToRowDataConverter extends Serializable {
        Object convert(Object reusedContainer, Object value);
    }
}
