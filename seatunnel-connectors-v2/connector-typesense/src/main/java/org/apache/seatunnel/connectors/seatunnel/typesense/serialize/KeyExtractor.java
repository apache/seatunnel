package org.apache.seatunnel.connectors.seatunnel.typesense.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.typesense.exception.TypesenseConnectorException;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@AllArgsConstructor
public class KeyExtractor implements Function<SeaTunnelRow, String>, Serializable {
    private final FieldFormatter[] fieldFormatters;
    private final String keyDelimiter;

    @Override
    public String apply(SeaTunnelRow row) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < fieldFormatters.length; i++) {
            if (i > 0) {
                builder.append(keyDelimiter);
            }
            String value = fieldFormatters[i].format(row);
            builder.append(value);
        }
        return builder.toString();
    }

    public static Function<SeaTunnelRow, String> createKeyExtractor(
            SeaTunnelRowType rowType, String[] primaryKeys, String keyDelimiter) {
        if (primaryKeys == null) {
            return row -> null;
        }

        List<FieldFormatter> fieldFormatters = new ArrayList<>(primaryKeys.length);
        for (String fieldName : primaryKeys) {
            int fieldIndex = rowType.indexOf(fieldName);
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(fieldIndex);
            FieldFormatter fieldFormatter = createFieldFormatter(fieldIndex, fieldType);
            fieldFormatters.add(fieldFormatter);
        }
        return new KeyExtractor(fieldFormatters.toArray(new FieldFormatter[0]), keyDelimiter);
    }

    private static FieldFormatter createFieldFormatter(
            int fieldIndex, SeaTunnelDataType fieldType) {
        return row -> {
            switch (fieldType.getSqlType()) {
                case ROW:
                case ARRAY:
                case MAP:
                    throw new TypesenseConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            "Unsupported type: " + fieldType);
                case DATE:
                    LocalDate localDate = (LocalDate) row.getField(fieldIndex);
                    return localDate.toString();
                case TIME:
                    LocalTime localTime = (LocalTime) row.getField(fieldIndex);
                    return localTime.toString();
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) row.getField(fieldIndex);
                    return localDateTime.toString();
                default:
                    return row.getField(fieldIndex).toString();
            }
        };
    }

    private interface FieldFormatter extends Serializable {
        String format(SeaTunnelRow row);
    }
}
