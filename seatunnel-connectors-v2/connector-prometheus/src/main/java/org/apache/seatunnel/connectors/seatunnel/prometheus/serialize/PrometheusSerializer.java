package org.apache.seatunnel.connectors.seatunnel.prometheus.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.prometheus.Exception.PrometheusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.prometheus.write.Point;

import com.google.common.base.Strings;
import lombok.NonNull;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class PrometheusSerializer implements Serializer {

    private final Function<SeaTunnelRow, Long> timestampExtractor;
    private final Function<SeaTunnelRow, Double> valueExtractor;
    private final Function<SeaTunnelRow, Map> labelExtractor;

    public PrometheusSerializer(
            @NonNull SeaTunnelRowType seaTunnelRowType,
            String timestampKey,
            String labelKey,
            String valueKey) {
        this.valueExtractor = createValueExtractor(seaTunnelRowType, valueKey);
        this.timestampExtractor = createTimestampExtractor(seaTunnelRowType, timestampKey);
        this.labelExtractor = createLabelExtractor(seaTunnelRowType, labelKey);
    }

    @Override
    public Point serialize(SeaTunnelRow seaTunnelRow) {
        Long timestamp = timestampExtractor.apply(seaTunnelRow);
        Double value = valueExtractor.apply(seaTunnelRow);
        Map<String, String> label = labelExtractor.apply(seaTunnelRow);
        Point point =
                Point.builder().metricLableMap(label).value(value).timestamp(timestamp).build();

        return point;
    }

    private Function<SeaTunnelRow, Map> createLabelExtractor(
            SeaTunnelRowType seaTunnelRowType, String labelKey) {
        if (Strings.isNullOrEmpty(labelKey)) {
            return row -> new HashMap();
        }
        int labelFieldIndex = seaTunnelRowType.indexOf(labelKey);
        return row -> {
            Object value = row.getField(labelFieldIndex);
            if (value == null) {
                return new HashMap();
            }
            SeaTunnelDataType<?> valueFieldType = seaTunnelRowType.getFieldType(labelFieldIndex);
            switch (valueFieldType.getSqlType()) {
                case MAP:
                    return (Map) value;
                default:
                    throw new PrometheusConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type: " + valueFieldType);
            }
        };
    }

    private Function<SeaTunnelRow, Double> createValueExtractor(
            SeaTunnelRowType seaTunnelRowType, String valueKey) {
        if (Strings.isNullOrEmpty(valueKey)) {
            return row -> Double.NaN;
        }

        int valueFieldIndex = seaTunnelRowType.indexOf(valueKey);
        return row -> {
            Object value = row.getField(valueFieldIndex);
            if (value == null) {
                return Double.NaN;
            }
            SeaTunnelDataType<?> valueFieldType = seaTunnelRowType.getFieldType(valueFieldIndex);
            switch (valueFieldType.getSqlType()) {
                case STRING:
                case INT:
                case FLOAT:
                    return Double.parseDouble((String) value);
                case DOUBLE:
                    return (Double) value;
                default:
                    throw new PrometheusConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type: " + valueFieldType);
            }
        };
    }

    private Function<SeaTunnelRow, Long> createTimestampExtractor(
            SeaTunnelRowType seaTunnelRowType, String timestampKey) {
        if (Strings.isNullOrEmpty(timestampKey)) {
            return row -> System.currentTimeMillis();
        }

        int timestampFieldIndex = seaTunnelRowType.indexOf(timestampKey);
        return row -> {
            Object timestamp = row.getField(timestampFieldIndex);
            if (timestamp == null) {
                return System.currentTimeMillis();
            }
            SeaTunnelDataType<?> timestampFieldType =
                    seaTunnelRowType.getFieldType(timestampFieldIndex);
            switch (timestampFieldType.getSqlType()) {
                case STRING:
                    return Long.parseLong((String) timestamp);
                case TIMESTAMP:
                    return ((LocalDateTime) timestamp)
                            .atZone(ZoneOffset.UTC)
                            .toInstant()
                            .toEpochMilli();
                case BIGINT:
                    return (Long) timestamp;
                default:
                    throw new PrometheusConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type: " + timestampFieldType);
            }
        };
    }
}
