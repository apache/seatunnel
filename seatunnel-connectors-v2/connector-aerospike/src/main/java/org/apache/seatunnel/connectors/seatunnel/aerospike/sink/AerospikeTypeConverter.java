package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeConfig;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeDataType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.exception.AerospikeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.aerospike.exception.AerospikeErrorCode;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AerospikeTypeConverter {

    private final Map<String, AerospikeDataType> fieldTypeMapping;
    @Getter private final List<String> fieldNames;

    public AerospikeTypeConverter(SeaTunnelRowType rowType, ReadonlyConfig config) {
        this.fieldTypeMapping = new HashMap<>();
        Map<String, String> configFieldTypes = config.get(AerospikeConfig.FIELD_TYPES);

        if (configFieldTypes == null || configFieldTypes.isEmpty()) {
            String[] allFields = rowType.getFieldNames();
            this.fieldNames = Arrays.asList(allFields);
            for (String field : allFields) {
                int index = rowType.indexOf(field);
                SeaTunnelDataType<?> seaTunnelType = rowType.getFieldType(index);
                fieldTypeMapping.put(field, mapSeaTunnelType(seaTunnelType));
            }
        } else {
            this.fieldNames = new ArrayList<>(configFieldTypes.keySet());
            for (String fieldName : configFieldTypes.keySet()) {
                int index = rowType.indexOf(fieldName);
                if (index == -1) {
                    throw new AerospikeConnectorException(
                            AerospikeErrorCode.INVALID_CONFIG,
                            "Field '" + fieldName + "' not found in source data");
                }
                fieldTypeMapping.put(
                        fieldName, AerospikeDataType.valueOf(configFieldTypes.get(fieldName)));
            }
        }
    }

    private AerospikeDataType mapSeaTunnelType(SeaTunnelDataType<?> seaTunnelType) {
        switch (seaTunnelType.getSqlType()) {
            case STRING:
                return AerospikeDataType.STRING;
            case INT:
                return AerospikeDataType.INTEGER;
            case BIGINT:
                return AerospikeDataType.LONG;
            case FLOAT:
                return AerospikeDataType.FLOAT;
            case DOUBLE:
                return AerospikeDataType.DOUBLE;
            case BOOLEAN:
                return AerospikeDataType.BOOLEAN;
            case MAP:
                if (!(seaTunnelType instanceof MapType)) {
                    throw new AerospikeConnectorException(
                            AerospikeErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Invalid MAP type: " + seaTunnelType.getClass().getSimpleName());
                }
                return AerospikeDataType.MAP;
            case ARRAY:
                if (!(seaTunnelType instanceof ArrayType)) {
                    throw new AerospikeConnectorException(
                            AerospikeErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Invalid ARRAY type: " + seaTunnelType.getClass().getSimpleName());
                }
                return AerospikeDataType.LIST;
            case DATE:
            case TIMESTAMP:
                return AerospikeDataType.LONG;
            default:
                throw new AerospikeConnectorException(
                        AerospikeErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported SeaTunnel type: " + seaTunnelType.getSqlType());
        }
    }

    public AerospikeDataType getFieldType(String fieldName) {
        AerospikeDataType type = fieldTypeMapping.get(fieldName);
        if (type == null) {
            throw new AerospikeConnectorException(
                    AerospikeErrorCode.UNSUPPORTED_DATA_TYPE,
                    "No type mapping for field: " + fieldName);
        }
        return type;
    }
}
