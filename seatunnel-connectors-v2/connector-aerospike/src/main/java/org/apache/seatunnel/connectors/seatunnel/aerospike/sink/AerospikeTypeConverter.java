package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeConfig;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeDataType;

import java.util.HashMap;
import java.util.Map;

public class AerospikeTypeConverter {

    private final Map<String, AerospikeDataType> fieldTypeMapping;

    public AerospikeTypeConverter(SeaTunnelRowType rowType, ReadonlyConfig config) {
        this.fieldTypeMapping = new HashMap<>();
        Map<String, String> configFieldTypes =
                config.getOptional(AerospikeConfig.FIELD_TYPES).orElse(new HashMap<>());
        String[] fieldNames = rowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();

        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            if (configFieldTypes.containsKey(fieldName)) {
                fieldTypeMapping.put(
                        fieldName, AerospikeDataType.valueOf(configFieldTypes.get(fieldName)));
            } else {
                fieldTypeMapping.put(fieldName, convertSeaTunnelType(fieldTypes[i]));
            }
        }
    }

    private AerospikeDataType convertSeaTunnelType(SeaTunnelDataType<?> seaTunnelType) {
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
                return AerospikeDataType.MAP;
            case ARRAY:
                return AerospikeDataType.LIST;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported SeaTunnel data type: " + seaTunnelType);
        }
    }

    public AerospikeDataType getFieldType(String fieldName) {
        return fieldTypeMapping.get(fieldName);
    }
}
