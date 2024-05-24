package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

class SchemaUtilsTest {

    @Test
    void testToIcebergSchemaWithPk() {
        String[] fieldNames = new String[] {"id", "name", "description", "weight"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType[] {
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE
                };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, dataTypes);
        List<String> pks = Arrays.asList("id", "name");
        ReadonlyConfig readonlyConfig =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(SinkConfig.TABLE_PRIMARY_KEYS.key(), String.join(",", pks));
                            }
                        });
        Schema schema = SchemaUtils.toIcebergSchema(rowType, readonlyConfig);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(fieldNames.length, schema.columns().size());
        for (Types.NestedField column : schema.columns()) {
            Assertions.assertEquals(fieldNames[column.fieldId() - 1], column.name());
            if (pks.contains(column.name())) {
                Assertions.assertEquals(Boolean.TRUE, column.isRequired());
            } else {
                Assertions.assertEquals(Boolean.FALSE, column.isRequired());
            }
        }
        Assertions.assertNotNull(schema.identifierFieldIds());
        Assertions.assertEquals(pks.size(), schema.identifierFieldIds().size());
        for (Integer identifierFieldId : schema.identifierFieldIds()) {
            Assertions.assertEquals(
                    pks.get(identifierFieldId - 1), fieldNames[identifierFieldId - 1]);
        }
    }

    @Test
    void testToIcebergSchemaWithoutPk() {
        String[] fieldNames = new String[] {"id", "name", "description", "weight"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType[] {
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE
                };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, dataTypes);
        ReadonlyConfig readonlyConfig =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                            }
                        });
        Schema schema = SchemaUtils.toIcebergSchema(rowType, readonlyConfig);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(fieldNames.length, schema.columns().size());
        for (Types.NestedField column : schema.columns()) {
            Assertions.assertEquals(fieldNames[column.fieldId() - 1], column.name());
            Assertions.assertEquals(Boolean.FALSE, column.isRequired());
        }
    }
}
