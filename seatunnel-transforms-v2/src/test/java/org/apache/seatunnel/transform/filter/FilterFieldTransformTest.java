package org.apache.seatunnel.transform.filter;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FilterFieldTransformTest {

    static List<String> filterKeys = Arrays.asList("key2", "key3");
    static CatalogTable catalogTable;
    static Object[] values;

    @BeforeAll
    static void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "key1",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key2",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key3",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key4",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "key5",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "comment");
        values = new Object[] {"value1", "value2", "value3", "value4", "value5"};
        SeaTunnelRow inputRow = new SeaTunnelRow(values);
    }

    @Test
    void testKeepMode() {
        // default keep mode
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FilterFieldTransformConfig.KEY_FIELDS.key(), filterKeys);

        FilterFieldTransform filterFieldTransform =
                new FilterFieldTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        // test output schema
        TableSchema resultSchema = filterFieldTransform.transformTableSchema();
        Assertions.assertNotNull(resultSchema);
        Assertions.assertEquals(filterKeys.size(), resultSchema.getColumns().size());
        for (int i = 0; i < resultSchema.getColumns().size(); i++) {
            Assertions.assertEquals(filterKeys.get(i), resultSchema.getColumns().get(i).getName());
        }

        // test output row
        SeaTunnelRow input = new SeaTunnelRow(values);
        SeaTunnelRow output = filterFieldTransform.transformRow(input);
        Assertions.assertNotNull(output);
        Assertions.assertEquals(filterKeys.size(), output.getFields().length);
        for (int i = 0; i < resultSchema.getFieldNames().length; i++) {
            Integer originalIndex =
                    catalogTable
                            .getTableSchema()
                            .toPhysicalRowDataType()
                            .indexOf(resultSchema.getFieldNames()[i]);
            // test the row's field value
            Assertions.assertEquals(input.getFields()[originalIndex], output.getFields()[i]);
        }
    }

    @Test
    void testExcludeMode() {
        // exclude mode
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(FilterFieldTransformConfig.KEY_FIELDS.key(), filterKeys);
        configMap.put(FilterFieldTransformConfig.MODE.key(), ExecuteModeEnum.DELETE);
        FilterFieldTransform filterFieldTransform =
                new FilterFieldTransform(ReadonlyConfig.fromMap(configMap), catalogTable);

        // test output schema
        TableSchema resultSchema = filterFieldTransform.transformTableSchema();
        Assertions.assertNotNull(resultSchema);
        Assertions.assertEquals(
                catalogTable.getTableSchema().getColumns().size() - filterKeys.size(),
                resultSchema.getColumns().size());
        for (int i = 0; i < catalogTable.getTableSchema().getFieldNames().length; i++) {
            if (!filterKeys.contains(catalogTable.getTableSchema().getFieldNames()[i])) {
                int finalI = i;
                Assertions.assertTrue(
                        resultSchema.getColumns().stream()
                                .anyMatch(
                                        column ->
                                                column.getName()
                                                        .equals(
                                                                catalogTable.getTableSchema()
                                                                        .getFieldNames()[finalI])));
            }
        }

        // test output row
        SeaTunnelRow input = new SeaTunnelRow(values);
        SeaTunnelRow output = filterFieldTransform.transformRow(input);
        Assertions.assertNotNull(output);
        Assertions.assertEquals(
                catalogTable.getTableSchema().getColumns().size() - filterKeys.size(),
                output.getFields().length);
        for (int i = 0; i < output.getFields().length; i++) {
            if (!filterKeys.contains(catalogTable.getTableSchema().getFieldNames()[i])) {
                Integer originalIndex =
                        catalogTable
                                .getTableSchema()
                                .toPhysicalRowDataType()
                                .indexOf(catalogTable.getTableSchema().getFieldNames()[i]);
                // test the row's field value
                Assertions.assertEquals(input.getFields()[originalIndex], output.getFields()[i]);
            }
        }
    }
}
