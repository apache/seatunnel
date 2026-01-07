package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.IdentityFlatMapTransform;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class SQLMultiCatalogFlatMapTransformTest {

    @Test
    void testCreateIdentityTransform() {
        String tableName = "test";
        String[] fields = new String[] {"id", "int", "string"};
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        tableName,
                        "test",
                        "test",
                        "test",
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.INT_TYPE, BasicType.STRING_TYPE
                                }));
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                TransformCommonOptions.TABLE_MATCH_REGEX.key(), ".exclude"));

        FakeSQLMultiCatalogFlatMapTransform transform =
                new FakeSQLMultiCatalogFlatMapTransform(Collections.singletonList(table), config);

        Assertions.assertInstanceOf(
                IdentityFlatMapTransform.class,
                transform.getTransformMap().get(table.getTableId().toTablePath().toString()));
    }

    private static class FakeSQLMultiCatalogFlatMapTransform
            extends SQLMultiCatalogFlatMapTransform {

        private FakeSQLMultiCatalogFlatMapTransform(
                List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
            super(inputCatalogTables, config);
        }

        private Map<String, SeaTunnelTransform<SeaTunnelRow>> getTransformMap() {
            return this.transformMap;
        }
    }
}
