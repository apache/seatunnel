/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.common.IdentityFlatMapTransform;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class SQLMultiCatalogFlatMapTransformTest {

    @Test
    void testGetPluginNameAndBuildTransform() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("test", "test", "test", "test", rowType);
        List<CatalogTable> tables = Collections.singletonList(catalogTable);

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                SQLTransform.KEY_QUERY.key(), "select * from dual"));

        SQLMultiCatalogFlatMapTransform transform =
                new SQLMultiCatalogFlatMapTransform(tables, config);

        Assertions.assertEquals(SQLTransform.PLUGIN_NAME, transform.getPluginName());

        SeaTunnelFlatMapTransform<?> inner = transform.buildTransform(catalogTable, config);
        Assertions.assertInstanceOf(SQLTransform.class, inner);
    }

    @Test
    void testCreateIdentityTransform() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("test", "test", "test", "test", rowType);
        List<CatalogTable> tables = Collections.singletonList(catalogTable);
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                TransformCommonOptions.TABLE_MATCH_REGEX.key(), ".exclude"));

        TestSQLMultiCatalogFlatMapTransform transform =
                new TestSQLMultiCatalogFlatMapTransform(tables, config);

        Assertions.assertInstanceOf(
                IdentityFlatMapTransform.class,
                transform
                        .getTransformMap()
                        .get(tables.get(0).getTableId().toTablePath().toString()));
    }

    @Test
    void testClassifyRowError() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("test", "test", "test", "test", rowType);
        SQLMultiCatalogFlatMapTransform transform =
                new SQLMultiCatalogFlatMapTransform(
                        Collections.singletonList(catalogTable),
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap(
                                        SQLTransform.KEY_QUERY.key(), "select * from dual")));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "name"});

        Assertions.assertEquals(
                RowErrorClassification.ROW_ERROR,
                transform.classifyRowError(
                        TransformCommonError.sqlExpressionError(
                                "select * from dual", new RuntimeException("error")),
                        row));
        Assertions.assertEquals(
                RowErrorClassification.ROW_ERROR,
                transform.classifyRowError(
                        new RuntimeException(
                                TransformCommonError.sqlWhereStatementError(
                                        "id > 0", new RuntimeException("error"))),
                        row));
        Assertions.assertEquals(
                RowErrorClassification.SYSTEM_ERROR,
                transform.classifyRowError(
                        TransformCommonError.encryptionError("name", new RuntimeException("error")),
                        row));
        Assertions.assertEquals(
                RowErrorClassification.SYSTEM_ERROR,
                transform.classifyRowError(
                        TransformCommonError.sqlWhereStatementError(
                                "id BETWEEN 1 AND 5",
                                new TransformException(
                                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                                        "Unsupported SQL Expression")),
                        row));
        Assertions.assertEquals(
                RowErrorClassification.SYSTEM_ERROR,
                transform.classifyRowError(new RuntimeException("error"), row));
    }

    private static class TestSQLMultiCatalogFlatMapTransform
            extends SQLMultiCatalogFlatMapTransform {

        private TestSQLMultiCatalogFlatMapTransform(
                List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
            super(inputCatalogTables, config);
        }

        private Map<String, SeaTunnelTransform<SeaTunnelRow>> getTransformMap() {
            return this.transformMap;
        }
    }
}
