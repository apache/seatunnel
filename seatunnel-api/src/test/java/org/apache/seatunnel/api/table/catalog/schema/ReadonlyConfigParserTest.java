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

package org.apache.seatunnel.api.table.catalog.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.List;

class ReadonlyConfigParserTest extends BaseConfigParserTest {

    private static final String COLUMN_CONFIG = "/conf/catalog/schema_column.conf";
    private static final String FIELD_CONFIG = "/conf/catalog/schema_field.conf";

    @Test
    void parseColumn() throws FileNotFoundException, URISyntaxException {
        ReadonlyConfig config = getReadonlyConfig(COLUMN_CONFIG);

        ReadonlyConfigParser readonlyConfigParser = new ReadonlyConfigParser();
        TableSchema tableSchema = readonlyConfigParser.parse(config);
        assertPrimaryKey(tableSchema);
        assertConstraintKey(tableSchema);
        assertColumn(tableSchema, true);
    }

    @Test
    void parseField() throws FileNotFoundException, URISyntaxException {
        ReadonlyConfig config = getReadonlyConfig(FIELD_CONFIG);

        ReadonlyConfigParser readonlyConfigParser = new ReadonlyConfigParser();
        TableSchema tableSchema = readonlyConfigParser.parse(config);
        assertPrimaryKey(tableSchema);
        assertConstraintKey(tableSchema);
        assertColumn(tableSchema, false);
    }

    private void assertPrimaryKey(TableSchema tableSchema) {
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        Assertions.assertEquals("id", primaryKey.getPrimaryKey());
        Assertions.assertEquals("id", primaryKey.getColumnNames().get(0));
    }

    private void assertConstraintKey(TableSchema tableSchema) {
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();
        ConstraintKey constraintKey = constraintKeys.get(0);
        Assertions.assertEquals("id_index", constraintKey.getConstraintName());
        Assertions.assertEquals(
                ConstraintKey.ConstraintType.INDEX_KEY, constraintKey.getConstraintType());
        Assertions.assertEquals("id", constraintKey.getColumnNames().get(0).getColumnName());
        Assertions.assertEquals(
                ConstraintKey.ColumnSortType.ASC,
                constraintKey.getColumnNames().get(0).getSortType());
    }

    private void assertColumn(TableSchema tableSchema, boolean comeFromColumnConfig) {
        List<Column> columns = tableSchema.getColumns();
        Assertions.assertEquals(19, columns.size());

        Assertions.assertEquals("id", columns.get(0).getName());

        Assertions.assertEquals("map", columns.get(1).getName());
        Assertions.assertEquals(
                "map<string, map<string, string>>",
                columns.get(1).getDataType().toString().toLowerCase());

        Assertions.assertEquals("map_array", columns.get(2).getName());
        Assertions.assertEquals(
                "map<string, map<string, array<int>>>",
                columns.get(2).getDataType().toString().toLowerCase());

        Assertions.assertEquals("array", columns.get(3).getName());
        Assertions.assertEquals(
                "array<tinyint>", columns.get(3).getDataType().toString().toLowerCase());

        Assertions.assertEquals("string", columns.get(4).getName());
        Assertions.assertEquals("string", columns.get(4).getDataType().toString().toLowerCase());

        Assertions.assertEquals("row", columns.get(18).getName());
        Assertions.assertEquals(SqlType.ROW, columns.get(18).getDataType().getSqlType());

        SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) columns.get(18).getDataType();
        Assertions.assertEquals(18, seaTunnelRowType.getTotalFields());

        SeaTunnelRowType seatunnalRowType1 = (SeaTunnelRowType) seaTunnelRowType.getFieldType(17);
        Assertions.assertEquals(17, seatunnalRowType1.getTotalFields());

        if (comeFromColumnConfig) {
            Assertions.assertEquals(0, columns.get(0).getDefaultValue());
            Assertions.assertEquals("I'm default value", columns.get(4).getDefaultValue());
            Assertions.assertEquals(false, columns.get(5).getDefaultValue());
            Assertions.assertEquals(1.1, columns.get(10).getDefaultValue());
            Assertions.assertEquals("2020-01-01", columns.get(15).getDefaultValue());
            Assertions.assertEquals(4294967295L, columns.get(4).getColumnLength());
        }
    }
}
