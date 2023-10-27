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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class CatalogTableParserTest extends BaseConfigParserTest {

    private final String columnConfig = "/conf/catalog/schema_column.conf";
    private final String schemaFieldConfig = "/conf/catalog/schema_field.conf";

    private final String schemasColumnConfig = "/conf/catalog/schemas_column.conf";

    @Test
    void parseSchema() {
        CatalogTableParser catalogTableParser = new CatalogTableParser("testCatalog");
        List<CatalogTable> catalogTables =
                catalogTableParser.parse(getReadonlyConfig(columnConfig));

        Assertions.assertEquals(1, catalogTables.size());
        CatalogTable catalogTable = catalogTables.get(0);
        Assertions.assertEquals("testCatalog", catalogTable.getCatalogName());

        TableSchema tableSchema = catalogTable.getTableSchema();
        Assertions.assertEquals(19, tableSchema.getColumns().size());
        Assertions.assertNotNull(tableSchema.getPrimaryKey());
        Assertions.assertEquals(1, tableSchema.getConstraintKeys().size());
    }

    @Test
    void parseSchemas() {
        CatalogTableParser catalogTableParser = new CatalogTableParser("testCatalog");
        List<CatalogTable> catalogTables =
                catalogTableParser.parse(getReadonlyConfig(schemasColumnConfig));
        Assertions.assertEquals(2, catalogTables.size());

        CatalogTable catalogTable1 = catalogTables.get(0);
        Assertions.assertEquals("testCatalog", catalogTable1.getCatalogName());
        Assertions.assertEquals(
                "testCatalog.database.schema.table1", catalogTable1.getTableId().toString());
        Assertions.assertEquals(1, catalogTable1.getTableSchema().getColumns().size());

        CatalogTable catalogTable2 = catalogTables.get(1);
        Assertions.assertEquals("testCatalog", catalogTable2.getCatalogName());
        Assertions.assertEquals(
                "testCatalog.database.schema.table2", catalogTable2.getTableId().toString());
        Assertions.assertEquals(2, catalogTable2.getTableSchema().getColumns().size());
    }

    @Test
    void parseSchemaFields() {
        CatalogTableParser catalogTableParser = new CatalogTableParser("testCatalog");
        List<CatalogTable> catalogTables =
                catalogTableParser.parse(getReadonlyConfig(schemaFieldConfig));

        Assertions.assertEquals(1, catalogTables.size());
        CatalogTable catalogTable = catalogTables.get(0);
        Assertions.assertEquals("testCatalog", catalogTable.getCatalogName());

        TableSchema tableSchema = catalogTable.getTableSchema();
        Assertions.assertEquals(19, tableSchema.getColumns().size());
        Assertions.assertNotNull(tableSchema.getPrimaryKey());
        Assertions.assertEquals(1, tableSchema.getConstraintKeys().size());
    }
}
