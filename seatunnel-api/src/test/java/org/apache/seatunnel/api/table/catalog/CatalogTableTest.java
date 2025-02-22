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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class CatalogTableTest {

    @Test
    public void testCatalogTableModifyOptionsOrPartitionKeys() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
        catalogTable.getOptions().put("test", "value");
        catalogTable.getPartitionKeys().add("test");
    }

    @Test
    public void testReadCatalogTableWithUnsupportedType() {
        Catalog catalog =
                new InMemoryCatalogFactory()
                        .createCatalog("InMemory", ReadonlyConfig.fromMap(new HashMap<>()));
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                catalog.getTables(
                                        ReadonlyConfig.fromMap(
                                                new HashMap<String, Object>() {
                                                    {
                                                        put(
                                                                ConnectorCommonOptions.TABLE_NAMES
                                                                        .key(),
                                                                Arrays.asList(
                                                                        "unsupported.public.table1",
                                                                        "unsupported.public.table2"));
                                                    }
                                                })));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-21], ErrorDescription:['InMemory' tables unsupported get catalog table，"
                        + "the corresponding field types in the following tables are not supported: '{\"unsupported.public.table1\""
                        + ":{\"field1\":\"interval\",\"field2\":\"interval2\"},\"unsupported.public.table2\":{\"field1\":\"interval\","
                        + "\"field2\":\"interval2\"}}']",
                exception.getMessage());
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        result.put(
                "unsupported.public.table1",
                new HashMap<String, String>() {
                    {
                        put("field1", "interval");
                        put("field2", "interval2");
                    }
                });
        result.put(
                "unsupported.public.table2",
                new HashMap<String, String>() {
                    {
                        put("field1", "interval");
                        put("field2", "interval2");
                    }
                });
        Assertions.assertEquals(result, exception.getParamsValueAs("tableUnsupportedTypes"));
    }

    @Test
    public void testCatalogTableWithIllegalFieldNames() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "  ", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
        SeaTunnelException exception =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () ->
                                new TableTransformFactoryContext(
                                        Collections.singletonList(catalogTable), null, null));
        SeaTunnelException exception2 =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () -> new TableSinkFactoryContext(catalogTable, null, null));
        Assertions.assertEquals(
                "Table database.table field name cannot be empty", exception.getMessage());
        Assertions.assertEquals(
                "Table database.table field name cannot be empty", exception2.getMessage());

        CatalogTable catalogTable2 =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "name1", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "name1", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
        SeaTunnelException exception3 =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () ->
                                new TableTransformFactoryContext(
                                        Collections.singletonList(catalogTable2), null, null));
        SeaTunnelException exception4 =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () -> new TableSinkFactoryContext(catalogTable2, null, null));
        Assertions.assertEquals(
                "Table database.table field name1 duplicate", exception3.getMessage());
        Assertions.assertEquals(
                "Table database.table field name1 duplicate", exception4.getMessage());
    }
}
