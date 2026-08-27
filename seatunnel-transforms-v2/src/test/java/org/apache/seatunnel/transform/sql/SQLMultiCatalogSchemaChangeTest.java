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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper-level reproduction for {@link SQLMultiCatalogFlatMapTransform}: confirms the fix in
 * {@code AbstractMultiCatalogTransform.mapSchemaChangeEvent} dispatches schema-change events into
 * the inner {@link SQLTransform}, so post-ALTER rows preserve their new column values through a
 * {@code select *}-style query.
 */
public class SQLMultiCatalogSchemaChangeTest {

    private static final TablePath TBL = TablePath.of("ricky_test", "static_inventory");

    private static CatalogTable buildBaseTable() {
        return CatalogTable.of(
                TableIdentifier.of("catalog", TBL),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build(),
                new HashMap<>(),
                new ArrayList<>(),
                "test");
    }

    @Test
    void sqlMultiCatalogWrapperPropagatesSchemaChange() {
        CatalogTable baseTable = buildBaseTable();

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("query", "select * from " + TBL.getTableName());
        ReadonlyConfig config = ReadonlyConfig.fromMap(cfg);

        SQLMultiCatalogFlatMapTransform wrapper =
                new SQLMultiCatalogFlatMapTransform(Collections.singletonList(baseTable), config);

        // Pre-ALTER row through wrapper: 2-col select * → 2 cols out
        SeaTunnelRow preRow = new SeaTunnelRow(new Object[] {1L, "Widget A"});
        preRow.setTableId(TBL.getFullName());
        List<SeaTunnelRow> preOut = wrapper.flatMap(preRow);
        Assertions.assertEquals(1, preOut.size());
        Assertions.assertEquals(2, preOut.get(0).getArity(), "pre-ALTER select *: 2 cols");

        // Live ALTER ADD discount_pct, is_featured
        TableIdentifier tid = baseTable.getTableId();
        AlterTableColumnsEvent alter =
                new AlterTableColumnsEvent(tid)
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        tid,
                                        PhysicalColumn.of(
                                                "discount_pct",
                                                BasicType.DOUBLE_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null)))
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        tid,
                                        PhysicalColumn.of(
                                                "is_featured",
                                                BasicType.BOOLEAN_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null)));

        wrapper.mapSchemaChangeEvent(alter);

        // Post-ALTER row through wrapper: 4-col select * → 4 cols out (all values preserved)
        SeaTunnelRow postRow =
                new SeaTunnelRow(new Object[] {2L, "Premium A", 10.00d, Boolean.TRUE});
        postRow.setTableId(TBL.getFullName());
        List<SeaTunnelRow> postOut = wrapper.flatMap(postRow);

        Assertions.assertEquals(1, postOut.size());
        SeaTunnelRow postSql = postOut.get(0);
        Assertions.assertEquals(
                4,
                postSql.getArity(),
                "post-ALTER select * MUST be 4 cols (id, name, discount_pct, is_featured). "
                        + "If 2, the wrapper swallowed ALTER and inner SQLTransform's sqlEngine "
                        + "still has the old 2-col schema.");
        Assertions.assertEquals(2L, postSql.getField(0));
        Assertions.assertEquals("Premium A", postSql.getField(1));
        Assertions.assertEquals(10.00d, postSql.getField(2), "discount_pct preserved");
        Assertions.assertEquals(Boolean.TRUE, postSql.getField(3), "is_featured preserved");
    }
}
