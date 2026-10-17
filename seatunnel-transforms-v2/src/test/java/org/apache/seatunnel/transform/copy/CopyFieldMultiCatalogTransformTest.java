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

package org.apache.seatunnel.transform.copy;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class CopyFieldMultiCatalogTransformTest {

    @Test
    void allMatchModeAppliesCopyRulesInDeclarationOrder() {
        CatalogTable table = catalogTable();
        String tablePath = table.getTableId().toTablePath().toString();
        CopyFieldMultiCatalogTransform transform =
                new CopyFieldMultiCatalogTransform(
                        Collections.singletonList(table), allMatchConfig(tablePath));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "name-value"});
        row.setTableId(tablePath);

        SeaTunnelRow output = transform.map(row);

        Assertions.assertEquals(
                Arrays.asList("id", "name", "name2", "name3"), fieldNames(transform));
        Assertions.assertEquals(4, output.getArity());
        Assertions.assertEquals("name-value", output.getField(2));
        Assertions.assertEquals("name-value", output.getField(3));
    }

    /**
     * Verifies that Copy accepts JSON logical fields without changing their String value or type.
     *
     * <p>Native JSON source pipelines can therefore retain JSON semantics after copying a field.
     */
    @Test
    void copyJsonFieldPreservesTypeAndValue() {
        String json = "{\"id\":1,\"nested\":[true,2]}";
        CatalogTable table =
                CatalogTableUtil.getCatalogTable(
                        "catalog",
                        "database",
                        "schema",
                        "json_table",
                        new SeaTunnelRowType(
                                new String[] {"payload"},
                                new SeaTunnelDataType[] {BasicType.JSON_TYPE}));
        Map<String, Object> config = new HashMap<>();
        config.put("fields", Collections.singletonMap("payload_copy", "payload"));
        CopyFieldTransform transform =
                new CopyFieldTransform(
                        CopyTransformConfig.of(ReadonlyConfig.fromMap(config)), table);
        SeaTunnelRowType outputType = transform.getProducedCatalogTable().getSeaTunnelRowType();

        SeaTunnelRow output = transform.map(new SeaTunnelRow(new Object[] {json}));

        Assertions.assertEquals(BasicType.JSON_TYPE, outputType.getFieldType(1));
        Assertions.assertEquals(json, output.getField(1));
    }

    private static ReadonlyConfig allMatchConfig(String tablePath) {
        Map<String, Object> firstRule = copyRule(tablePath, "name", "name2");
        Map<String, Object> secondRule = copyRule(tablePath, "name2", "name3");
        Map<String, Object> config = new HashMap<>();
        config.put(
                TransformCommonOptions.RULE_MATCH_MODE.key(),
                TransformCommonOptions.RuleMatchMode.ALL_MATCH);
        config.put(TransformCommonOptions.MULTI_TABLES.key(), Arrays.asList(firstRule, secondRule));
        return ReadonlyConfig.fromMap(config);
    }

    private static Map<String, Object> copyRule(
            String tablePath, String srcField, String destField) {
        Map<String, Object> rule = new HashMap<>();
        rule.put(TransformCommonOptions.TABLE_PATH.key(), tablePath);
        rule.put(CopyTransformConfig.SRC_FIELD.key(), srcField);
        rule.put(CopyTransformConfig.DEST_FIELD.key(), destField);
        return rule;
    }

    private static CatalogTable catalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        return CatalogTableUtil.getCatalogTable("catalog", "database", "schema", "orders", rowType);
    }

    private static List<String> fieldNames(CopyFieldMultiCatalogTransform transform) {
        return transform.getProducedCatalogTable().getTableSchema().getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }
}
