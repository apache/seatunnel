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

package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

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
                                put(
                                        IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(),
                                        String.join(",", pks));
                            }
                        });
        Schema schema =
                SchemaUtils.toIcebergSchema(
                        CatalogTableUtil.getCatalogTable("default", rowType).getTableSchema(),
                        readonlyConfig);
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
        Schema schema =
                SchemaUtils.toIcebergSchema(
                        CatalogTableUtil.getCatalogTable("default", rowType).getTableSchema(),
                        readonlyConfig);
        Assertions.assertNotNull(schema);
        Assertions.assertEquals(fieldNames.length, schema.columns().size());
        for (Types.NestedField column : schema.columns()) {
            Assertions.assertEquals(fieldNames[column.fieldId() - 1], column.name());
            Assertions.assertEquals(Boolean.FALSE, column.isRequired());
        }
        Assertions.assertTrue(
                schema.identifierFieldIds().isEmpty(),
                "identifier-field-ids must be empty when iceberg.table.primary-keys is not configured");
    }

    /**
     * Regression test for the MySQL PK fallback bug.
     *
     * <p>Before the fix, {@code SchemaUtils.toIcebergSchema()} fell back to {@code
     * tableSchema.getPrimaryKey()} when {@code iceberg.table.primary-keys} was not set, silently
     * copying the CDC source table PK into {@code identifier-field-ids}. This activated {@code
     * BaseEqualityDeltaWriter}, which emits positional deletes for repeated keys within the same
     * checkpoint window, causing silent INSERT data loss in append-only CDC pipelines.
     *
     * <p>After the fix, {@code identifier-field-ids} must remain empty when the config key is
     * absent, regardless of whether the source {@link TableSchema} carries a primary key.
     */
    @Test
    void testToIcebergSchemaDoesNotInheritSourceTablePkWhenNotConfigured() {
        String[] fieldNames = new String[] {"id", "name", "value"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType[] {
                    BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                };

        // Build a TableSchema that has a primary key on "id" — simulating a CDC source table
        // (e.g. MySQL) whose PK would previously be silently inherited as identifier-field-ids.
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, (Long) null, false, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, (Long) null, true, null, null),
                        PhysicalColumn.of(
                                "value", BasicType.STRING_TYPE, (Long) null, true, null, null));
        PrimaryKey sourcePk = PrimaryKey.of("pk_id", Collections.singletonList("id"));
        TableSchema tableSchemaWithPk =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(sourcePk)
                        .constraintKey(Collections.emptyList())
                        .build();

        // No iceberg.table.primary-keys in config — append-only CDC pipeline scenario
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(new HashMap<>());

        Schema schema = SchemaUtils.toIcebergSchema(tableSchemaWithPk, readonlyConfig);

        Assertions.assertNotNull(schema);
        Assertions.assertEquals(fieldNames.length, schema.columns().size());
        // identifier-field-ids must be empty — source PK must NOT be inherited
        Assertions.assertTrue(
                schema.identifierFieldIds().isEmpty(),
                "identifier-field-ids must be empty when iceberg.table.primary-keys is not "
                        + "configured, even if the source TableSchema has a primary key. "
                        + "Inheriting the source PK silently activates BaseEqualityDeltaWriter "
                        + "and causes positional deletes in append-only CDC pipelines.");
    }

    /**
     * Guard against the regression introduced by the SchemaUtils PK-fallback fix: when {@code
     * iceberg.table.upsert-mode-enabled=true} is set without an explicit {@code
     * iceberg.table.primary-keys}, the sink must fail fast with a clear error rather than silently
     * creating a delta writer with an empty identifier-field set (which would produce broken upsert
     * semantics).
     */
    @Test
    void testIcebergSinkConfigThrowsWhenUpsertModeEnabledWithoutPrimaryKeys() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(IcebergSinkOptions.TABLE_UPSERT_MODE_ENABLED_PROP.key(), true);
                                // iceberg.table.primary-keys deliberately absent
                            }
                        });
        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new IcebergSinkConfig(config),
                        "IcebergSinkConfig must reject upsert-mode-enabled=true without an "
                                + "explicit iceberg.table.primary-keys configuration");
        Assertions.assertTrue(
                ex.getMessage().contains(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key()),
                "Error message should name the missing config key");
    }

    /**
     * Regression test for the catalog-path CatalogTable PK fallback.
     *
     * <p>{@code SchemaUtils.autoCreateTable(Catalog, TablePath, CatalogTable, ReadonlyConfig)}
     * overload 1 falls back to the CatalogTable's own primary key when {@code
     * iceberg.table.primary-keys} is not in config. This is intentionally different from the CDC
     * sink path ({@code toIcebergSchema(TableSchema, ReadonlyConfig)}) which must NOT inherit the
     * source PK (see apache/seatunnel#10747).
     *
     * <p>This test exercises the schema-building half of that overload directly via the
     * package-visible {@code toIcebergSchema(TableSchema, List)} overload to verify that the PK
     * fallback correctly sets identifier-field-ids.
     */
    @Test
    void testAutoCreateTableCatalogPathPreservesCatalogTablePk() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, (Long) null, false, null, null),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, (Long) null, true, null, null),
                        PhysicalColumn.of(
                                "value", BasicType.STRING_TYPE, (Long) null, true, null, null));
        PrimaryKey catalogPk = PrimaryKey.of("catalog_pk", Collections.singletonList("id"));
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(catalogPk)
                        .constraintKey(Collections.emptyList())
                        .build();

        // No TABLE_PRIMARY_KEYS in config — autoCreateTable overload 1 falls back to CatalogTable
        // PK.
        ReadonlyConfig configWithoutPkKey = ReadonlyConfig.fromMap(new HashMap<>());

        // Replicate the catalog-path PK resolution logic from autoCreateTable overload 1.
        List<String> pkColumns =
                configWithoutPkKey
                        .getOptional(IcebergSinkOptions.TABLE_PRIMARY_KEYS)
                        .map(e -> IcebergSinkConfig.stringToList(e, ","))
                        .orElseGet(
                                () ->
                                        Optional.ofNullable(tableSchema.getPrimaryKey())
                                                .map(PrimaryKey::getColumnNames)
                                                .orElse(Collections.emptyList()));

        // Call the package-private core method directly (same call autoCreateTable uses).
        Schema schema = SchemaUtils.toIcebergSchema(tableSchema, pkColumns);

        Assertions.assertFalse(
                schema.identifierFieldIds().isEmpty(),
                "autoCreateTable catalog path must preserve CatalogTable PK into "
                        + "identifier-field-ids when iceberg.table.primary-keys is absent");
        Assertions.assertEquals(
                1,
                schema.identifierFieldIds().size(),
                "Only the single CatalogTable PK column (id) should be an identifier field");
        Types.NestedField idField = schema.findField("id");
        Assertions.assertNotNull(idField, "Field 'id' must exist in schema");
        Assertions.assertTrue(
                schema.identifierFieldIds().contains(idField.fieldId()),
                "id field must be in identifier-field-ids");
        Assertions.assertTrue(idField.isRequired(), "PK field 'id' must be marked required");

        // Non-PK fields must remain optional.
        Assertions.assertFalse(schema.findField("name").isRequired());
        Assertions.assertFalse(schema.findField("value").isRequired());
    }
}
