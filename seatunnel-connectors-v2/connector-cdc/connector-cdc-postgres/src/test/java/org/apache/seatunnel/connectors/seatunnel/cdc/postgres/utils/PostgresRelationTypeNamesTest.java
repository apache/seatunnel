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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresRelationSchemaChangeResolver;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresRelationSchemaRecord;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresRelationTypeNames.PgType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Tests PostgreSQL DDL type spelling and its propagation to schema-change events. */
class PostgresRelationTypeNamesTest {

    private static final String SCHEMA_NAME = "public";
    private static final String TABLE_NAME = "t5";

    private static final int JOB_STATUS_OID = 24776;
    private static final int ORDER_OID = 24782;
    private static final int MOOD_OID = 24788;
    private static final int RANGE_OID = 24796;
    private static final int LOWER_CASE_OID = 24770;
    private static final int GEOMETRY_OID = 18000;
    private static final int ARRAY_OID = 24775;
    private static final int COMPOSITE_OID = 24802;

    @Test
    void shouldUseDdlSpellingForUserDefinedTypes() {
        Table relation =
                relation(
                        column("id", 1, Types.INTEGER, 23, "int4"),
                        column("st2", 2, Types.VARCHAR, JOB_STATUS_OID, "JobStatus"),
                        column("o", 3, Types.VARCHAR, ORDER_OID, "order"),
                        column("m", 4, Types.VARCHAR, MOOD_OID, "mood"),
                        column("r2", 5, Types.OTHER, RANGE_OID, "MyRange"),
                        column("c", 6, Types.VARCHAR, LOWER_CASE_OID, "jobstatus_lc"));

        Table qualified = PostgresRelationTypeNames.qualify(relation, knownTypes());

        Assertions.assertEquals(
                Arrays.asList(
                        "int4",
                        "\"JobStatus\"",
                        "\"order\"",
                        "inv.mood",
                        "\"MyRange\"",
                        "jobstatus_lc"),
                typeNames(qualified));
        Assertions.assertEquals(relation.id(), qualified.id());
        Assertions.assertEquals(
                relation.primaryKeyColumnNames(), qualified.primaryKeyColumnNames());
        Assertions.assertEquals(
                "JobStatus",
                qualified.columnWithName("st2").typeExpression(),
                "the type expression Debezium converts values with is kept");
    }

    @Test
    void shouldKeepBaseTypesCompositesDomainsAndArrays() {
        Map<Long, PgType> types = new HashMap<>(knownTypes());
        types.put((long) GEOMETRY_OID, new PgType("geometry", "b", "public.geometry"));
        types.put((long) ARRAY_OID, new PgType("_JobStatus", "b", "\"JobStatus\"[]"));
        types.put((long) COMPOSITE_OID, new PgType("Address", "c", "\"Address\""));
        types.put(4_294_967_000L, new PgType("IntRange", "r", "\"IntRange\""));
        Table relation =
                relation(
                        column("id", 1, Types.INTEGER, 23, "int4"),
                        column("g", 2, Types.OTHER, GEOMETRY_OID, "geometry"),
                        column("roles", 3, Types.ARRAY, ARRAY_OID, "_JobStatus"),
                        // a domain column reports its base type OID with the domain name
                        column("d", 4, Types.VARCHAR, JOB_STATUS_OID, "status_domain"),
                        // Debezium reports composites as STRUCT, which the sink cannot add
                        column("addr", 5, Types.STRUCT, COMPOSITE_OID, "Address"),
                        // OIDs above 2^31 arrive as negative ints
                        column("p", 6, Types.OTHER, (int) 4_294_967_000L, "IntRange"));

        Table qualified = PostgresRelationTypeNames.qualify(relation, types);

        Assertions.assertEquals(
                Arrays.asList(
                        "int4",
                        "geometry",
                        "_JobStatus",
                        "status_domain",
                        "Address",
                        "\"IntRange\""),
                typeNames(qualified));
    }

    @Test
    void shouldNotQueryWhenAllTypesAreBuiltIn() throws Exception {
        Connection connection = mock(Connection.class);
        Table relation =
                relation(
                        column("id", 1, Types.INTEGER, 23, "int4"),
                        column("name", 2, Types.VARCHAR, 1043, "varchar"));

        Assertions.assertSame(relation, PostgresRelationTypeNames.qualify(connection, relation));
        verifyNoInteractions(connection);
    }

    @Test
    void shouldFailWhenTypeOidIsUnknown() {
        Table relation =
                relation(
                        column("id", 1, Types.INTEGER, 23, "int4"),
                        column("st2", 2, Types.VARCHAR, JOB_STATUS_OID, "JobStatus"));

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> PostgresRelationTypeNames.qualify(relation, Collections.emptyMap()));

        Assertions.assertEquals(
                PostgresConnectorErrorCode.RESOLVE_COLUMN_TYPE_NAME_FAILED,
                exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(
                exception.getMessage().contains("Type OID 24776 of column st2 in table"),
                exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains(SCHEMA_NAME + "." + TABLE_NAME),
                exception.getMessage());
    }

    @Test
    void shouldReachAddColumnEventAsSourceType() {
        Table relation =
                relation(
                        column("id", 1, Types.INTEGER, 23, "int4"),
                        column("st2", 2, Types.VARCHAR, JOB_STATUS_OID, "JobStatus"),
                        column("m", 3, Types.VARCHAR, MOOD_OID, "mood"),
                        column("r2", 4, Types.OTHER, RANGE_OID, "MyRange"));

        AlterTableColumnsEvent event =
                (AlterTableColumnsEvent)
                        new PostgresRelationSchemaChangeResolver()
                                .resolve(
                                        PostgresRelationSchemaRecord.create(
                                                PostgresRelationTypeNames.qualify(
                                                        relation, knownTypes()),
                                                Collections.singletonMap("server", "postgres"),
                                                Collections.singletonMap("lsn", 100L),
                                                "postgres.public.t5"),
                                        Collections.singletonList(cachedTable()));

        List<String> sourceTypes = new ArrayList<>();
        for (AlterTableColumnEvent columnEvent : event.getEvents()) {
            org.apache.seatunnel.api.table.catalog.Column added =
                    ((AlterTableAddColumnEvent) columnEvent).getColumn();
            Assertions.assertEquals(BasicType.STRING_TYPE, added.getDataType(), added.getName());
            sourceTypes.add(added.getSourceType());
        }
        Assertions.assertEquals(
                Arrays.asList("\"JobStatus\"", "inv.mood", "\"MyRange\""), sourceTypes);
    }

    private static Map<Long, PgType> knownTypes() {
        Map<Long, PgType> types = new HashMap<>();
        types.put((long) JOB_STATUS_OID, new PgType("JobStatus", "e", "\"JobStatus\""));
        types.put((long) ORDER_OID, new PgType("order", "e", "\"order\""));
        types.put((long) MOOD_OID, new PgType("mood", "e", "inv.mood"));
        types.put((long) RANGE_OID, new PgType("MyRange", "r", "\"MyRange\""));
        types.put((long) LOWER_CASE_OID, new PgType("jobstatus_lc", "e", "jobstatus_lc"));
        return types;
    }

    private static CatalogTable cachedTable() {
        return CatalogTable.of(
                TableIdentifier.of(null, "postgres", SCHEMA_NAME, TABLE_NAME),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .nullable(false)
                                        .sourceType("int4")
                                        .build())
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null,
                null);
    }

    private static Table relation(Column... columns) {
        return Table.editor()
                .tableId(new TableId(null, SCHEMA_NAME, TABLE_NAME))
                .setPrimaryKeyNames(Collections.singletonList("id"))
                .setColumns(Arrays.asList(columns))
                .create();
    }

    private static Column column(
            String name, int position, int jdbcType, int nativeType, String typeName) {
        return Column.editor()
                .name(name)
                .jdbcType(jdbcType)
                .nativeType(nativeType)
                .type(typeName, typeName)
                .position(position)
                .optional(!"id".equals(name))
                .create();
    }

    private static List<String> typeNames(Table table) {
        return table.columns().stream().map(Column::typeName).collect(Collectors.toList());
    }
}
