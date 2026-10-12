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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.lob;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.OracleLobUnavailableValueHandling;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleLobUnavailableValueHandlerTest {

    private static final String PLACEHOLDER = "__debezium_unavailable_value";

    @Test
    public void deleteBeforeImageNullsLobPlaceholdersOnly() {
        CatalogTable table = lobTable(true);
        RecordingReselector reselector = new RecordingReselector(null);
        OracleLobUnavailableValueHandler handler = handler(reselector, true);
        SeaTunnelRow row = row(table, RowKind.DELETE);

        handler.handle(sourceRecord("123"), row, Collections.singletonList(table));

        Assertions.assertEquals(11, row.getField(0));
        Assertions.assertEquals(PLACEHOLDER, row.getField(1));
        Assertions.assertNull(row.getField(2));
        Assertions.assertNull(row.getField(3));
        Assertions.assertNull(reselector.schema);
    }

    @Test
    public void updateBeforeImageNullsLobPlaceholders() {
        CatalogTable table = lobTable(true);
        SeaTunnelRow row = row(table, RowKind.UPDATE_BEFORE);

        handler(new RecordingReselector(null), true)
                .handle(sourceRecord("123"), row, Collections.singletonList(table));

        Assertions.assertNull(row.getField(2));
        Assertions.assertNull(row.getField(3));
        Assertions.assertEquals(PLACEHOLDER, row.getField(1));
    }

    @Test
    public void updateAfterImageReselectsPlaceholderLobsByPrimaryKeyAndScn() {
        CatalogTable table = lobTable(true);
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("VAL_CLOB", "real-clob");
        values.put("VAL_BLOB", new byte[] {9, 8});
        RecordingReselector reselector = new RecordingReselector(values);
        SeaTunnelRow row = row(table, RowKind.UPDATE_AFTER);

        handler(reselector, true)
                .handle(sourceRecord("54321"), row, Collections.singletonList(table));

        Assertions.assertEquals("DEBEZIUM", reselector.schema);
        Assertions.assertEquals("LOB_TYPES", reselector.table);
        Assertions.assertEquals("54321", reselector.commitScn);
        Assertions.assertEquals(Collections.singletonList("ID"), reselector.primaryKeyColumns);
        Assertions.assertEquals(11, reselector.primaryKeyValues.get(0));
        Assertions.assertTrue(reselector.lobColumns.contains("VAL_CLOB"));
        Assertions.assertTrue(reselector.lobColumns.contains("VAL_BLOB"));
        Assertions.assertEquals("real-clob", row.getField(2));
        Assertions.assertArrayEquals(new byte[] {9, 8}, (byte[]) row.getField(3));
        Assertions.assertEquals(PLACEHOLDER, row.getField(1));
    }

    @Test
    public void varcharValueEqualToThePlaceholderIsKept() {
        CatalogTable table = lobTable(true);
        SeaTunnelRow row = row(table, RowKind.UPDATE_AFTER);
        row.setField(2, "real-clob");
        row.setField(3, new byte[] {1});
        RecordingReselector reselector =
                new RecordingReselector(Collections.<String, Object>emptyMap());

        handler(reselector, true).handle(sourceRecord("1"), row, Collections.singletonList(table));

        Assertions.assertNull(reselector.schema);
        Assertions.assertEquals(PLACEHOLDER, row.getField(1));
        Assertions.assertEquals("real-clob", row.getField(2));
    }

    @Test
    public void missingRowUsesNullHandlingByDefault() {
        CatalogTable table = lobTable(true);
        RecordingReselector reselector = new RecordingReselector(null);
        SeaTunnelRow row = row(table, RowKind.INSERT);

        handler(reselector, true).handle(sourceRecord("9"), row, Collections.singletonList(table));

        Assertions.assertNull(row.getField(2));
        Assertions.assertNull(row.getField(3));
    }

    @Test
    public void reselectDisabledKeepsPlaceholderWhenConfigured() {
        CatalogTable table = lobTable(true);
        SeaTunnelRow row = row(table, RowKind.UPDATE_AFTER);
        OracleLobUnavailableValueHandler handler =
                new OracleLobUnavailableValueHandler(
                        PLACEHOLDER, OracleLobUnavailableValueHandling.WARN_AND_KEEP, false, null);

        handler.handle(sourceRecord("9"), row, Collections.singletonList(table));

        Assertions.assertEquals(PLACEHOLDER, row.getField(2));
        Assertions.assertArrayEquals(PLACEHOLDER.getBytes(), (byte[]) row.getField(3));
    }

    @Test
    public void failModeThrowsWhenReselectIsImpossible() {
        CatalogTable table = lobTable(false);
        SeaTunnelRow row = row(table, RowKind.UPDATE_AFTER);
        OracleLobUnavailableValueHandler handler =
                new OracleLobUnavailableValueHandler(
                        PLACEHOLDER, OracleLobUnavailableValueHandling.FAIL, true, null);

        Assertions.assertThrows(
                SeaTunnelException.class,
                () -> handler.handle(sourceRecord("9"), row, Collections.singletonList(table)));
        Assertions.assertEquals(PLACEHOLDER, row.getField(2));
    }

    @Test
    public void customPlaceholderDoesNotMatchTheDefaultSentinel() {
        CatalogTable table = lobTable(true);
        SeaTunnelRow row = row(table, RowKind.DELETE);
        OracleLobUnavailableValueHandler handler =
                new OracleLobUnavailableValueHandler(
                        "MISSING", OracleLobUnavailableValueHandling.NULL, false, null);

        handler.handle(sourceRecord(null), row, Collections.singletonList(table));

        Assertions.assertEquals(PLACEHOLDER, row.getField(2));
        row.setField(2, "MISSING");
        handler.handle(sourceRecord(null), row, Collections.singletonList(table));
        Assertions.assertNull(row.getField(2));
    }

    @Test
    public void nullLobOnInsertIsNotReselected() {
        CatalogTable table = lobTable(true);
        RecordingReselector reselector = new RecordingReselector(null);
        SeaTunnelRow row = row(table, RowKind.INSERT);
        row.setField(2, null);
        row.setField(3, new byte[] {1});

        handler(reselector, true)
                .handle(sourceRecord("123"), row, Collections.singletonList(table));

        Assertions.assertNull(reselector.schema);
        Assertions.assertNull(row.getField(2));
        Assertions.assertArrayEquals(new byte[] {1}, (byte[]) row.getField(3));
        Assertions.assertEquals(PLACEHOLDER, row.getField(1));
    }

    @Test
    public void reselectSqlExceptionKeepsPlaceholderWhenConfigured() {
        CatalogTable table = lobTable(true);
        SeaTunnelRow row = row(table, RowKind.UPDATE_AFTER);
        OracleLobUnavailableValueHandler handler =
                new OracleLobUnavailableValueHandler(
                        PLACEHOLDER,
                        OracleLobUnavailableValueHandling.WARN_AND_KEEP,
                        true,
                        throwingReselector());

        handler.handle(sourceRecord("9"), row, Collections.singletonList(table));

        Assertions.assertEquals(PLACEHOLDER, row.getField(2));
        Assertions.assertArrayEquals(PLACEHOLDER.getBytes(), (byte[]) row.getField(3));
    }

    @Test
    public void reselectSqlExceptionNullsPlaceholderWhenConfigured() {
        CatalogTable table = lobTable(true);
        SeaTunnelRow row = row(table, RowKind.INSERT);
        OracleLobUnavailableValueHandler handler =
                new OracleLobUnavailableValueHandler(
                        PLACEHOLDER,
                        OracleLobUnavailableValueHandling.NULL,
                        true,
                        throwingReselector());

        handler.handle(sourceRecord("9"), row, Collections.singletonList(table));

        Assertions.assertNull(row.getField(2));
        Assertions.assertNull(row.getField(3));
    }

    @Test
    public void reselectSqlExceptionFailsOnlyWhenConfigured() {
        CatalogTable table = lobTable(true);
        SeaTunnelRow row = row(table, RowKind.UPDATE_AFTER);
        OracleLobUnavailableValueHandler handler =
                new OracleLobUnavailableValueHandler(
                        PLACEHOLDER,
                        OracleLobUnavailableValueHandling.FAIL,
                        true,
                        throwingReselector());

        Assertions.assertThrows(
                SeaTunnelException.class,
                () -> handler.handle(sourceRecord("9"), row, Collections.singletonList(table)));
        Assertions.assertEquals(PLACEHOLDER, row.getField(2));
    }

    @Test
    public void lobSourceTypeRecognition() {
        Assertions.assertTrue(OracleLobUnavailableValueHandler.isLobSourceType("CLOB"));
        Assertions.assertTrue(OracleLobUnavailableValueHandler.isLobSourceType("nclob"));
        Assertions.assertTrue(OracleLobUnavailableValueHandler.isLobSourceType("BLOB(4000)"));
        Assertions.assertFalse(OracleLobUnavailableValueHandler.isLobSourceType("VARCHAR2"));
        Assertions.assertFalse(OracleLobUnavailableValueHandler.isLobSourceType(null));
    }

    private static OracleLobColumnReselector throwingReselector() {
        return new RecordingReselector(
                null, new SQLException("ORA-00942: table or view does not exist", "42000", 942));
    }

    private static OracleLobUnavailableValueHandler handler(
            OracleLobColumnReselector reselector, boolean reselectEnabled) {
        return new OracleLobUnavailableValueHandler(
                PLACEHOLDER, OracleLobUnavailableValueHandling.NULL, reselectEnabled, reselector);
    }

    private static SeaTunnelRow row(CatalogTable table, RowKind rowKind) {
        SeaTunnelRow row = new SeaTunnelRow(4);
        row.setField(0, 11);
        row.setField(1, PLACEHOLDER);
        row.setField(2, PLACEHOLDER);
        row.setField(3, PLACEHOLDER.getBytes());
        row.setRowKind(rowKind);
        row.setTableId(table.getTablePath().toString());
        return row;
    }

    private static CatalogTable lobTable(boolean withPrimaryKey) {
        TableSchema.Builder schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "ID",
                                        BasicType.INT_TYPE,
                                        9L,
                                        false,
                                        null,
                                        "",
                                        "NUMBER",
                                        Collections.emptyMap()))
                        .column(
                                PhysicalColumn.of(
                                        "VAL_VARCHAR",
                                        BasicType.STRING_TYPE,
                                        100L,
                                        true,
                                        null,
                                        "",
                                        "VARCHAR2",
                                        Collections.emptyMap()))
                        .column(
                                PhysicalColumn.of(
                                        "VAL_CLOB",
                                        BasicType.STRING_TYPE,
                                        0L,
                                        true,
                                        null,
                                        "",
                                        "CLOB",
                                        Collections.emptyMap()))
                        .column(
                                PhysicalColumn.of(
                                        "VAL_BLOB",
                                        PrimitiveByteArrayType.INSTANCE,
                                        0L,
                                        true,
                                        null,
                                        "",
                                        "BLOB",
                                        Collections.emptyMap()));
        if (withPrimaryKey) {
            schema.primaryKey(PrimaryKey.of("pk", Collections.singletonList("ID")));
        }
        return CatalogTable.of(
                TableIdentifier.of("oracle", "ORCLCDB", "DEBEZIUM", "LOB_TYPES"),
                schema.build(),
                Collections.<String, String>emptyMap(),
                Collections.<String>emptyList(),
                "");
    }

    private static SourceRecord sourceRecord(String commitScn) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field("db", Schema.STRING_SCHEMA)
                        .field("schema", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .field("commit_scn", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        Struct source =
                new Struct(sourceSchema)
                        .put("db", "ORCLCDB")
                        .put("schema", "DEBEZIUM")
                        .put("table", "LOB_TYPES");
        if (commitScn != null) {
            source.put("commit_scn", commitScn);
        }
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field("source", sourceSchema)
                        .field("op", Schema.STRING_SCHEMA)
                        .build();
        Struct value = new Struct(valueSchema).put("source", source).put("op", "u");
        return new SourceRecord(
                Collections.emptyMap(), Collections.emptyMap(), "topic", valueSchema, value);
    }

    private static final class RecordingReselector implements OracleLobColumnReselector {
        private final Map<String, Object> values;
        private final SQLException failure;
        private String schema;
        private String table;
        private List<String> lobColumns;
        private List<String> primaryKeyColumns;
        private List<Object> primaryKeyValues;
        private String commitScn;

        private RecordingReselector(Map<String, Object> values) {
            this(values, null);
        }

        private RecordingReselector(Map<String, Object> values, SQLException failure) {
            this.values = values;
            this.failure = failure;
        }

        @Override
        public ReselectResult reselect(
                String schema,
                String table,
                List<String> lobColumns,
                List<String> primaryKeyColumns,
                List<Object> primaryKeyValues,
                String commitScn)
                throws SQLException {
            if (failure != null) {
                throw failure;
            }
            this.schema = schema;
            this.table = table;
            this.lobColumns = lobColumns;
            this.primaryKeyColumns = primaryKeyColumns;
            this.primaryKeyValues = primaryKeyValues;
            this.commitScn = commitScn;
            if (values == null) {
                return ReselectResult.notFound();
            }
            return ReselectResult.found(values);
        }

        @Override
        public void close() {}
    }
}
