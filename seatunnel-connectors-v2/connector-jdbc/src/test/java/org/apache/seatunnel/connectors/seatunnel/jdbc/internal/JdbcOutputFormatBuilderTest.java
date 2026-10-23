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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.TestConnection;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlserverJdbcRowConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class JdbcOutputFormatBuilderTest {

    @Test
    public void testKeyExtractor() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        SeaTunnelRowType pkType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        int[] pkFields = Arrays.stream(pkType.getFieldNames()).mapToInt(rowType::indexOf).toArray();

        SeaTunnelRow insertRow = new SeaTunnelRow(new Object[] {1, "a", 60});
        insertRow.setTableId("test");
        insertRow.setRowKind(RowKind.INSERT);
        SeaTunnelRow updateBefore = new SeaTunnelRow(new Object[] {1, "a"});
        updateBefore.setTableId("test");
        updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        SeaTunnelRow updateAfter = new SeaTunnelRow(new Object[] {1, "b"});
        updateAfter.setTableId("test");
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        SeaTunnelRow deleteRow = new SeaTunnelRow(new Object[] {1});
        deleteRow.setTableId("test");
        deleteRow.setRowKind(RowKind.DELETE);

        Function<SeaTunnelRow, SeaTunnelRow> keyExtractor =
                JdbcOutputFormatBuilder.createKeyExtractor(pkFields);
        keyExtractor.apply(insertRow);

        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(insertRow));
        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateBefore));
        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateAfter));
        Assertions.assertEquals(keyExtractor.apply(insertRow), keyExtractor.apply(deleteRow));

        updateBefore.setTableId("test1");
        Assertions.assertNotEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateBefore));
        updateAfter.setField(0, "2");
        Assertions.assertNotEquals(keyExtractor.apply(insertRow), keyExtractor.apply(updateAfter));
    }

    @Test
    public void testBuildFormatWithDatabaseWithDot()
            throws SQLException, ClassNotFoundException, IOException {

        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 22L, false, null, "id"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put("database", "databasewith.dot");
        config.put("table", "dbo.tableName");

        SqlServerDialect dialect = Mockito.mock(SqlServerDialect.class);
        Mockito.when(dialect.getRowConverter()).thenReturn(new SqlserverJdbcRowConverter());
        Mockito.when(
                        dialect.getInsertIntoStatement(
                                Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn("");

        SimpleJdbcConnectionProvider provider = Mockito.mock(SimpleJdbcConnectionProvider.class);
        Mockito.when(provider.getOrEstablishConnection()).thenReturn(new TestConnection());
        Mockito.when(provider.getConnection()).thenReturn(new TestConnection());

        JdbcOutputFormat outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect,
                                provider,
                                JdbcSinkConfig.of(ReadonlyConfig.fromMap(config)),
                                schema,
                                schema)
                        .build();
        outputFormat.open();

        ArgumentCaptor<String> database = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> table = ArgumentCaptor.forClass(String.class);

        Mockito.verify(dialect)
                .getInsertIntoStatement(database.capture(), table.capture(), Mockito.any());

        Assertions.assertEquals("databasewith.dot", database.getValue());
        Assertions.assertEquals("dbo.tableName", table.getValue());
    }

    @Test
    public void testOracleAppendValuesKeepsConventionalInsertSql() {
        JdbcSinkConfig config =
                JdbcSinkConfig.builder()
                        .oracleInsertMode(JdbcSinkConfig.OracleInsertMode.CONVENTIONAL)
                        .build();

        String sql =
                JdbcOutputFormatBuilder.applyOracleAppendValuesHintIfNeeded(
                        config, "INSERT INTO TEST_TABLE (ID) VALUES (:ID)");

        Assertions.assertEquals("INSERT INTO TEST_TABLE (ID) VALUES (:ID)", sql);
    }

    @Test
    public void testOracleAppendValuesAddsHintToGeneratedInsertSql() {
        JdbcSinkConfig config =
                JdbcSinkConfig.builder()
                        .oracleInsertMode(JdbcSinkConfig.OracleInsertMode.APPEND_VALUES)
                        .build();

        String sql =
                JdbcOutputFormatBuilder.applyOracleAppendValuesHintIfNeeded(
                        config, "INSERT INTO TEST_TABLE (ID) VALUES (:ID)");

        Assertions.assertEquals(
                "INSERT /*+ APPEND_VALUES */ INTO TEST_TABLE (ID) VALUES (:ID)", sql);
    }

    @Test
    public void testOracleAppendValuesRejectsNonOracleDialect() {
        JdbcSinkConfig config = appendValuesConfigBuilder().build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> newBuilder(new MysqlDialect(), config).build());

        Assertions.assertEquals(
                "oracle_insert_mode=APPEND_VALUES only supports Oracle JDBC sink.",
                exception.getMessage());
    }

    @Test
    public void testOracleAppendValuesRejectsPrimaryKeys() {
        JdbcSinkConfig config =
                appendValuesConfigBuilder().primaryKeys(Collections.singletonList("ID")).build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> newBuilder(new OracleDialect(), config).build());

        Assertions.assertEquals(
                "oracle_insert_mode=APPEND_VALUES only supports insert-only writes without primary keys.",
                exception.getMessage());
    }

    private static JdbcSinkConfig.JdbcSinkConfigBuilder appendValuesConfigBuilder() {
        return JdbcSinkConfig.builder()
                .database("TEST_SCHEMA")
                .table("TEST_TABLE")
                .jdbcConnectionConfig(JdbcConnectionConfig.builder().autoCommit(true).build())
                .oracleInsertMode(JdbcSinkConfig.OracleInsertMode.APPEND_VALUES);
    }

    private static JdbcOutputFormatBuilder newBuilder(
            org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect dialect,
            JdbcSinkConfig config) {
        return new JdbcOutputFormatBuilder(
                dialect,
                Mockito.mock(SimpleJdbcConnectionProvider.class),
                config,
                oracleAppendValuesTableSchema(),
                null);
    }

    private static TableSchema oracleAppendValuesTableSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("ID", BasicType.INT_TYPE, 22L, false, null, "ID"))
                .build();
    }
}
