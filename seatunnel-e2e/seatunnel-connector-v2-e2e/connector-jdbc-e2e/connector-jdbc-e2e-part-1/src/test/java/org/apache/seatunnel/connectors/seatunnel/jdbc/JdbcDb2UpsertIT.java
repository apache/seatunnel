/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2Dialect;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class JdbcDb2UpsertIT extends JdbcDb2IT {

    private static final String CREATE_SQL_SINK =
            "create table %s\n"
                    + "(\n"
                    + "    C_BOOLEAN          BOOLEAN,\n"
                    + "    C_SMALLINT         SMALLINT,\n"
                    + "    C_INT              INTEGER NOT NULL PRIMARY KEY,\n"
                    + "    C_INTEGER          INTEGER,\n"
                    + "    C_BIGINT           BIGINT,\n"
                    + "    C_DECIMAL          DECIMAL(5),\n"
                    + "    C_DEC              DECIMAL(5),\n"
                    + "    C_NUMERIC          DECIMAL(5),\n"
                    + "    C_NUM              DECIMAL(5),\n"
                    + "    C_REAL             REAL,\n"
                    + "    C_FLOAT            DOUBLE,\n"
                    + "    C_DOUBLE           DOUBLE,\n"
                    + "    C_DOUBLE_PRECISION DOUBLE,\n"
                    + "    C_CHAR             CHARACTER(1),\n"
                    + "    C_VARCHAR          VARCHAR(255),\n"
                    + "    C_BINARY           BINARY(1),\n"
                    + "    C_VARBINARY        VARBINARY(2048),\n"
                    + "    C_DATE             DATE,\n"
                    + "    C_UPDATED_AT       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
                    + "    \"c_int_2\"             INTEGER\n"
                    + ");\n";

    // create a trigger to update the timestamp when the row is updated.
    // if no changes are made to the row, the timestamp should not be updated.
    private static final String CREATE_TRIGGER_SQL =
            "CREATE TRIGGER c_updated_at_trigger\n"
                    + "    BEFORE UPDATE ON %s\n"
                    + "    REFERENCING NEW AS new_row\n"
                    + "    FOR EACH ROW\n"
                    + "BEGIN ATOMIC\n"
                    + "SET new_row.c_updated_at = CURRENT_TIMESTAMP;\n"
                    + "END;";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_db2_source_and_sink_upsert.conf");

    @Override
    JdbcCase getJdbcCase() {
        jdbcCase = super.getJdbcCase();
        jdbcCase.setSinkCreateSql(CREATE_SQL_SINK);
        jdbcCase.setConfigFile(CONFIG_FILE);
        jdbcCase.setAdditionalSqlOnSink(CREATE_TRIGGER_SQL);
        return jdbcCase;
    }

    @TestTemplate
    public void testDb2UpsertE2e(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        try {
            // step 1: run the job to migrate data from source to sink.
            Container.ExecResult execResult =
                    container.executeJob("/jdbc_db2_source_and_sink_upsert.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            List<List<Object>> updatedAtTimestampsBeforeUpdate =
                    query(
                            String.format(
                                    // Keep row order stable so repeated reads compare the same
                                    // records.
                                    "SELECT C_UPDATED_AT FROM %s ORDER BY C_INT",
                                    buildTableInfoWithSchema(DB2_DATABASE, DB2_SINK)));
            // step 2: run the job to update the data in the sink.
            // expected: timestamps should not be updated as the data is not changed.
            execResult = container.executeJob("/jdbc_db2_source_and_sink_upsert.conf");
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            List<List<Object>> updatedAtTimestampsAfterUpdate =
                    query(
                            String.format(
                                    "SELECT C_UPDATED_AT FROM %s ORDER BY C_INT",
                                    buildTableInfoWithSchema(DB2_DATABASE, DB2_SINK)));
            Assertions.assertIterableEquals(
                    updatedAtTimestampsBeforeUpdate, updatedAtTimestampsAfterUpdate);
        } finally {
            clearTable(DB2_DATABASE, DB2_SINK);
        }
    }

    @Test
    void testPreparedMergePreservesNullTransitionsAndSkipsUnchangedRows() throws Exception {
        DB2Dialect dialect = new DB2Dialect();
        String table = dialect.tableIdentifier(DB2_DATABASE, "NULL_SAFE_UPSERT");
        String trigger = dialect.tableIdentifier(DB2_DATABASE, "NULL_SAFE_UPSERT_TRIGGER");
        try (Statement ddl = connection.createStatement()) {
            ddl.execute(
                    "CREATE TABLE "
                            + table
                            + " (ID INT NOT NULL PRIMARY KEY, TXT VARCHAR(20), "
                            + "NUM INT, FLAG BOOLEAN, BIN VARBINARY(16), DAY_VALUE DATE, UPDATES INT DEFAULT 0)");
            try {
                ddl.execute(
                        "CREATE TRIGGER "
                                + trigger
                                + " BEFORE UPDATE ON "
                                + table
                                + " REFERENCING OLD AS O NEW AS N FOR EACH ROW "
                                + "BEGIN ATOMIC SET N.UPDATES = O.UPDATES + 1; END");
                TableSchema schema =
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "ID", BasicType.INT_TYPE, 0L, false, null, null))
                                .column(
                                        PhysicalColumn.of(
                                                "TXT",
                                                BasicType.STRING_TYPE,
                                                20L,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "NUM", BasicType.INT_TYPE, 0L, true, null, null))
                                .column(
                                        PhysicalColumn.of(
                                                "FLAG",
                                                BasicType.BOOLEAN_TYPE,
                                                0L,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "BIN",
                                                PrimitiveByteArrayType.INSTANCE,
                                                16L,
                                                true,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "DAY_VALUE",
                                                LocalTimeType.LOCAL_DATE_TYPE,
                                                0L,
                                                true,
                                                null,
                                                null))
                                .build();
                String merge =
                        dialect.getUpsertStatement(
                                        DB2_DATABASE,
                                        "NULL_SAFE_UPSERT",
                                        schema.getFieldNames(),
                                        new String[] {"ID"})
                                .get();
                Object[] row = {1, null, null, null, null, null};
                Object[] values = {"value", 42, true, new byte[] {1, 2}, LocalDate.of(2024, 1, 2)};
                int updates = 0;
                try (PreparedStatement statement = connection.prepareStatement(merge)) {
                    // First execution binds NULL markers, exercising native type inference.
                    mergeRow(dialect, schema, statement, row);
                    assertMergeRow(table, row, updates);
                    for (int i = 1; i < row.length; i++) {
                        row[i] = values[i - 1];
                        mergeRow(dialect, schema, statement, row);
                        assertMergeRow(table, row, ++updates);
                        mergeRow(dialect, schema, statement, row);
                        assertMergeRow(table, row, updates);
                        row[i] = null;
                        mergeRow(dialect, schema, statement, row);
                        assertMergeRow(table, row, ++updates);
                        mergeRow(dialect, schema, statement, row);
                        assertMergeRow(table, row, updates);
                    }
                    row[1] = "old";
                    mergeRow(dialect, schema, statement, row);
                    row[1] = "new";
                    mergeRow(dialect, schema, statement, row);
                    assertMergeRow(table, row, updates + 2);
                    row[0] = 2;
                    mergeRow(dialect, schema, statement, row);
                    assertMergeRow(table, row, 0);
                }
            } finally {
                ddl.execute("DROP TABLE " + table);
                connection.commit();
            }
        }
    }

    private void mergeRow(
            DB2Dialect dialect, TableSchema schema, PreparedStatement statement, Object[] row)
            throws SQLException {
        dialect.getRowConverter().toExternal(schema, null, new SeaTunnelRow(row), statement);
        statement.executeUpdate();
        connection.commit();
    }

    private void assertMergeRow(String table, Object[] row, int updates) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT ID, TXT, NUM, FLAG, BIN, DAY_VALUE, UPDATES FROM "
                                        + table
                                        + " WHERE ID = "
                                        + row[0])) {
            Assertions.assertTrue(result.next());
            Assertions.assertEquals(row[1], result.getString(2));
            Assertions.assertEquals(row[2], result.getObject(3));
            Assertions.assertEquals(row[3], result.getObject(4));
            Assertions.assertArrayEquals((byte[]) row[4], result.getBytes(5));
            Assertions.assertEquals(
                    row[5], result.getDate(6) == null ? null : result.getDate(6).toLocalDate());
            Assertions.assertEquals(
                    updates, result.getInt(7), "Unchanged values must not fire UPDATE triggers");
            Assertions.assertFalse(result.next());
        }
    }

    private List<List<Object>> query(String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getString(i));
                }
                result.add(objects);
                log.debug(String.format("Print query, sql: %s, data: %s", sql, objects));
            }
            connection.commit();
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
