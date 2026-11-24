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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.opengauss;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenGaussDialectTest {

    private static final String URL = "jdbc:opengauss://localhost:5432/postgres";

    @Test
    void loaderReturnsOpenGaussDialectForOpenGaussUrl() {
        JdbcDialect dialect = JdbcDialectLoader.load(URL, "", null, "", null);
        assertEquals(OpenGaussDialect.class, dialect.getClass());

        Optional<String> upsertStatement =
                dialect.getUpsertStatement(
                        "test_db", "test_table", new String[] {"id", "name"}, new String[] {"id"});
        assertTrue(upsertStatement.isPresent());
        assertTrue(upsertStatement.get().contains("ON DUPLICATE KEY UPDATE"));
    }

    @Test
    void loaderKeepsFieldIdeForOpenGaussDialect() {
        JdbcDialect dialect = JdbcDialectLoader.load(URL, "", null, "uppercase", null);
        assertEquals(OpenGaussDialect.class, dialect.getClass());
        assertEquals("\"NAME\"", dialect.quoteIdentifier("name"));
    }

    @Test
    void loaderKeepsNumericSpecialValueAsNullForOpenGaussDialect() throws SQLException {
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder().url(URL).numericSpecialValueAsNull(true).build();
        JdbcDialect dialect = JdbcDialectLoader.load(URL, "", null, "", config);
        assertEquals(OpenGaussDialect.class, dialect.getClass());

        SeaTunnelRow row = dialect.getRowConverter().toInternal(numericNaNResultSet(), schema());
        assertEquals(1, row.getField(0));
        assertNull(row.getField(1));

        JdbcDialect defaultDialect = JdbcDialectLoader.load(URL, "", null, "", null);
        assertThrows(
                SQLException.class,
                () -> defaultDialect.getRowConverter().toInternal(numericNaNResultSet(), schema()));
    }

    private static TableSchema schema() {
        return TableSchema.builder()
                .column(PhysicalColumn.builder().name("id").dataType(BasicType.INT_TYPE).build())
                .column(
                        PhysicalColumn.builder()
                                .name("amount")
                                .dataType(new DecimalType(10, 2))
                                .build())
                .build();
    }

    /** A row whose NUMERIC column holds NaN, which the driver cannot read as a BigDecimal. */
    private static ResultSet numericNaNResultSet() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(metaData);
        when(metaData.getColumnCount()).thenReturn(2);
        when(metaData.getColumnTypeName(1)).thenReturn("INT4");
        when(metaData.getColumnTypeName(2)).thenReturn("NUMERIC");
        when(rs.getObject(1)).thenReturn(1);
        when(rs.getInt(1)).thenReturn(1);
        when(rs.getBigDecimal(2))
                .thenThrow(
                        new PSQLException(
                                "Bad value for type BigDecimal : NaN",
                                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE));
        when(rs.getString(2)).thenReturn("NaN");
        return rs;
    }

    @Test
    void returnsUpsertStatementWhenUpdateClauseIsNotEmpty() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] pkNames = {"id"};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, pkNames);
        assertTrue(upsertStatement.isPresent());
        assertEquals(
                "INSERT INTO \"test_db\".\"test_table\" (\"id\", \"name\", \"age\") VALUES (:id, :name, :age) ON DUPLICATE KEY UPDATE \"name\"=EXCLUDED.\"name\", \"age\"=EXCLUDED.\"age\"",
                upsertStatement.get());
    }

    @Test
    void returnsEmptyWhenUpdateClauseIsEmpty() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {"id"};
        String[] pkNames = {"id"};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, pkNames);
        assertFalse(upsertStatement.isPresent());
    }

    @Test
    void handlesEmptyFieldNames() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {};
        String[] pkNames = {"id"};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, pkNames);
        assertFalse(upsertStatement.isPresent());
    }

    @Test
    void handlesEmptyUniqueKeyFields() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] pkNames = {};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, pkNames);
        assertTrue(upsertStatement.isPresent());
        assertEquals(
                "INSERT INTO \"test_db\".\"test_table\" (\"id\", \"name\", \"age\") VALUES (:id, :name, :age) ON DUPLICATE KEY UPDATE \"id\"=EXCLUDED.\"id\", \"name\"=EXCLUDED.\"name\", \"age\"=EXCLUDED.\"age\"",
                upsertStatement.get());
    }
}
