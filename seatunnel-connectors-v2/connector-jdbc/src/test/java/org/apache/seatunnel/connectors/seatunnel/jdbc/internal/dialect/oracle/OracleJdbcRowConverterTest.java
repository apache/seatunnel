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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.StatementBoundLobs;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_CLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_NCLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_VARCHAR2;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class OracleJdbcRowConverterTest {

    private static final String FIELD_NAME = "payload";

    private final OracleJdbcRowConverter oracleJdbcRowConverter = new OracleJdbcRowConverter();

    @Test
    public void testNullClobUsesTypedNullBinding() throws SQLException {
        assertOracleNullBinding(BasicType.STRING_TYPE, ORACLE_CLOB, Types.CLOB);
    }

    @Test
    public void testNullNclobUsesTypedNullBinding() throws SQLException {
        assertOracleNullBinding(BasicType.STRING_TYPE, ORACLE_NCLOB, Types.NCLOB);
    }

    @Test
    public void testNullBlobUsesTypedNullBinding() throws SQLException {
        assertOracleNullBinding(ArrayType.BYTE_ARRAY_TYPE, ORACLE_BLOB, Types.BLOB);
    }

    @Test
    public void testNullVarchar2UsesTypedNullBinding() throws SQLException {
        assertOracleNullBinding(BasicType.STRING_TYPE, ORACLE_VARCHAR2, Types.VARCHAR);
    }

    @Test
    public void testNonOracleConverterKeepsDefaultUntypedNullBinding() throws SQLException {
        TableSchema tableSchema = createTableSchema(BasicType.STRING_TYPE, null);
        TableSchema databaseTableSchema = createTableSchema(BasicType.STRING_TYPE, ORACLE_CLOB);
        PreparedStatement statement = mock(PreparedStatement.class);

        new DefaultJdbcRowConverter()
                .toExternal(
                        tableSchema,
                        databaseTableSchema,
                        new SeaTunnelRow(new Object[] {null}),
                        statement);

        verify(statement).setObject(1, null);
        verify(statement, never()).setNull(1, Types.CLOB);
        verifyNoMoreInteractions(statement);
    }

    @Test
    public void testShortClobKeepsCharacterStream() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        writeLob(ORACLE_CLOB, "abc", statement);

        verify(statement).setCharacterStream(eq(1), any(Reader.class), eq(3));
        verify(statement, never()).getConnection();
        verify(statement, never()).setClob(anyInt(), any(Clob.class));
    }

    @Test
    public void testLongClobUsesCreateClobAndFreesAfterBatch() throws SQLException {
        String value = boundaryValue();
        Connection connection = mock(Connection.class);
        Clob clob = mock(Clob.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(connection.createClob()).thenReturn(clob);

        writeLob(ORACLE_CLOB, value, statement);

        verify(clob).setString(1, value);
        verify(statement).setClob(1, clob);
        verify(statement, never()).setCharacterStream(anyInt(), any(Reader.class), anyInt());
        verify(statement, never()).setCharacterStream(anyInt(), any(Reader.class), anyLong());

        StatementBoundLobs.free(statement);
        verify(clob).free();
    }

    @Test
    public void testLongNclobUsesCreateNClob() throws SQLException {
        String value = boundaryValue();
        Connection connection = mock(Connection.class);
        NClob nclob = mock(NClob.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(connection.createNClob()).thenReturn(nclob);

        writeLob(ORACLE_NCLOB, value, statement);

        verify(nclob).setString(1, value);
        verify(statement).setNClob(1, nclob);
        verify(statement, never()).setNCharacterStream(anyInt(), any(Reader.class), anyLong());

        StatementBoundLobs.free(statement);
        verify(nclob).free();
    }

    @Test
    public void testEmptyClobUsesEmptyLocator() throws SQLException {
        Connection connection = mock(Connection.class);
        Clob clob = mock(Clob.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(connection.createClob()).thenReturn(clob);

        writeLob(ORACLE_CLOB, "", statement);

        verify(statement).setClob(1, clob);
        verify(clob, never()).setString(anyLong(), any());
        verify(statement, never()).setCharacterStream(anyInt(), any(Reader.class), anyInt());
        verify(statement, never()).setCharacterStream(anyInt(), any(Reader.class), anyLong());

        StatementBoundLobs.free(statement);
        verify(clob).free();
    }

    @Test
    public void testEmptyNclobUsesEmptyLocator() throws SQLException {
        Connection connection = mock(Connection.class);
        NClob nclob = mock(NClob.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(connection.createNClob()).thenReturn(nclob);

        writeLob(ORACLE_NCLOB, "", statement);

        verify(statement).setNClob(1, nclob);
        verify(nclob, never()).setString(anyLong(), any());
        verify(statement, never()).setNCharacterStream(anyInt(), any(Reader.class), anyLong());

        StatementBoundLobs.free(statement);
        verify(nclob).free();
    }

    @Test
    public void testEmptyBlobUsesEmptyLocator() throws SQLException {
        Connection connection = mock(Connection.class);
        Blob blob = mock(Blob.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.getConnection()).thenReturn(connection);
        when(connection.createBlob()).thenReturn(blob);

        writeBytes(ORACLE_BLOB, new byte[0], statement);

        verify(statement).setBlob(1, blob);
        verify(statement, never()).setBinaryStream(anyInt(), any(InputStream.class), anyInt());
        verify(statement, never()).setBinaryStream(anyInt(), any(InputStream.class), anyLong());

        StatementBoundLobs.free(statement);
        verify(blob).free();
    }

    @Test
    public void testNonEmptyBlobKeepsBinaryStream() throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);

        writeBytes(ORACLE_BLOB, new byte[] {0x01, 0x02}, statement);

        verify(statement).setBinaryStream(eq(1), any(InputStream.class), eq(2));
        verify(statement, never()).getConnection();
        verify(statement, never()).setBlob(anyInt(), any(Blob.class));
    }

    private void writeLob(String sourceType, String value, PreparedStatement statement)
            throws SQLException {
        TableSchema tableSchema = createTableSchema(BasicType.STRING_TYPE, null);
        TableSchema databaseTableSchema = createTableSchema(BasicType.STRING_TYPE, sourceType);
        oracleJdbcRowConverter.toExternal(
                tableSchema,
                databaseTableSchema,
                new SeaTunnelRow(new Object[] {value}),
                statement);
    }

    private void writeBytes(String sourceType, byte[] value, PreparedStatement statement)
            throws SQLException {
        TableSchema tableSchema = createTableSchema(PrimitiveByteArrayType.INSTANCE, null);
        TableSchema databaseTableSchema =
                createTableSchema(PrimitiveByteArrayType.INSTANCE, sourceType);
        oracleJdbcRowConverter.toExternal(
                tableSchema,
                databaseTableSchema,
                new SeaTunnelRow(new Object[] {value}),
                statement);
    }

    private static String boundaryValue() {
        StringBuilder builder = new StringBuilder(OracleJdbcRowConverter.LOB_UTF16_BLOCK + 2);
        for (int i = 0; i < OracleJdbcRowConverter.LOB_UTF16_BLOCK - 1; i++) {
            builder.append('a');
        }
        builder.append("\uD83D\uDE00");
        return builder.toString();
    }

    private void assertOracleNullBinding(
            SeaTunnelDataType<?> seaTunnelDataType, String sourceType, int expectedJdbcType)
            throws SQLException {
        TableSchema tableSchema = createTableSchema(seaTunnelDataType, null);
        TableSchema databaseTableSchema = createTableSchema(seaTunnelDataType, sourceType);
        PreparedStatement statement = mock(PreparedStatement.class);

        oracleJdbcRowConverter.toExternal(
                tableSchema, databaseTableSchema, new SeaTunnelRow(new Object[] {null}), statement);

        verify(statement).setNull(1, expectedJdbcType);
        verify(statement, never()).setObject(1, null);
        verifyNoMoreInteractions(statement);
    }

    private TableSchema createTableSchema(
            SeaTunnelDataType<?> seaTunnelDataType, String sourceType) {
        PhysicalColumn.PhysicalColumnBuilder columnBuilder =
                PhysicalColumn.builder().name(FIELD_NAME).dataType(seaTunnelDataType);
        if (sourceType != null) {
            columnBuilder.sourceType(sourceType);
        }
        Column column = columnBuilder.build();
        return TableSchema.builder().columns(Collections.singletonList(column)).build();
    }

    private static final class DefaultJdbcRowConverter extends AbstractJdbcRowConverter {

        @Override
        public String converterName() {
            return "default-test-converter";
        }
    }
}
