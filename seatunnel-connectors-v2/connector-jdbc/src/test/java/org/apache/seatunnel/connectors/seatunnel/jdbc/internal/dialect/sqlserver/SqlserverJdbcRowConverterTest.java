/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.Types;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

public class SqlserverJdbcRowConverterTest {

    @Test
    void testSetNullToStatementByDataTypeForImage() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        SqlserverJdbcRowConverter converter = new SqlserverJdbcRowConverter();
        SeaTunnelDataType<?> bytesType = PrimitiveByteArrayType.INSTANCE;

        converter.setNullToStatementByDataType(
                statement, bytesType, 1, SqlServerTypeConverter.SQLSERVER_IMAGE);

        verify(statement).setNull(1, Types.LONGVARBINARY);
    }

    @Test
    void testSetNullToStatementByDataTypeForVarbinary() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        SqlserverJdbcRowConverter converter = new SqlserverJdbcRowConverter();
        SeaTunnelDataType<?> bytesType = PrimitiveByteArrayType.INSTANCE;

        converter.setNullToStatementByDataType(
                statement, bytesType, 1, SqlServerTypeConverter.SQLSERVER_VARBINARY);

        verify(statement).setNull(1, Types.VARBINARY);
    }

    @Test
    void testSetNullToStatementByDataTypeForBinary() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        SqlserverJdbcRowConverter converter = new SqlserverJdbcRowConverter();
        SeaTunnelDataType<?> bytesType = PrimitiveByteArrayType.INSTANCE;

        converter.setNullToStatementByDataType(
                statement, bytesType, 1, SqlServerTypeConverter.SQLSERVER_BINARY);

        verify(statement).setNull(1, Types.BINARY);
    }

    @Test
    void testSetNullToStatementByDataTypeForMaxVarbinary() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        SqlserverJdbcRowConverter converter = new SqlserverJdbcRowConverter();
        SeaTunnelDataType<?> bytesType = PrimitiveByteArrayType.INSTANCE;

        converter.setNullToStatementByDataType(
                statement, bytesType, 1, SqlServerTypeConverter.MAX_VARBINARY);

        verify(statement).setNull(1, Types.VARBINARY);
    }
}
