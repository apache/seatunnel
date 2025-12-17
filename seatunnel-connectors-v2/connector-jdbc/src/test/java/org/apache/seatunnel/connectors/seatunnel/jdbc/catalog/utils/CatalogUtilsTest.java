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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CatalogUtilsTest {

    @Test
    void testPrimaryKeysNameWithOutSpecialChar() throws SQLException {
        Optional<PrimaryKey> primaryKey =
                CatalogUtils.getPrimaryKey(new TestDatabaseMetaData(), TablePath.of("test.test"));
        Assertions.assertEquals("testfdawe_", primaryKey.get().getPrimaryKey());
    }

    @Test
    void testConstraintKeysNameWithOutSpecialChar() throws SQLException {
        List<ConstraintKey> constraintKeys =
                CatalogUtils.getConstraintKeys(
                        new TestDatabaseMetaData(), TablePath.of("test.test"));
        Assertions.assertEquals("testfdawe_", constraintKeys.get(0).getConstraintName());
    }

    @Test
    void testGetTableCommentWithJdbcDialectTypeMapper() throws SQLException {
        TableSchema tableSchema =
                CatalogUtils.getTableSchema(
                        new TestDatabaseMetaData(),
                        TablePath.of("test.test"),
                        new JdbcDialectTypeMapper() {
                            @Override
                            public Column mappingColumn(BasicTypeDefine typeDefine) {
                                return JdbcDialectTypeMapper.super.mappingColumn(typeDefine);
                            }
                        });
        Assertions.assertEquals("id comment", tableSchema.getColumns().get(0).getComment());

        TableSchema tableSchema2 =
                CatalogUtils.getTableSchema(
                        new TestDatabaseMetaData(),
                        TablePath.of("test.test"),
                        new JdbcDialectTypeMapper() {
                            @Override
                            public Column mappingColumn(BasicTypeDefine typeDefine) {
                                return PhysicalColumn.of(
                                        typeDefine.getName(),
                                        BasicType.VOID_TYPE,
                                        typeDefine.getLength(),
                                        typeDefine.isNullable(),
                                        typeDefine.getScale(),
                                        typeDefine.getComment());
                            }
                        });
        Assertions.assertEquals("id comment", tableSchema2.getColumns().get(0).getComment());
    }

    @Test
    void testGetCatalogTableWithPrimaryKeyFromQuery() throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

        when(connection.prepareStatement("select id, name from test_table"))
                .thenReturn(preparedStatement);
        when(preparedStatement.getMetaData()).thenReturn(resultSetMetaData);

        when(resultSetMetaData.getColumnCount()).thenReturn(2);
        when(resultSetMetaData.getColumnLabel(1)).thenReturn("id");
        when(resultSetMetaData.getColumnLabel(2)).thenReturn("name");
        when(resultSetMetaData.getTableName(1)).thenReturn("test_table");
        when(resultSetMetaData.getCatalogName(1)).thenReturn("test_db");
        when(resultSetMetaData.getSchemaName(1)).thenReturn(null);
        when(resultSetMetaData.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);
        when(resultSetMetaData.isNullable(2)).thenReturn(ResultSetMetaData.columnNullable);

        when(connection.getMetaData()).thenReturn(new TestDatabaseMetaData());

        JdbcDialectTypeMapper typeMapper =
                new JdbcDialectTypeMapper() {
                    @Override
                    public Column mappingColumn(BasicTypeDefine typeDefine) {
                        return PhysicalColumn.of(
                                typeDefine.getName(),
                                BasicType.VOID_TYPE,
                                typeDefine.getLength(),
                                typeDefine.isNullable(),
                                null,
                                null);
                    }
                };

        CatalogTable catalogTable =
                CatalogUtils.getCatalogTable(
                        connection, "select id, name from test_table", typeMapper);

        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        Assertions.assertNotNull(primaryKey);
        Assertions.assertEquals("testfdawe_", primaryKey.getPrimaryKey());
        Assertions.assertEquals(1, primaryKey.getColumnNames().size());
        Assertions.assertEquals("id", primaryKey.getColumnNames().get(0));
    }

    @Test
    void testGetCatalogTableNotApplyPrimaryKeyWhenMissingColumns() throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

        when(connection.prepareStatement("select name from test_table"))
                .thenReturn(preparedStatement);
        when(preparedStatement.getMetaData()).thenReturn(resultSetMetaData);

        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getColumnLabel(1)).thenReturn("name");
        when(resultSetMetaData.getTableName(1)).thenReturn("test_table");
        when(resultSetMetaData.getCatalogName(1)).thenReturn("test_db");
        when(resultSetMetaData.getSchemaName(1)).thenReturn(null);
        when(resultSetMetaData.isNullable(1)).thenReturn(ResultSetMetaData.columnNullable);

        when(connection.getMetaData()).thenReturn(new TestDatabaseMetaData());

        JdbcDialectTypeMapper typeMapper =
                new JdbcDialectTypeMapper() {
                    @Override
                    public Column mappingColumn(BasicTypeDefine typeDefine) {
                        return PhysicalColumn.of(
                                typeDefine.getName(),
                                BasicType.VOID_TYPE,
                                typeDefine.getLength(),
                                typeDefine.isNullable(),
                                null,
                                null);
                    }
                };

        CatalogTable catalogTable =
                CatalogUtils.getCatalogTable(connection, "select name from test_table", typeMapper);

        Assertions.assertNull(catalogTable.getTableSchema().getPrimaryKey());
    }
}
