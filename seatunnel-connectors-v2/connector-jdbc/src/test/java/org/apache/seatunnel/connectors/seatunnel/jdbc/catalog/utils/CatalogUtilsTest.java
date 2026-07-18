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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    void testGetTableSchemaPropagatesPrimaryKeyMetadataFailure() {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public java.sql.ResultSet getPrimaryKeys(
                            String catalog, String schema, String table) throws SQLException {
                        throw new SQLException("getPrimaryKeys is not supported");
                    }
                };

        Assertions.assertThrows(
                SQLException.class,
                () ->
                        CatalogUtils.getTableSchema(
                                metadata,
                                TablePath.of("test.test"),
                                new JdbcDialectTypeMapper() {}));
    }

    @Test
    void testGetCatalogTableCanSkipPrimaryKeyMetadata() throws SQLException {
        Connection connection =
                new TestConnection() {
                    @Override
                    public java.sql.DatabaseMetaData getMetaData() {
                        return new TestDatabaseMetaData() {
                            @Override
                            public java.sql.ResultSet getPrimaryKeys(
                                    String catalog, String schema, String table)
                                    throws SQLException {
                                throw new SQLException("getPrimaryKeys is not supported");
                            }
                        };
                    }
                };

        TablePath tablePath = TablePath.of("test.test");
        JdbcDialect dialect = mock(JdbcDialect.class);
        when(dialect.supportsPrimaryKeyMetadata()).thenReturn(false);
        when(dialect.getJdbcDialectTypeMapper()).thenReturn(new JdbcDialectTypeMapper() {});
        when(dialect.getPartitionKeys(connection, tablePath)).thenReturn(Arrays.asList("dt", "hr"));

        CatalogTable catalogTable = CatalogUtils.getCatalogTable(connection, tablePath, dialect);

        Assertions.assertNull(catalogTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(Arrays.asList("dt", "hr"), catalogTable.getPartitionKeys());
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
    void testGetTableSchemaFiltersOutOtherMatchedTables() throws SQLException {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public java.sql.ResultSet getColumns(
                            String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String columnNamePattern)
                            throws SQLException {
                        List<Map<String, Object>> value = new ArrayList<>();
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "user_info");
                                        put("TABLE_SCHEM", "public");
                                        put("COLUMN_NAME", "id");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "id comment");
                                    }
                                });
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "userAinfo");
                                        put("TABLE_SCHEM", "public");
                                        put("COLUMN_NAME", "bad");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "should be filtered");
                                    }
                                });
                        return new TestResultSet(value);
                    }
                };

        TablePath tablePath = TablePath.of("test_db", "public", "user_info");

        TableSchema tableSchema =
                CatalogUtils.getTableSchema(
                        metadata,
                        tablePath,
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

        Assertions.assertEquals(1, tableSchema.getColumns().size());
        Assertions.assertEquals("id", tableSchema.getColumns().get(0).getName());
        Assertions.assertEquals("id comment", tableSchema.getColumns().get(0).getComment());

        TableSchema fallbackTableSchema =
                CatalogUtils.getTableSchema(metadata, tablePath, new JdbcDialectTypeMapper() {});
        Assertions.assertEquals(1, fallbackTableSchema.getColumns().size());
        Assertions.assertEquals("id", fallbackTableSchema.getColumns().get(0).getName());
    }

    @Test
    void testGetTableSchemaFiltersOutPercentageWildcard() throws SQLException {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public java.sql.ResultSet getColumns(
                            String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String columnNamePattern)
                            throws SQLException {
                        List<Map<String, Object>> value = new ArrayList<>();
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "user%info");
                                        put("TABLE_SCHEM", "public");
                                        put("COLUMN_NAME", "id");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "id comment");
                                    }
                                });
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "userXYZinfo");
                                        put("TABLE_SCHEM", "public");
                                        put("COLUMN_NAME", "bad");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "should be filtered");
                                    }
                                });
                        return new TestResultSet(value);
                    }
                };

        TablePath tablePath = TablePath.of("test_db", "public", "user%info");
        TableSchema tableSchema =
                CatalogUtils.getTableSchema(metadata, tablePath, new JdbcDialectTypeMapper() {});
        Assertions.assertEquals(1, tableSchema.getColumns().size());
        Assertions.assertEquals("id", tableSchema.getColumns().get(0).getName());
    }

    @Test
    void testGetTableSchemaFiltersOutSchemaWildcard() throws SQLException {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public java.sql.ResultSet getColumns(
                            String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String columnNamePattern)
                            throws SQLException {
                        List<Map<String, Object>> value = new ArrayList<>();
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "user_info");
                                        put("TABLE_SCHEM", "pub_lic");
                                        put("COLUMN_NAME", "id");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "id comment");
                                    }
                                });
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "user_info");
                                        put("TABLE_SCHEM", "pubAlic");
                                        put("COLUMN_NAME", "bad");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "should be filtered");
                                    }
                                });
                        return new TestResultSet(value);
                    }
                };

        TablePath tablePath = TablePath.of("test_db", "pub_lic", "user_info");
        TableSchema tableSchema =
                CatalogUtils.getTableSchema(metadata, tablePath, new JdbcDialectTypeMapper() {});
        Assertions.assertEquals(1, tableSchema.getColumns().size());
        Assertions.assertEquals("id", tableSchema.getColumns().get(0).getName());
    }

    @Test
    void testGetTableSchemaEmptyWhenAllFiltered() throws SQLException {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public java.sql.ResultSet getColumns(
                            String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String columnNamePattern)
                            throws SQLException {
                        List<Map<String, Object>> value = new ArrayList<>();
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "other_table");
                                        put("TABLE_SCHEM", "public");
                                        put("COLUMN_NAME", "bad");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "should be filtered");
                                    }
                                });
                        return new TestResultSet(value);
                    }
                };

        TablePath tablePath = TablePath.of("test_db", "public", "user_info");
        TableSchema tableSchema =
                CatalogUtils.getTableSchema(metadata, tablePath, new JdbcDialectTypeMapper() {});
        Assertions.assertTrue(tableSchema.getColumns().isEmpty());
    }

    @Test
    void testGetTableSchemaCaseSensitiveIdentifiersRequireExactMatch() throws SQLException {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public boolean supportsMixedCaseIdentifiers() throws SQLException {
                        return true;
                    }

                    @Override
                    public java.sql.ResultSet getColumns(
                            String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String columnNamePattern)
                            throws SQLException {
                        List<Map<String, Object>> value = new ArrayList<>();
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "userinfo");
                                        put("TABLE_SCHEM", "public");
                                        put("COLUMN_NAME", "id");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "id comment");
                                    }
                                });
                        return new TestResultSet(value);
                    }
                };

        TablePath tablePath = TablePath.of("test_db", "public", "UserInfo");
        TableSchema tableSchema =
                CatalogUtils.getTableSchema(metadata, tablePath, new JdbcDialectTypeMapper() {});
        Assertions.assertEquals(Collections.emptyList(), tableSchema.getColumns());
    }

    @Test
    void testGetTableSchemaFallsBackWhenIdentifierCaseMetadataUnsupported() throws SQLException {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public boolean supportsMixedCaseIdentifiers() throws SQLException {
                        // Hive JDBC 3.1.3 throws a plain SQLException for this metadata API on
                        // JDK 8, not SQLFeatureNotSupportedException.
                        throw new SQLException("Method not supported");
                    }

                    @Override
                    public java.sql.ResultSet getColumns(
                            String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String columnNamePattern)
                            throws SQLException {
                        List<Map<String, Object>> value = new ArrayList<>();
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "USER_INFO");
                                        put("TABLE_SCHEM", "public");
                                        put("COLUMN_NAME", "id");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "id comment");
                                    }
                                });
                        return new TestResultSet(value);
                    }
                };

        TablePath tablePath = TablePath.of("test_db", "public", "user_info");
        TableSchema tableSchema =
                CatalogUtils.getTableSchema(metadata, tablePath, new JdbcDialectTypeMapper() {});

        Assertions.assertEquals(1, tableSchema.getColumns().size());
        Assertions.assertEquals("id", tableSchema.getColumns().get(0).getName());
    }

    @Test
    void testGetTableSchemaPropagatesIdentifierCaseMetadataFailure() {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public boolean supportsMixedCaseIdentifiers() throws SQLException {
                        throw new SQLException("connection broken");
                    }
                };

        SQLException exception =
                Assertions.assertThrows(
                        SQLException.class,
                        () ->
                                CatalogUtils.getTableSchema(
                                        metadata,
                                        TablePath.of("test_db", "public", "user_info"),
                                        new JdbcDialectTypeMapper() {}));
        Assertions.assertEquals("connection broken", exception.getMessage());
    }

    @Test
    void testGetTableSchemaStoresUpperCaseIdentifiersCanMatchLowerCaseInput() throws SQLException {
        TestDatabaseMetaData metadata =
                new TestDatabaseMetaData() {
                    @Override
                    public boolean supportsMixedCaseIdentifiers() throws SQLException {
                        return false;
                    }

                    @Override
                    public boolean storesUpperCaseIdentifiers() throws SQLException {
                        return true;
                    }

                    @Override
                    public java.sql.ResultSet getColumns(
                            String catalog,
                            String schemaPattern,
                            String tableNamePattern,
                            String columnNamePattern)
                            throws SQLException {
                        List<Map<String, Object>> value = new ArrayList<>();
                        value.add(
                                new HashMap<String, Object>() {
                                    {
                                        put("TABLE_NAME", "USER_INFO");
                                        put("TABLE_SCHEM", "PUBLIC");
                                        put("COLUMN_NAME", "id");
                                        put("DATA_TYPE", 1);
                                        put("TYPE_NAME", "INT");
                                        put("COLUMN_SIZE", 11);
                                        put("DECIMAL_DIGITS", 0);
                                        put("NULLABLE", 0);
                                        put("REMARKS", "id comment");
                                    }
                                });
                        return new TestResultSet(value);
                    }
                };

        TablePath tablePath = TablePath.of("test_db", "public", "user_info");
        TableSchema tableSchema =
                CatalogUtils.getTableSchema(metadata, tablePath, new JdbcDialectTypeMapper() {});
        Assertions.assertEquals(1, tableSchema.getColumns().size());
        Assertions.assertEquals("id", tableSchema.getColumns().get(0).getName());
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

    @Test
    void testGetCatalogTableKeepsPartitionKeys() throws SQLException {
        Connection connection =
                new TestConnection() {
                    @Override
                    public java.sql.DatabaseMetaData getMetaData() {
                        return new TestDatabaseMetaData();
                    }
                };

        CatalogTable catalogTable =
                CatalogUtils.getCatalogTable(
                        connection,
                        TablePath.of("test.test"),
                        new JdbcDialectTypeMapper() {
                            @Override
                            public Column mappingColumn(BasicTypeDefine typeDefine) {
                                return JdbcDialectTypeMapper.super.mappingColumn(typeDefine);
                            }
                        },
                        Arrays.asList("dt", "hr"));

        Assertions.assertEquals(Arrays.asList("dt", "hr"), catalogTable.getPartitionKeys());
    }
}
