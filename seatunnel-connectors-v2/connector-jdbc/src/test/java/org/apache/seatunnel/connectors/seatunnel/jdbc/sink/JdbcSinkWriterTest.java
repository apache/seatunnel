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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.com.zaxxer.hikari.HikariDataSource;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionValidationUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/** Tests JDBC sink connection pool validation query customization. */
class JdbcSinkWriterTest {

    /** Verifies that Xugu pools use a validation query compatible with the driver. */
    @Test
    void testApplyConnectionValidationSetsXuguValidationQuery() {
        HikariDataSource dataSource = new HikariDataSource();
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(JdbcConnectionValidationUtils.XUGU_DRIVER)
                        .url("jdbc:xugu://localhost:5138/SYSTEM")
                        .build();

        JdbcSinkWriter.applyConnectionValidation(dataSource, jdbcConnectionConfig);

        Assertions.assertEquals(
                JdbcConnectionValidationUtils.XUGU_VALIDATION_QUERY,
                dataSource.getConnectionTestQuery());
        dataSource.close();
    }

    /** Verifies that other drivers keep Hikari's default validation behavior. */
    @Test
    void testApplyConnectionValidationKeepsDefaultDriverValidation() {
        HikariDataSource dataSource = new HikariDataSource();
        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName("org.postgresql.Driver")
                        .url("jdbc:postgresql://localhost:5432/test")
                        .build();

        JdbcSinkWriter.applyConnectionValidation(dataSource, jdbcConnectionConfig);

        Assertions.assertNull(dataSource.getConnectionTestQuery());
        dataSource.close();
    }

    /** Oracle sink ignores user auto_commit=true at runtime to keep failed batches atomic. */
    @Test
    void testOracleSinkResourceManagerUsesManualCommit() {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.ORACLE);
        Mockito.when(dialect.getJdbcConnectionProvider(Mockito.any()))
                .thenReturn(Mockito.mock(JdbcConnectionProvider.class));
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));

        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(DummyDriver.class.getName())
                        .url("jdbc:dummy:oracle-auto-commit")
                        .autoCommit(true)
                        .build();
        JdbcSinkConfig jdbcSinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(jdbcConnectionConfig)
                        .simpleSql("INSERT INTO TEST_TABLE(ID) VALUES (?)")
                        .build();
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("ID", BasicType.INT_TYPE, 22L, false, null, "ID"))
                        .build();
        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        null,
                        Mockito.mock(SinkWriter.Context.class),
                        dialect,
                        jdbcSinkConfig,
                        tableSchema,
                        tableSchema,
                        null);

        MultiTableResourceManager<ConnectionPoolManager> resourceManager =
                writer.initMultiTableResourceManager(1, 1);

        try {
            ConnectionPoolManager connectionPoolManager =
                    resourceManager.getSharedResource().orElseThrow(AssertionError::new);
            Assertions.assertFalse(connectionPoolManager.getConnectionPool().isAutoCommit());
        } finally {
            resourceManager.close();
        }
    }

    /** Oracle single-table sink also ignores user auto_commit=true at runtime. */
    @Test
    void testOracleSinkSingleTableProviderUsesManualCommit() {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = Mockito.mock(JdbcConnectionProvider.class);
        ArgumentCaptor<JdbcConnectionConfig> configCaptor =
                ArgumentCaptor.forClass(JdbcConnectionConfig.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.ORACLE);
        Mockito.when(dialect.getJdbcConnectionProvider(configCaptor.capture()))
                .thenReturn(connectionProvider);
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));

        JdbcConnectionConfig jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .driverName(DummyDriver.class.getName())
                        .url("jdbc:dummy:oracle-single-table")
                        .autoCommit(true)
                        .build();
        JdbcSinkConfig jdbcSinkConfig = buildJdbcSinkConfig(jdbcConnectionConfig);
        TableSchema tableSchema = buildTableSchema();

        new JdbcSinkWriter(
                null,
                Mockito.mock(SinkWriter.Context.class),
                dialect,
                jdbcSinkConfig,
                tableSchema,
                tableSchema,
                null);

        Assertions.assertTrue(jdbcConnectionConfig.isAutoCommit());
        Assertions.assertFalse(configCaptor.getValue().isAutoCommit());
    }

    /** Close must rollback, not commit, when a previous flush failure is already recorded. */
    @Test
    void testCloseShouldNotCommitAfterKnownFlushFailure() throws Exception {
        JdbcDialect dialect = Mockito.mock(JdbcDialect.class);
        JdbcConnectionProvider connectionProvider = Mockito.mock(JdbcConnectionProvider.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(dialect.dialectName()).thenReturn(DatabaseIdentifier.POSTGRESQL);
        Mockito.when(dialect.getJdbcConnectionProvider(Mockito.any()))
                .thenReturn(connectionProvider);
        Mockito.when(dialect.getRowConverter()).thenReturn(Mockito.mock(JdbcRowConverter.class));
        Mockito.when(connectionProvider.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);

        JdbcSinkWriter writer =
                new JdbcSinkWriter(
                        null,
                        Mockito.mock(SinkWriter.Context.class),
                        dialect,
                        buildJdbcSinkConfig(
                                JdbcConnectionConfig.builder()
                                        .driverName(DummyDriver.class.getName())
                                        .url("jdbc:dummy:close-flush-failure")
                                        .autoCommit(false)
                                        .build()),
                        buildTableSchema(),
                        buildTableSchema(),
                        null);
        JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat =
                Mockito.mock(JdbcOutputFormat.class);
        writer.outputFormat = outputFormat;
        writer.isOpen = true;
        Mockito.doThrow(new RuntimeException("previous flush failed"))
                .when(outputFormat)
                .checkFlushException();

        Assertions.assertThrows(JdbcConnectorException.class, writer::close);

        Mockito.verify(outputFormat, Mockito.never()).flush();
        Mockito.verify(connection, Mockito.never()).commit();
        Mockito.verify(connection).rollback();
        Mockito.verify(outputFormat).close();
    }

    private static JdbcSinkConfig buildJdbcSinkConfig(JdbcConnectionConfig jdbcConnectionConfig) {
        return JdbcSinkConfig.builder()
                .jdbcConnectionConfig(jdbcConnectionConfig)
                .simpleSql("INSERT INTO TEST_TABLE(ID) VALUES (?)")
                .build();
    }

    private static TableSchema buildTableSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("ID", BasicType.INT_TYPE, 22L, false, null, "ID"))
                .build();
    }

    public static class DummyDriver implements Driver {
        @Override
        public Connection connect(String url, Properties info) {
            return null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:dummy:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
