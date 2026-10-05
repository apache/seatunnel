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

package org.apache.seatunnel.connectors.seatunnel.fluss.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSinkOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.Append;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that the Fluss sink releases all partially or fully initialized client resources.
 *
 * <p>The tests preserve the primary failure while checking every later cleanup failure.
 */
public class FlussSinkWriterTest {

    private SinkWriter.Context context;
    private CatalogTable catalogTable;
    private ReadonlyConfig pluginConfig;
    private Connection connection;
    private Table table;

    /**
     * Creates the common writer dependencies with an append-only Fluss table.
     *
     * <p>Individual tests override only the lifecycle stage that they need to fail.
     */
    @BeforeEach
    void setUp() {
        context = mock(SinkWriter.Context.class);
        catalogTable = mock(CatalogTable.class);
        pluginConfig = mock(ReadonlyConfig.class);
        connection = mock(Connection.class);
        table = mock(Table.class);

        TableSchema tableSchema = mock(TableSchema.class);
        SeaTunnelRowType rowType = new SeaTunnelRowType(new String[0], new SeaTunnelDataType<?>[0]);
        when(catalogTable.getTableSchema()).thenReturn(tableSchema);
        when(tableSchema.toPhysicalRowDataType()).thenReturn(rowType);

        when(pluginConfig.get(FlussSinkOptions.BOOTSTRAP_SERVERS)).thenReturn("localhost:9123");
        when(pluginConfig.getOptional(FlussSinkOptions.CLIENT_CONFIG)).thenReturn(Optional.empty());
        when(pluginConfig.getOptional(FlussSinkOptions.DATABASE))
                .thenReturn(Optional.of("database"));
        when(pluginConfig.getOptional(FlussSinkOptions.TABLE)).thenReturn(Optional.of("table"));

        TableInfo tableInfo = mock(TableInfo.class);
        Append append = mock(Append.class);
        AppendWriter appendWriter = mock(AppendWriter.class);
        when(connection.getTable(any(TablePath.class))).thenReturn(table);
        when(table.getTableInfo()).thenReturn(tableInfo);
        when(tableInfo.hasPrimaryKey()).thenReturn(false);
        when(table.newAppend()).thenReturn(append);
        when(append.createWriter()).thenReturn(appendWriter);
    }

    /**
     * Verifies that constructor cleanup keeps the initialization failure as the primary cause.
     *
     * @throws Exception if the mocked connection cannot be configured for the close failure
     */
    @Test
    void shouldCloseConnectionWhenTableCreationFails() throws Exception {
        RuntimeException initializationFailure = new RuntimeException("table creation failed");
        Exception connectionCloseFailure = new Exception("connection close failed");
        when(connection.getTable(any(TablePath.class))).thenThrow(initializationFailure);
        doThrow(connectionCloseFailure).when(connection).close();

        try (MockedStatic<ConnectionFactory> connectionFactory =
                mockStatic(ConnectionFactory.class)) {
            connectionFactory
                    .when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(connection);

            RuntimeException thrown =
                    assertThrows(
                            RuntimeException.class,
                            () -> new FlussSinkWriter(context, catalogTable, pluginConfig));

            assertSame(initializationFailure, thrown);
            assertEquals(1, thrown.getSuppressed().length);
            assertSame(connectionCloseFailure, thrown.getSuppressed()[0]);
            verify(connection).close();
        }
    }

    /**
     * Verifies that normal shutdown closes every initialized resource without an error.
     *
     * <p>This is the primary successful lifecycle path for the shared close helper.
     *
     * @throws Exception if the mocked resources cannot be verified for close invocation
     */
    @Test
    void shouldCloseAllResourcesWithoutFailure() throws Exception {
        FlussSinkWriter sinkWriter = createSinkWriter();

        sinkWriter.close();

        verify(table).close();
        verify(connection).close();
    }

    /**
     * Verifies that normal shutdown closes both resources and retains both close failures.
     *
     * @throws Exception if the mocked resources cannot be configured for close failures
     */
    @Test
    void shouldCloseAllResourcesAndAggregateFailures() throws Exception {
        FlussSinkWriter sinkWriter = createSinkWriter();

        Exception tableCloseFailure = new Exception("table close failed");
        Exception connectionCloseFailure = new Exception("connection close failed");
        doThrow(tableCloseFailure).when(table).close();
        doThrow(connectionCloseFailure).when(connection).close();

        SeaTunnelRuntimeException thrown =
                assertThrows(SeaTunnelRuntimeException.class, sinkWriter::close);

        assertSame(tableCloseFailure, thrown.getCause());
        assertEquals(1, thrown.getCause().getSuppressed().length);
        assertSame(connectionCloseFailure, thrown.getCause().getSuppressed()[0]);
        assertTrue(thrown.getMessage().contains(FlussSinkOptions.CONNECTOR_IDENTITY));
        verify(table).close();
        verify(connection).close();
    }

    /**
     * Creates a fully initialized writer while keeping the static factory mock scoped locally.
     *
     * @return the initialized Fluss sink writer
     */
    private FlussSinkWriter createSinkWriter() {
        try (MockedStatic<ConnectionFactory> connectionFactory =
                mockStatic(ConnectionFactory.class)) {
            connectionFactory
                    .when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(connection);
            return new FlussSinkWriter(context, catalogTable, pluginConfig);
        }
    }
}
