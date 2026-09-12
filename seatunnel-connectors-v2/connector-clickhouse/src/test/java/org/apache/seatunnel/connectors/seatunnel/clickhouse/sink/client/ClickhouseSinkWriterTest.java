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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClickhouseSinkWriterTest {

    @Test
    void shouldRegisterTimerFlushAction() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        createWriterWithPendingRow(context, statement);

        verify(context, times(1)).registerFlushAction(actionCaptor.capture());
        actionCaptor.getValue().run();
        verify(statement, times(1)).executeBatch();
    }

    @Test
    void shouldPropagateTimerFlushFailure() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);
        SQLException expected = new SQLException("timer flush failed");
        doThrow(expected).when(statement).executeBatch();

        createWriterWithPendingRow(context, statement);

        verify(context).registerFlushAction(actionCaptor.capture());
        ClickhouseConnectorException actual =
                Assertions.assertThrows(
                        ClickhouseConnectorException.class, actionCaptor.getValue()::run);
        Assertions.assertSame(expected, actual.getCause());
    }

    private void createWriterWithPendingRow(SinkWriter.Context context, PreparedStatement statement)
            throws Exception {
        Shard shard = mock(Shard.class);
        when(shard.getJdbcUrl()).thenReturn("jdbc:clickhouse://localhost:8123/default");
        ShardMetadata shardMetadata =
                new ShardMetadata(
                        null,
                        null,
                        null,
                        "default",
                        "timer_flush",
                        "MergeTree",
                        false,
                        shard,
                        "default",
                        "");
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        ReaderOption option =
                ReaderOption.builder()
                        .shardMetadata(shardMetadata)
                        .properties(new Properties())
                        .seaTunnelRowType(rowType)
                        .tableEngine("MergeTree")
                        .tableSchema(Collections.singletonMap("id", "Int32"))
                        .bulkSize(10000)
                        .build();

        try (MockedConstruction<ClickhouseProxy> ignored =
                        Mockito.mockConstruction(ClickhouseProxy.class);
                MockedConstruction<ClickHouseConnectionImpl> ignoredConnection =
                        Mockito.mockConstruction(
                                ClickHouseConnectionImpl.class,
                                (mock, constructionContext) ->
                                        when(mock.prepareStatement(Mockito.anyString()))
                                                .thenReturn(statement))) {
            ClickhouseSinkWriter writer = new ClickhouseSinkWriter(option, context);
            writer.write(new SeaTunnelRow(new Object[] {1}));
        }
    }
}
