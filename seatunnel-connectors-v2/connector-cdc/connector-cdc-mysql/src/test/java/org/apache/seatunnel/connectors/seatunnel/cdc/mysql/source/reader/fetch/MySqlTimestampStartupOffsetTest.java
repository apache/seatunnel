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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Covers timestamp bootstrap versus restored offsets, and the actual failure diagnostics retained
 * when MySQL negotiates a different binary-log status statement.
 */
public class MySqlTimestampStartupOffsetTest {

    @Test
    public void testBinlogDiagnosticsUseNegotiatedStatementAndKeepResultFields() throws Exception {
        for (String statement :
                new String[] {
                    MySqlConnection.MASTER_STATUS_STATEMENT,
                    MySqlConnection.BINARY_LOG_STATUS_STATEMENT
                }) {
            MySqlConnection connection = Mockito.mock(MySqlConnection.class);
            ResultSet status = Mockito.mock(ResultSet.class);
            Mockito.when(connection.binaryLogStatusStatement()).thenReturn(statement);
            Mockito.when(status.next()).thenReturn(true);
            Mockito.when(status.getString("File")).thenReturn("mysql-bin.000042");
            Mockito.when(status.getString("Position")).thenReturn("4096");
            Mockito.when(status.getString("Executed_Gtid_Set"))
                    .thenReturn("24bc7850-2c16-11e6-a073-0242ac110002:1-17");
            Mockito.when(connection.query(Mockito.eq(statement), Mockito.any()))
                    .thenAnswer(
                            invocation -> {
                                JdbcConnection.ResultSetConsumer consumer =
                                        invocation.getArgument(1);
                                consumer.accept(status);
                                return connection;
                            });

            Assertions.assertEquals(
                    Collections.singletonList(
                            "MySQL-CDC diagnostic: binary log status file=mysql-bin.000042, "
                                    + "position=4096, executed_gtid_set="
                                    + "24bc7850-2c16-11e6-a073-0242ac110002:1-17"),
                    collectBinaryLogDiagnostics(connection));

            Mockito.verify(connection).query(Mockito.eq(statement), Mockito.any());
            Mockito.verify(status).getString("File");
            Mockito.verify(status).getString("Position");
            Mockito.verify(status).getString("Executed_Gtid_Set");
        }
    }

    @Test
    public void testBinlogDiagnosticsReportEmptyAndFailedQueries() throws Exception {
        MySqlConnection connection = Mockito.mock(MySqlConnection.class);
        String statement = MySqlConnection.BINARY_LOG_STATUS_STATEMENT;
        Mockito.when(connection.binaryLogStatusStatement()).thenReturn(statement);
        ResultSet status = Mockito.mock(ResultSet.class);
        Mockito.when(status.next()).thenReturn(false);
        Mockito.when(connection.query(Mockito.eq(statement), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            JdbcConnection.ResultSetConsumer consumer = invocation.getArgument(1);
                            consumer.accept(status);
                            return connection;
                        });
        Assertions.assertEquals(
                Collections.singletonList(
                        "MySQL-CDC diagnostic: " + statement + " returned empty result"),
                collectBinaryLogDiagnostics(connection));

        Mockito.doThrow(new SQLException("status unavailable"))
                .when(connection)
                .query(Mockito.eq(statement), Mockito.any());
        Assertions.assertEquals(
                Collections.singletonList(
                        "MySQL-CDC diagnostic: failed to query "
                                + statement
                                + ": status unavailable"),
                collectBinaryLogDiagnostics(connection));
    }

    @Test
    public void testTimestampStartupResolvesConfiguredTimestampOnlyForBootstrapOffset() {
        BinlogOffset timestampOffset = new BinlogOffset(1716076800L);
        BinlogOffset restoredOffset = new BinlogOffset("mysql-bin.000021", 4096L);

        Assertions.assertTrue(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, timestampOffset));
        Assertions.assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredOffset));
    }

    @Test
    public void testCheckpointRestoreUsesPersistedBinlogOffsetAfterTimestampBootstrap() {
        BinlogOffset timestampOffset = new BinlogOffset(1716076800L);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("test.orders")),
                        timestampOffset,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());

        IncrementalSplitState splitState = new IncrementalSplitState(incrementalSplit);
        BinlogOffset checkpointOffset = new BinlogOffset("mysql-bin.000021", 4096L);
        splitState.setStartupOffset(checkpointOffset);

        IncrementalSplit restoredSplit = splitState.toSourceSplit();

        Assertions.assertEquals(checkpointOffset, restoredSplit.getStartupOffset());
        Assertions.assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredSplit.getStartupOffset()));
    }

    /**
     * Captures formatted production log events and restores logger state even if the query fails.
     */
    private static List<String> collectBinaryLogDiagnostics(MySqlConnection connection) {
        Logger logger = (Logger) LogManager.getLogger(MySqlSourceFetchTaskContext.class);
        Level previousLevel = logger.getLevel();
        List<String> messages = new ArrayList<>();
        AbstractAppender appender =
                new AbstractAppender(
                        "mysql-binlog-diagnostics-test",
                        null,
                        PatternLayout.createDefaultLayout(),
                        false,
                        Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        try {
            MySqlSourceFetchTaskContext.logMySqlBinaryLogStatus(connection);
            return messages;
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }
}
