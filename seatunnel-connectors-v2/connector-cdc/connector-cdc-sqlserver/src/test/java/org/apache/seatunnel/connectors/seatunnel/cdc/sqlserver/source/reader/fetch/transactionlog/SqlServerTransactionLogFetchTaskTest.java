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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.transactionlog;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.scan.SqlServerSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.transactionlog.SqlServerTransactionLogFetchTask.TransactionLogSplitReadTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import io.debezium.DebeziumException;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceTimestampMode;
import io.debezium.connector.sqlserver.SqlServerChangeTable;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.connector.sqlserver.TxLogPosition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SqlServerTransactionLogFetchTaskTest {

    private static final TableId TABLE = new TableId("test_db", "dbo", "A");
    private static final Lsn LOW = lsn(10);
    private static final Lsn HIGH = lsn(20);
    private static final SqlServerPartition PARTITION =
            new SqlServerPartition("test_server", "test_db", false);

    @ParameterizedTest
    @MethodSource("queryBounds")
    void testBoundedQueryNeverReadsPastHigh(Lsn queryHigh, Lsn expected) throws Exception {
        Fixture fixture = new Fixture(true);
        Assertions.assertEquals(expected, fixture.task.limitQueryUpperBound(queryHigh));
    }

    private static Stream<Arguments> queryBounds() {
        return Stream.of(
                Arguments.of(lsn(15), lsn(15)),
                Arguments.of(HIGH, HIGH),
                Arguments.of(lsn(30), HIGH),
                Arguments.of(Lsn.NULL, Lsn.NULL));
    }

    @ParameterizedTest
    @MethodSource("completeCaptures")
    void testCompleteCaptureCoverageAllowsBoundedRead(SqlServerChangeTable[] captures)
            throws Exception {
        Fixture fixture = new Fixture(true);
        Assertions.assertDoesNotThrow(() -> fixture.task.validateQueryTables(captures, LOW, HIGH));
    }

    private static Stream<Arguments> completeCaptures() {
        return Stream.of(
                Arguments.of((Object) new SqlServerChangeTable[] {capture(TABLE, 1, null)}),
                Arguments.of((Object) new SqlServerChangeTable[] {capture(TABLE, 1, 21)}),
                Arguments.of(
                        (Object)
                                new SqlServerChangeTable[] {
                                    capture(TABLE, 1, 15), capture(TABLE, 15, null)
                                }),
                Arguments.of(
                        (Object)
                                new SqlServerChangeTable[] {
                                    capture(TABLE, 20, null), capture(TABLE, 1, 20)
                                }),
                Arguments.of(
                        (Object)
                                new SqlServerChangeTable[] {
                                    capture(TABLE, 1, 16), capture(TABLE, 15, null)
                                }));
    }

    @ParameterizedTest
    @MethodSource("incompleteCaptures")
    void testMissingCaptureCoverageCannotBeTreatedAsAnEmptyResult(SqlServerChangeTable[] captures)
            throws Exception {
        Fixture fixture = new Fixture(true);
        DebeziumException error =
                Assertions.assertThrows(
                        DebeziumException.class,
                        () -> fixture.task.validateQueryTables(captures, LOW, HIGH));
        Assertions.assertTrue(error.getMessage().contains("Cannot complete bounded backfill"));
        Assertions.assertTrue(error.getMessage().contains(TABLE.toString()));
        verifyNoInteractions(fixture.dispatcher);
        verify(fixture.context, never()).finished();
    }

    private static Stream<Arguments> incompleteCaptures() {
        return Stream.of(
                Arguments.of((Object) new SqlServerChangeTable[0]),
                Arguments.of(
                        (Object)
                                new SqlServerChangeTable[] {
                                    capture(new TableId("test_db", "dbo", "Other"), 1, null)
                                }),
                Arguments.of((Object) new SqlServerChangeTable[] {capture(TABLE, 11, null)}),
                Arguments.of((Object) new SqlServerChangeTable[] {capture(TABLE, 1, 20)}),
                Arguments.of(
                        (Object)
                                new SqlServerChangeTable[] {
                                    capture(TABLE, 1, 14), capture(TABLE, 15, null)
                                }),
                Arguments.of(
                        (Object)
                                new SqlServerChangeTable[] {
                                    new SqlServerChangeTable(
                                            TABLE, "unknown", 1, Lsn.NULL, Collections.emptyList())
                                }));
    }

    @Test
    void testEmptyCompletedIntervalEmitsEndAtHighWithoutChangingRowOffset() throws Exception {
        Fixture fixture = new Fixture(true);
        Map<String, Object> rowOffset = new HashMap<>();
        rowOffset.put("commit_lsn", LOW.toString());
        rowOffset.put("event_serial_no", 7L);
        Map<String, Object> originalOffset = new HashMap<>(rowOffset);

        fixture.task.afterQueryCompleted(PARTITION, rowOffset, HIGH);

        verify(fixture.dispatcher)
                .dispatchWatermarkEvent(
                        PARTITION.getSourcePartition(),
                        fixture.split,
                        fixture.split.getStopOffset(),
                        WatermarkKind.END);
        verify(fixture.context).finished();
        Assertions.assertEquals(originalOffset, rowOffset);
    }

    @Test
    void testIncompleteQueryDoesNotFinishBackfill() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.task.afterQueryCompleted(PARTITION, Collections.emptyMap(), lsn(19));
        verifyNoInteractions(fixture.dispatcher);
        verify(fixture.context, never()).finished();
    }

    @Test
    void testInterruptedEndDispatchDoesNotFinishBackfill() throws Exception {
        Fixture fixture = new Fixture(true);
        doThrow(new InterruptedException("test interruption"))
                .when(fixture.dispatcher)
                .dispatchWatermarkEvent(
                        PARTITION.getSourcePartition(),
                        fixture.split,
                        fixture.split.getStopOffset(),
                        WatermarkKind.END);

        Assertions.assertThrows(
                InterruptedException.class,
                () -> fixture.task.afterQueryCompleted(PARTITION, Collections.emptyMap(), HIGH));

        verify(fixture.context, never()).finished();
    }

    @Test
    void testRowCallbackDoesNotFinishBeforeQueryConsumption() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.task.afterHandleLsn(
                PARTITION, Collections.singletonMap("commit_lsn", lsn(30).toString()));
        verifyNoInteractions(fixture.dispatcher);
        verify(fixture.context, never()).finished();
    }

    @Test
    void testUnboundedReadRetainsQueryAndCompletionBehavior() throws Exception {
        Fixture fixture = new Fixture(false);
        Assertions.assertEquals(lsn(30), fixture.task.limitQueryUpperBound(lsn(30)));
        fixture.task.validateQueryTables(new SqlServerChangeTable[0], LOW, HIGH);
        fixture.task.afterQueryCompleted(PARTITION, Collections.emptyMap(), lsn(30));
        verifyNoInteractions(fixture.dispatcher);
        verify(fixture.context, never()).finished();
    }

    @Test
    void testEmptyIterationConsumesResultsAndRollsBackBeforeEnd() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.prepareIteration();
        ResultSet emptyResult = mock(ResultSet.class);
        fixture.returnEmptyChanges(emptyResult);

        fixture.task.executeIteration(fixture.context, PARTITION, fixture.offset);

        InOrder order =
                inOrder(emptyResult, fixture.connection, fixture.dispatcher, fixture.context);
        order.verify(emptyResult).next();
        order.verify(fixture.connection).rollback();
        order.verify(fixture.dispatcher)
                .dispatchWatermarkEvent(
                        PARTITION.getSourcePartition(), fixture.split,
                        fixture.split.getStopOffset(), WatermarkKind.END);
        order.verify(fixture.context).finished();
        verifyNoInteractions(fixture.errorHandler);
    }

    @Test
    void testFailedChangeQueryCannotFinishBackfill() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.prepareIteration();
        SQLException failure = new SQLException("test query failure");
        doThrow(failure)
                .when(fixture.connection)
                .getChangesForTables(
                        eq("test_db"),
                        any(SqlServerChangeTable[].class),
                        eq(LOW),
                        eq(HIGH),
                        any(JdbcConnection.BlockingMultiResultSetConsumer.class));

        fixture.task.executeIteration(fixture.context, PARTITION, fixture.offset);

        verify(fixture.errorHandler).setProducerThrowable(failure);
        verifyNoInteractions(fixture.dispatcher);
        verify(fixture.context, never()).finished();
    }

    @Test
    void testFailedRollbackCannotFinishBackfill() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.prepareIteration();
        fixture.returnEmptyChanges(mock(ResultSet.class));
        SQLException failure = new SQLException("test rollback failure");
        doThrow(failure).when(fixture.connection).rollback();

        fixture.task.executeIteration(fixture.context, PARTITION, fixture.offset);

        verify(fixture.errorHandler).setProducerThrowable(failure);
        verifyNoInteractions(fixture.dispatcher);
        verify(fixture.context, never()).finished();
    }

    private static Lsn lsn(int value) {
        return Lsn.valueOf(String.format("00000001:%08x:0001", value));
    }

    private static SqlServerChangeTable capture(TableId table, int start, Integer stop) {
        SqlServerChangeTable capture =
                new SqlServerChangeTable(
                        table, "capture_" + start, start, lsn(start), Collections.emptyList());
        if (stop != null) {
            capture.setStopLsn(lsn(stop));
        }
        return capture;
    }

    private static class Fixture {
        private final SqlServerConnectorConfig config =
                mock(SqlServerConnectorConfig.class, RETURNS_DEEP_STUBS);
        private final SqlServerConnection connection = mock(SqlServerConnection.class);
        private final SqlServerDatabaseSchema schema = mock(SqlServerDatabaseSchema.class);
        private final ErrorHandler errorHandler = mock(ErrorHandler.class);
        private final SqlServerOffsetContext offset = mock(SqlServerOffsetContext.class);
        private final JdbcSourceEventDispatcher<SqlServerPartition> dispatcher =
                mock(JdbcSourceEventDispatcher.class);
        private final SqlServerSnapshotFetchTask.SnapshotBinlogSplitChangeEventSourceContext
                context =
                        mock(
                                SqlServerSnapshotFetchTask
                                        .SnapshotBinlogSplitChangeEventSourceContext.class);
        private final IncrementalSplit split;
        private final TransactionLogSplitReadTask task;

        private Fixture(boolean bounded) throws Exception {
            split =
                    new IncrementalSplit(
                            "test_split",
                            Collections.singletonList(TABLE),
                            LsnOffset.valueOf(LOW.toString()),
                            bounded
                                    ? LsnOffset.valueOf(HIGH.toString())
                                    : LsnOffset.NO_STOPPING_OFFSET,
                            Collections.emptyList());
            when(config.getPollInterval()).thenReturn(Duration.ofMillis(1));
            task =
                    new TransactionLogSplitReadTask(
                            config,
                            connection,
                            mock(SqlServerConnection.class),
                            dispatcher,
                            errorHandler,
                            schema,
                            split);
            // A stopped context initializes the task without reading from a database.
            task.execute(context, PARTITION, mock(SqlServerOffsetContext.class));
        }

        private void prepareIteration() throws Exception {
            when(config.getSnapshotMode())
                    .thenReturn(SqlServerConnectorConfig.SnapshotMode.INITIAL);
            when(config.getSourceTimestampMode()).thenReturn(SourceTimestampMode.COMMIT);
            when(config.getTableFilters().dataCollectionFilter().isIncluded(TABLE))
                    .thenReturn(true);
            when(context.isRunning()).thenReturn(true);
            when(offset.getChangePosition()).thenReturn(TxLogPosition.valueOf(LOW));
            doReturn(Collections.singletonMap("commit_lsn", LOW.toString()))
                    .when(offset)
                    .getOffset();
            when(connection.getMaxTransactionLsn("test_db")).thenReturn(lsn(30));
            when(connection.getChangeTables("test_db", HIGH))
                    .thenReturn(Collections.singletonList(capture(TABLE, 1, null)));
            when(schema.tableFor(TABLE)).thenReturn(mock(Table.class));
        }

        private void returnEmptyChanges(ResultSet emptyResult) throws Exception {
            doAnswer(
                            invocation -> {
                                JdbcConnection.BlockingMultiResultSetConsumer consumer =
                                        invocation.getArgument(4);
                                consumer.accept(new ResultSet[] {emptyResult});
                                verifyNoInteractions(dispatcher);
                                return null;
                            })
                    .when(connection)
                    .getChangesForTables(
                            eq("test_db"),
                            any(SqlServerChangeTable[].class),
                            eq(LOW),
                            eq(HIGH),
                            any(JdbcConnection.BlockingMultiResultSetConsumer.class));
        }
    }
}
