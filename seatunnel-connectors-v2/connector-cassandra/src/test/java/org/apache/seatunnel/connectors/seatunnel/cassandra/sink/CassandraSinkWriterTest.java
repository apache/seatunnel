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

package org.apache.seatunnel.connectors.seatunnel.cassandra.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CassandraSinkWriterTest {

    private final CqlSession session = mock(CqlSession.class);
    private final BoundStatement statement = mock(BoundStatement.class, RETURNS_SELF);
    private boolean tableInMetadata = true;
    private boolean tableInRefreshedMetadata = true;
    private int batchSize = 100;

    @Test
    void nullPrimaryKeyFailsTheWriteWithAsyncWrite() {
        assertNullPrimaryKeyFailsTheWrite(true, "t");
    }

    @Test
    void nullPrimaryKeyFailsTheWriteWithSyncWrite() {
        assertNullPrimaryKeyFailsTheWrite(false, "t");
    }

    @Test
    void nullPrimaryKeyFailsTheWriteWithATableNameIncludingTheKeyspace() {
        assertNullPrimaryKeyFailsTheWrite(true, "ks.t");
    }

    @Test
    void nullInAnotherColumnIsWritten() throws IOException {
        writer(true, "t").write(new SeaTunnelRow(new Object[] {1L, null}));

        verify(statement).setLong(0, 1L);
        verify(statement).setToNull(1);
        verify(session).executeAsync(statement);
    }

    @Test
    void primaryKeyMissingFromTheMetadataIsReadAfterARefresh() {
        tableInMetadata = false;

        assertNullPrimaryKeyFailsTheWrite(true, "t");
        verify(session).refreshSchema();
    }

    @Test
    void writerFailsAndClosesTheSessionWhenThePrimaryKeyCannotBeRead() {
        tableInMetadata = false;
        tableInRefreshedMetadata = false;

        CassandraConnectorException e =
                Assertions.assertThrows(CassandraConnectorException.class, () -> writer(true, "t"));

        Assertions.assertTrue(
                e.getMessage().contains("Cannot read the primary key of table ks.t"),
                e.getMessage());
        verify(session).close();
        verify(session, never()).executeAsync(any(Statement.class));
    }

    @Test
    void writerClosesTheSessionWhenThePrepareFails() {
        IllegalStateException prepareFailure = new IllegalStateException("prepare failed");
        when(session.prepare(anyString())).thenThrow(prepareFailure);

        CassandraParameters parameters = parameters(true, "t");
        IllegalStateException e =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                new CassandraSinkWriter(
                                        parameters,
                                        rowType(),
                                        mock(ColumnDefinitions.class),
                                        session));

        Assertions.assertSame(prepareFailure, e);
        verify(session).close();
    }

    @Test
    void failedAsyncWriteFailsTheCheckpoint() throws IOException {
        RuntimeException rejected = new RuntimeException("rejected");
        when(session.executeAsync(any(Statement.class))).thenReturn(written(), failed(rejected));
        CassandraSinkWriter writer = writer(true, "t");
        writer.write(row(1L));
        writer.write(row(2L));

        CassandraConnectorException e =
                Assertions.assertThrows(CassandraConnectorException.class, writer::prepareCommit);

        Assertions.assertTrue(
                e.getMessage().contains("1 of 2 writes to table t failed"), e.getMessage());
        Assertions.assertSame(rejected, e.getCause());
    }

    @Test
    void everyFailedAsyncWriteIsReported() throws IOException {
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");
        when(session.executeAsync(any(Statement.class)))
                .thenReturn(failed(first), written(), failed(second));
        CassandraSinkWriter writer = writer(true, "t");
        for (long id = 1; id <= 3; id++) {
            writer.write(row(id));
        }

        CassandraConnectorException e =
                Assertions.assertThrows(CassandraConnectorException.class, writer::prepareCommit);

        Assertions.assertTrue(
                e.getMessage().contains("2 of 3 writes to table t failed"), e.getMessage());
        Assertions.assertSame(first, e.getCause());
        Assertions.assertArrayEquals(new Throwable[] {second}, e.getSuppressed());
    }

    @Test
    void failedAsyncWriteFailsTheWriteThatReachesTheBatchSize() throws IOException {
        batchSize = 2;
        RuntimeException rejected = new RuntimeException("rejected");
        when(session.executeAsync(any(Statement.class))).thenReturn(failed(rejected), written());
        CassandraSinkWriter writer = writer(true, "t");
        writer.write(row(1L));

        CassandraConnectorException e =
                Assertions.assertThrows(
                        CassandraConnectorException.class, () -> writer.write(row(2L)));

        Assertions.assertTrue(
                e.getMessage().contains("1 of 2 writes to table t failed"), e.getMessage());
        Assertions.assertSame(rejected, e.getCause());
    }

    @Test
    void batchSizeOfOneWaitsForEachAsyncWrite() {
        batchSize = 1;
        RuntimeException rejected = new RuntimeException("rejected");
        when(session.executeAsync(any(Statement.class))).thenReturn(failed(rejected));
        CassandraSinkWriter writer = writer(true, "t");

        CassandraConnectorException e =
                Assertions.assertThrows(
                        CassandraConnectorException.class, () -> writer.write(row(1L)));

        Assertions.assertSame(rejected, e.getCause());
        verify(session, times(1)).executeAsync(any(Statement.class));
    }

    @Test
    void batchIsSentWhenItReachesTheBatchSize() throws IOException {
        batchSize = 2;
        CassandraSinkWriter writer = writer(false, "t");
        writer.write(row(1L));
        verify(session, never()).execute(any(Statement.class));

        writer.write(row(2L));
        verify(session, times(1)).execute(any(Statement.class));
    }

    @Test
    void checkpointPassesWhenEveryAsyncWriteSucceeded() throws IOException {
        when(session.executeAsync(any(Statement.class))).thenReturn(written());
        CassandraSinkWriter writer = writer(true, "t");
        writer.write(row(1L));

        Assertions.assertEquals(Optional.empty(), writer.prepareCommit());
        Assertions.assertEquals(Optional.empty(), writer.prepareCommit());
    }

    @Test
    void checkpointWritesTheBufferedBatch() throws IOException {
        CassandraSinkWriter writer = writer(false, "t");
        writer.write(row(1L));
        writer.write(row(2L));
        verify(session, never()).execute(any(Statement.class));

        writer.prepareCommit();
        verify(session, times(1)).execute(any(Statement.class));

        writer.prepareCommit();
        verify(session, times(1)).execute(any(Statement.class));
    }

    @Test
    void rejectedRowOfTheBatchFailsTheCheckpoint() throws IOException {
        RuntimeException rejected = new RuntimeException("rejected");
        when(session.execute(any(Statement.class))).thenThrow(rejected);
        CassandraSinkWriter writer = writer(false, "t");
        writer.write(row(1L));

        CassandraConnectorException e =
                Assertions.assertThrows(CassandraConnectorException.class, writer::prepareCommit);

        Assertions.assertTrue(
                e.getMessage().contains("Cannot write a row to table t"), e.getMessage());
        Assertions.assertSame(rejected, e.getCause());
    }

    @Test
    void closeFailsAndClosesTheSessionWhenTheLastWritesFail() throws IOException {
        RuntimeException rejected = new RuntimeException("rejected");
        when(session.executeAsync(any(Statement.class))).thenReturn(failed(rejected));
        CassandraSinkWriter writer = writer(true, "t");
        writer.write(row(1L));

        CassandraConnectorException e =
                Assertions.assertThrows(CassandraConnectorException.class, writer::close);

        Assertions.assertSame(rejected, e.getCause());
        verify(session).close();
    }

    @Test
    void abortClosesTheSessionWithoutWritingTheBufferedBatch() throws IOException {
        CassandraSinkWriter writer = writer(false, "t");
        writer.write(row(1L));

        writer.abortPrepare();
        verify(session).close();

        writer.close();
        verify(session, never()).execute(any(Statement.class));
        verify(session, times(1)).close();
    }

    @Test
    void abortAfterAFailedCheckpointClosesTheSessionOnce() throws IOException {
        when(session.executeAsync(any(Statement.class)))
                .thenReturn(failed(new RuntimeException("rejected")));
        CassandraSinkWriter writer = writer(true, "t");
        writer.write(row(1L));
        Assertions.assertThrows(CassandraConnectorException.class, writer::prepareCommit);

        writer.abortPrepare();
        writer.close();

        verify(session, times(1)).executeAsync(any(Statement.class));
        verify(session, times(1)).close();
    }

    @Test
    void abortDoesNotWaitForTheWritesInFlight() throws IOException {
        when(session.executeAsync(any(Statement.class))).thenReturn(new CompletableFuture<>());
        CassandraSinkWriter writer = writer(true, "t");
        writer.write(row(1L));

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), writer::abortPrepare);
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(10), writer::close);
        verify(session, times(1)).close();
    }

    @Test
    void abortFailsWhenTheSessionCannotBeClosed() {
        RuntimeException closeFailure = new RuntimeException("close");
        doThrow(closeFailure).when(session).close();
        CassandraSinkWriter writer = writer(true, "t");

        CassandraConnectorException e =
                Assertions.assertThrows(CassandraConnectorException.class, writer::abortPrepare);

        Assertions.assertSame(closeFailure, e.getCause());
    }

    private static SeaTunnelRow row(long id) {
        return new SeaTunnelRow(new Object[] {id, "a"});
    }

    private static CompletionStage<AsyncResultSet> written() {
        return CompletableFuture.completedFuture(mock(AsyncResultSet.class));
    }

    private static CompletionStage<AsyncResultSet> failed(Throwable error) {
        CompletableFuture<AsyncResultSet> write = new CompletableFuture<>();
        write.completeExceptionally(error);
        return write;
    }

    private void assertNullPrimaryKeyFailsTheWrite(boolean asyncWrite, String configuredTable) {
        CassandraSinkWriter writer = writer(asyncWrite, configuredTable);

        CassandraConnectorException e =
                Assertions.assertThrows(
                        CassandraConnectorException.class,
                        () -> writer.write(new SeaTunnelRow(new Object[] {null, "a"})));

        Assertions.assertEquals(
                "Primary key column id of table " + configuredTable + " is NULL",
                e.getCause().getMessage());
        verify(session, never()).executeAsync(any(Statement.class));
        verify(session, never()).execute(any(Statement.class));
    }

    private CassandraSinkWriter writer(boolean asyncWrite, String configuredTable) {
        CassandraParameters parameters = parameters(asyncWrite, configuredTable);

        ColumnDefinitions schema = mock(ColumnDefinitions.class);
        ColumnDefinition idColumn = column("id", DataTypes.BIGINT);
        ColumnDefinition textColumn = column("c", DataTypes.TEXT);
        when(schema.get(0)).thenReturn(idColumn);
        when(schema.get(1)).thenReturn(textColumn);

        PreparedStatement prepared = mock(PreparedStatement.class);
        when(session.prepare(anyString())).thenReturn(prepared);
        when(prepared.bind()).thenReturn(statement);
        when(prepared.getVariableDefinitions()).thenReturn(schema);

        ColumnMetadata key = mock(ColumnMetadata.class);
        when(key.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        TableMetadata table = mock(TableMetadata.class);
        when(table.getPrimaryKey()).thenReturn(Collections.singletonList(key));
        Metadata metadata = metadata(tableInMetadata ? table : null);
        Metadata refreshedMetadata = metadata(tableInRefreshedMetadata ? table : null);
        when(session.getMetadata()).thenReturn(metadata);
        when(session.refreshSchema()).thenReturn(refreshedMetadata);

        parameters.setBatchSize(batchSize);
        return new CassandraSinkWriter(parameters, rowType(), schema, session);
    }

    private static CassandraParameters parameters(boolean asyncWrite, String configuredTable) {
        CassandraParameters parameters = new CassandraParameters();
        parameters.setKeyspace("ks");
        parameters.setTable(configuredTable);
        parameters.setFields(Arrays.asList("id", "c"));
        parameters.setBatchSize(100);
        parameters.setBatchType(DefaultBatchType.UNLOGGED);
        parameters.setAsyncWrite(asyncWrite);
        return parameters;
    }

    private static SeaTunnelRowType rowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "c"},
                new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});
    }

    /** Driver metadata whose keyspace {@code ks} holds {@code table} as {@code t}, if not null. */
    private static Metadata metadata(TableMetadata table) {
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getTable(CqlIdentifier.fromCql("t"))).thenReturn(Optional.ofNullable(table));
        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspace(CqlIdentifier.fromCql("ks"))).thenReturn(Optional.of(keyspace));
        return metadata;
    }

    private static ColumnDefinition column(String name, DataType type) {
        ColumnDefinition column = mock(ColumnDefinition.class);
        when(column.getName()).thenReturn(CqlIdentifier.fromCql(name));
        when(column.getKeyspace()).thenReturn(CqlIdentifier.fromCql("ks"));
        when(column.getTable()).thenReturn(CqlIdentifier.fromCql("t"));
        when(column.getType()).thenReturn(type);
        return column;
    }
}
