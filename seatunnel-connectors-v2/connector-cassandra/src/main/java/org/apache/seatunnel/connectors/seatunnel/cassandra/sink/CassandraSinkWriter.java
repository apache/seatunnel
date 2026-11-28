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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cassandra.util.TypeConvertUtil;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class CassandraSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    /** Failed writes added as suppressed to the error, after the first one, its cause. */
    static final int MAX_SUPPRESSED_WRITE_FAILURES = 9;

    private final CassandraParameters cassandraParameters;
    private final SeaTunnelRowType seaTunnelRowType;
    private final ColumnDefinitions tableSchema;
    private final CqlSession session;
    private BatchStatement batchStatement;
    private List<BoundStatement> boundStatementList;
    private List<CompletionStage<AsyncResultSet>> completionStages;
    private final PreparedStatement preparedStatement;
    private final AtomicInteger counter = new AtomicInteger(0);

    /** Set once the session is closed, by {@link #close()} or {@link #abortPrepare()}. */
    private boolean sessionClosed;

    /**
     * The primary key columns of the target table, never empty. A NULL value for one of them is
     * rejected before anything is sent, with the table and column in the error.
     */
    private final Set<CqlIdentifier> primaryKeyColumns;

    public CassandraSinkWriter(
            CassandraParameters cassandraParameters,
            SeaTunnelRowType seaTunnelRowType,
            ColumnDefinitions tableSchema) {
        this(
                cassandraParameters,
                seaTunnelRowType,
                tableSchema,
                CassandraClient.getCqlSessionBuilder(
                                cassandraParameters.getHost(),
                                cassandraParameters.getKeyspace(),
                                cassandraParameters.getUsername(),
                                cassandraParameters.getPassword(),
                                cassandraParameters.getDatacenter())
                        .build());
    }

    /**
     * Creates a writer that owns {@code session}: {@link #close()} closes it, and so does this
     * constructor when it fails. It fails when the primary key of the target table cannot be read
     * from the driver schema metadata, also after one refresh, because a NULL key could then not be
     * rejected before the write.
     */
    CassandraSinkWriter(
            CassandraParameters cassandraParameters,
            SeaTunnelRowType seaTunnelRowType,
            ColumnDefinitions tableSchema,
            CqlSession session) {
        this.cassandraParameters = cassandraParameters;
        this.seaTunnelRowType = seaTunnelRowType;
        this.tableSchema = tableSchema;
        this.session = session;
        this.batchStatement = BatchStatement.builder(cassandraParameters.getBatchType()).build();
        this.boundStatementList = new ArrayList<>();
        this.completionStages = new ArrayList<>();
        try {
            this.preparedStatement = session.prepare(initPrepareCQL());
            this.primaryKeyColumns = readPrimaryKeyColumns();
        } catch (RuntimeException e) {
            try {
                session.close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    private Set<CqlIdentifier> readPrimaryKeyColumns() {
        // The prepared statement names the table as the server resolved it, so a configured name
        // such as keyspace.table needs no parsing here.
        ColumnDefinition insertedColumn = preparedStatement.getVariableDefinitions().get(0);
        CqlIdentifier keyspace = insertedColumn.getKeyspace();
        CqlIdentifier table = insertedColumn.getTable();
        Set<CqlIdentifier> primaryKey = primaryKeyColumns(session.getMetadata(), keyspace, table);
        if (primaryKey.isEmpty()) {
            // The driver may not have loaded the table yet, for example right after it was
            // created. A refresh reads the schema from the server, even if schema metadata is
            // disabled in the driver configuration.
            primaryKey = primaryKeyColumns(session.refreshSchema(), keyspace, table);
        }
        if (primaryKey.isEmpty()) {
            throw new CassandraConnectorException(
                    CassandraConnectorErrorCode.READ_PRIMARY_KEY_FAILED,
                    String.format(
                            "Cannot read the primary key of table %s.%s from the driver schema"
                                    + " metadata. It is needed to reject a NULL key before the"
                                    + " write.",
                            keyspace.asCql(true), table.asCql(true)));
        }
        return primaryKey;
    }

    private static Set<CqlIdentifier> primaryKeyColumns(
            Metadata metadata, CqlIdentifier keyspace, CqlIdentifier table) {
        return metadata.getKeyspace(keyspace)
                .flatMap(keyspaceMetadata -> keyspaceMetadata.getTable(table))
                .map(
                        tableMetadata ->
                                tableMetadata.getPrimaryKey().stream()
                                        .map(ColumnMetadata::getName)
                                        .collect(Collectors.toSet()))
                .orElse(Collections.emptySet());
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        BoundStatement boundStatement = this.preparedStatement.bind();
        addIntoBatch(row, boundStatement);
        // The row that reaches batch_size is flushed with the others, so that no more than
        // batch_size rows are buffered or in flight.
        if (counter.incrementAndGet() >= cassandraParameters.getBatchSize()) {
            flush();
        }
    }

    /**
     * Called at each checkpoint. Writes the rows buffered for the next batch and waits for the
     * asynchronous writes in flight, so that the checkpoint completes only when every row received
     * before it is in Cassandra, and fails when one of them could not be written.
     */
    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    /**
     * Writes the buffered batch, or waits for the asynchronous writes in flight, and throws when a
     * row could not be written. Called after {@code batch_size} rows, at each checkpoint and in
     * {@link #close()}.
     */
    private void flush() {
        counter.set(0);
        if (cassandraParameters.getAsyncWrite()) {
            waitForAsyncWrites();
        } else {
            writeBatch();
        }
    }

    private void waitForAsyncWrites() {
        if (completionStages.isEmpty()) {
            return;
        }
        int writes = completionStages.size();
        List<Throwable> failures = new ArrayList<>();
        try {
            for (CompletionStage<AsyncResultSet> write : completionStages) {
                try {
                    write.toCompletableFuture().get();
                } catch (ExecutionException e) {
                    failures.add(e.getCause());
                } catch (CancellationException e) {
                    failures.add(e);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CassandraConnectorException(
                    CassandraConnectorErrorCode.WRITE_FAILED,
                    "Interrupted while waiting for the writes to table "
                            + cassandraParameters.getTable(),
                    e);
        } finally {
            completionStages.clear();
        }
        if (!failures.isEmpty()) {
            CassandraConnectorException failure =
                    new CassandraConnectorException(
                            CassandraConnectorErrorCode.WRITE_FAILED,
                            String.format(
                                    "%d of %d writes to table %s failed; the first failure is the"
                                            + " cause",
                                    failures.size(), writes, cassandraParameters.getTable()),
                            failures.get(0));
            failures.stream()
                    .skip(1)
                    .limit(MAX_SUPPRESSED_WRITE_FAILURES)
                    .forEach(failure::addSuppressed);
            throw failure;
        }
    }

    private void writeBatch() {
        if (boundStatementList.isEmpty()) {
            return;
        }
        try {
            this.session.execute(this.batchStatement.addAll(boundStatementList));
        } catch (Exception e) {
            log.error("Batch insert error,Try inserting one by one!", e);
            for (BoundStatement statement : boundStatementList) {
                try {
                    this.session.execute(statement);
                } catch (RuntimeException rowFailure) {
                    throw new CassandraConnectorException(
                            CassandraConnectorErrorCode.WRITE_FAILED,
                            "Cannot write a row to table " + cassandraParameters.getTable(),
                            rowFailure);
                }
            }
        } finally {
            this.batchStatement.clear();
            this.boundStatementList.clear();
        }
    }

    private void addIntoBatch(SeaTunnelRow row, BoundStatement boundStatement) {
        try {
            for (int i = 0; i < cassandraParameters.getFields().size(); i++) {
                String fieldName = cassandraParameters.getFields().get(i);
                DataType dataType = tableSchema.get(i).getType();
                Object fieldValue = row.getField(seaTunnelRowType.indexOf(fieldName));
                // Reject a NULL key before anything is sent, with the table and the column.
                if (fieldValue == null
                        && primaryKeyColumns.contains(
                                preparedStatement.getVariableDefinitions().get(i).getName())) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Primary key column %s of table %s is NULL",
                                    fieldName, cassandraParameters.getTable()));
                }
                boundStatement =
                        TypeConvertUtil.reconvertAndInject(boundStatement, i, dataType, fieldValue);
            }
            if (cassandraParameters.getAsyncWrite()) {
                completionStages.add(session.executeAsync(boundStatement));
            } else {
                boundStatementList.add(boundStatement);
            }
        } catch (Exception e) {
            throw new CassandraConnectorException(
                    CassandraConnectorErrorCode.ADD_BATCH_DATA_FAILED, e);
        }
    }

    private String initPrepareCQL() {
        String[] placeholder = new String[cassandraParameters.getFields().size()];
        Arrays.fill(placeholder, "?");
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                cassandraParameters.getTable(),
                String.join(",", cassandraParameters.getFields()),
                String.join(",", placeholder));
    }

    /**
     * Called on a failed writer: by Spark when a write or {@link #prepareCommit()} throws, and by
     * Zeta when preparing a checkpoint fails. Drops the buffered rows and the writes in flight
     * without sending anything, because the engine writes them again when it retries or restores,
     * and closes the session. Spark does not call {@link #close()} after it, so the session would
     * otherwise keep its driver threads running.
     */
    @Override
    public void abortPrepare() {
        counter.set(0);
        completionStages.clear();
        boundStatementList.clear();
        closeSession();
    }

    /**
     * Writes what is left, as {@link #prepareCommit()} does, and closes the session in any case.
     * After {@link #abortPrepare()} there is nothing left to write and the session is closed.
     */
    @Override
    public void close() throws IOException {
        RuntimeException failure = null;
        try {
            flush();
        } catch (RuntimeException e) {
            failure = e;
        }
        try {
            closeSession();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void closeSession() {
        if (sessionClosed) {
            return;
        }
        sessionClosed = true;
        try {
            session.close();
        } catch (RuntimeException e) {
            throw new CassandraConnectorException(
                    CassandraConnectorErrorCode.CLOSE_CQL_SESSION_FAILED, e);
        }
    }
}
