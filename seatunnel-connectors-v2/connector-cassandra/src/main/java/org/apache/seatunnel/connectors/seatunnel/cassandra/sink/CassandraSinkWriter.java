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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cassandra.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

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
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class CassandraSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final CassandraParameters cassandraParameters;
    private final SeaTunnelRowType seaTunnelRowType;
    private final ColumnDefinitions tableSchema;
    private final CqlSession session;
    private BatchStatement batchStatement;
    private List<BoundStatement> boundStatementList;
    private List<CompletionStage<AsyncResultSet>> completionStages;
    private final PreparedStatement preparedStatement;
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * The primary key columns of the target table, never empty. A NULL value for one of them is
     * rejected before the write: the server would reject the write, and with {@code async_write} a
     * rejected write is only logged, so the job would succeed without the row.
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
                                    + " write, because a write that the server rejects with"
                                    + " async_write does not fail the job.",
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
        if (counter.getAndIncrement() >= cassandraParameters.getBatchSize()) {
            flush();
            counter.set(0);
        }
    }

    private void flush() {
        if (cassandraParameters.getAsyncWrite()) {
            completionStages.forEach(
                    resultStage ->
                            resultStage.whenComplete(
                                    (resultSet, error) -> {
                                        if (error != null) {
                                            log.error(ExceptionUtils.getMessage(error));
                                        }
                                    }));
            completionStages.clear();
        } else {
            try {
                this.session.execute(this.batchStatement.addAll(boundStatementList));
            } catch (Exception e) {
                log.error("Batch insert error,Try inserting one by one!", e);
                for (BoundStatement statement : boundStatementList) {
                    this.session.execute(statement);
                }
            } finally {
                this.batchStatement.clear();
                this.boundStatementList.clear();
            }
        }
    }

    private void addIntoBatch(SeaTunnelRow row, BoundStatement boundStatement) {
        try {
            for (int i = 0; i < cassandraParameters.getFields().size(); i++) {
                String fieldName = cassandraParameters.getFields().get(i);
                DataType dataType = tableSchema.get(i).getType();
                Object fieldValue = row.getField(seaTunnelRowType.indexOf(fieldName));
                // The server rejects a NULL key, but a failed write does not fail the job, so stop
                // here instead of losing the row.
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

    @Override
    public void close() throws IOException {
        flush();
        try {
            if (this.session != null) {
                this.session.close();
            }
        } catch (Exception e) {
            throw new CassandraConnectorException(
                    CassandraConnectorErrorCode.CLOSE_CQL_SESSION_FAILED, e);
        }
    }
}
