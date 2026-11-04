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
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CassandraSinkWriterTest {

    private final CqlSession session = mock(CqlSession.class);
    private final BoundStatement statement = mock(BoundStatement.class, RETURNS_SELF);

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
        CassandraParameters parameters = new CassandraParameters();
        parameters.setKeyspace("ks");
        parameters.setTable(configuredTable);
        parameters.setFields(Arrays.asList("id", "c"));
        parameters.setBatchSize(100);
        parameters.setBatchType(DefaultBatchType.UNLOGGED);
        parameters.setAsyncWrite(asyncWrite);

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
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getTable(CqlIdentifier.fromCql("t"))).thenReturn(Optional.of(table));
        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspace(CqlIdentifier.fromCql("ks"))).thenReturn(Optional.of(keyspace));
        when(session.getMetadata()).thenReturn(metadata);

        return new CassandraSinkWriter(
                parameters,
                new SeaTunnelRowType(
                        new String[] {"id", "c"},
                        new SeaTunnelDataType[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE}),
                schema,
                session);
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
