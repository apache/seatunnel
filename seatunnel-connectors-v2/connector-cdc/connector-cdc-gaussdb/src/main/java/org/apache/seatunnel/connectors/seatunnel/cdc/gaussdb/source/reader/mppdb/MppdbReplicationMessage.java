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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.source.reader.mppdb;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.pgoutput.PgOutputReplicationMessage;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/** Adapts normalized mppdb rows to Debezium's PostgreSQL change-record emitter contract. */
public final class MppdbReplicationMessage implements ReplicationMessage {

    /** Normalized mppdb row change. */
    private final MppdbWalChange change;

    /** PostgreSQL type registry loaded from the active GaussDB-compatible connection. */
    private final TypeRegistry typeRegistry;

    /** Event time used by Debezium source metadata. */
    private final Instant eventTime;

    /** Creates a Debezium message for one mppdb row change. */
    public MppdbReplicationMessage(
            MppdbWalChange change, TypeRegistry typeRegistry, Instant eventTime) {
        this.change = change;
        this.typeRegistry = typeRegistry;
        this.eventTime = eventTime;
    }

    /** Maps the mppdb DML operation to Debezium's replication operation. */
    @Override
    public Operation getOperation() {
        switch (change.getType()) {
            case INSERT:
                return Operation.INSERT;
            case UPDATE:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            default:
                throw new IllegalArgumentException(
                        "mppdb transaction record cannot be emitted as DML: " + change.getType());
        }
    }

    /** Returns the event timestamp assigned when the record was decoded. */
    @Override
    public Instant getCommitTime() {
        return eventTime;
    }

    /** Returns the transaction id when mppdb included one. */
    @Override
    public OptionalLong getTransactionId() {
        return change.getTransactionId() == 0
                ? OptionalLong.empty()
                : OptionalLong.of(change.getTransactionId());
    }

    /** Returns the PostgreSQL-compatible qualified table name. */
    @Override
    public String getTable() {
        return change.getSchema() + "." + change.getTable();
    }

    /** Returns replica identity values for UPDATE or DELETE. */
    @Override
    public List<Column> getOldTupleList() {
        return adaptColumns(change.getOldColumns());
    }

    /** Returns complete new values for INSERT or UPDATE. */
    @Override
    public List<Column> getNewTupleList() {
        return adaptColumns(change.getNewColumns());
    }

    /** mppdb includes a type identity but not Debezium length and scale metadata. */
    @Override
    public boolean hasTypeMetadata() {
        return false;
    }

    /** Each normalized object is complete for its own LSN. */
    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    /**
     * Schema evolution is intentionally disabled for mppdb because it emits no RELATION records.
     */
    @Override
    public boolean shouldSchemaBeSynchronized() {
        return false;
    }

    /** Converts normalized values into lazy Debezium column resolvers. */
    private List<Column> adaptColumns(List<MppdbWalChange.ColumnValue> columns) {
        return columns.stream().map(MppdbColumn::new).collect(Collectors.toList());
    }

    /** A lazily converted mppdb column consumed by {@link PgOutputReplicationMessage}. */
    private final class MppdbColumn implements Column {

        /** Normalized source value. */
        private final MppdbWalChange.ColumnValue value;

        /** Database type resolved by OID or canonical type name. */
        private final PostgresType postgresType;

        /** Creates a Debezium column and resolves its database type. */
        private MppdbColumn(MppdbWalChange.ColumnValue value) {
            this.value = value;
            this.postgresType =
                    value.getTypeOid() == 0
                            ? typeRegistry.get(TypeRegistry.normalizeTypeName(value.getTypeName()))
                            : typeRegistry.get(value.getTypeOid());
        }

        /** Returns the decoded column name. */
        @Override
        public String getName() {
            return value.getName();
        }

        /** Returns the database type resolved through Debezium's registry. */
        @Override
        public PostgresType getType() {
            return postgresType;
        }

        /** mppdb does not transmit type length and scale metadata. */
        @Override
        public ColumnTypeMetadata getTypeMetadata() {
            throw new UnsupportedOperationException(
                    "mppdb_decoding does not provide column type metadata");
        }

        /** Converts the PostgreSQL text representation to the Java value expected by Debezium. */
        @Override
        public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
            if (value.isNullValue()) {
                return null;
            }
            return PgOutputReplicationMessage.getValue(
                    value.getName(),
                    postgresType,
                    postgresType.getName(),
                    value.getValue(),
                    connection,
                    includeUnknownDatatypes,
                    typeRegistry);
        }

        /** Nullability comes from the live table schema rather than mppdb protocol metadata. */
        @Override
        public boolean isOptional() {
            return true;
        }
    }
}
