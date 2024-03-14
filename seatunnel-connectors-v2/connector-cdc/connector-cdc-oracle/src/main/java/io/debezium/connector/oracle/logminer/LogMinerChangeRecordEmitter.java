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

package io.debezium.connector.oracle.logminer;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import oracle.sql.ROWID;

import java.util.Optional;

/**
 * Copied from Debezium 1.9.8.Final. Emits change records based on an event read from Oracle
 * LogMiner.
 *
 * <p>This class add RowId and overrides the emit methods to put rowId in the header.
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private final Operation operation;
    private final String rowId;

    public LogMinerChangeRecordEmitter(
            OracleConnectorConfig connectorConfig,
            Partition partition,
            OffsetContext offset,
            Operation operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            OracleDatabaseSchema schema,
            Clock clock,
            String rowId) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues, newValues);
        this.operation = operation;
        this.rowId = rowId;
    }

    public LogMinerChangeRecordEmitter(
            OracleConnectorConfig connectorConfig,
            Partition partition,
            OffsetContext offset,
            EventType eventType,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            OracleDatabaseSchema schema,
            Clock clock,
            String rowId) {
        this(
                connectorConfig,
                partition,
                offset,
                getOperation(eventType),
                oldValues,
                newValues,
                table,
                schema,
                clock,
                rowId);
    }

    private static Operation getOperation(EventType eventType) {
        switch (eventType) {
            case INSERT:
                return Operation.CREATE;
            case UPDATE:
            case SELECT_LOB_LOCATOR:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            default:
                throw new DebeziumException("Unsupported operation type: " + eventType);
        }
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Optional<ConnectHeaders> getEmitConnectHeaders() {
        ConnectHeaders headers = new ConnectHeaders();
        headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));
        return Optional.of(headers);
    }
}
