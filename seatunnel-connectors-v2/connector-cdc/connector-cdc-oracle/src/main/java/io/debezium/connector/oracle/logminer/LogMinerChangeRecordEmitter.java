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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;
import oracle.sql.ROWID;

import java.util.Objects;

/**
 * Copied from Debezium 1.6.4.Final.
 *
 * <p>Emits change record based on a single {@link LogMinerDmlEntry} event.
 *
 * <p>This class overrides the emit methods to put the ROWID in the header.
 *
 * <p>Line 59 ~ 257: add ROWID and emit methods.
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerChangeRecordEmitter.class);

    private final int operation;
    private final Object[] oldValues;
    private final Object[] newValues;
    private final String rowId;

    public LogMinerChangeRecordEmitter(
            OffsetContext offset,
            int operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            Clock clock) {
        super(offset, table, clock);
        this.operation = operation;
        this.oldValues = oldValues;
        this.newValues = newValues;
        this.rowId = null;
    }

    public LogMinerChangeRecordEmitter(
            OffsetContext offset,
            int operation,
            Object[] oldValues,
            Object[] newValues,
            Table table,
            Clock clock,
            String rowId) {
        super(offset, table, clock);
        this.operation = operation;
        this.oldValues = oldValues;
        this.newValues = newValues;
        this.rowId = rowId;
    }

    @Override
    protected Operation getOperation() {
        switch (operation) {
            case RowMapper.INSERT:
                return Operation.CREATE;
            case RowMapper.UPDATE:
            case RowMapper.SELECT_LOB_LOCATOR:
                return Operation.UPDATE;
            case RowMapper.DELETE:
                return Operation.DELETE;
            default:
                throw new DebeziumException("Unsupported operation type: " + operation);
        }
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver)
            throws InterruptedException {
        TableSchema tableSchema = (TableSchema) schema;
        Operation operation = getOperation();

        switch (operation) {
            case CREATE:
                emitCreateRecord(receiver, tableSchema);
                break;
            case READ:
                emitReadRecord(receiver, tableSchema);
                break;
            case UPDATE:
                emitUpdateRecord(receiver, tableSchema);
                break;
            case DELETE:
                emitDeleteRecord(receiver, tableSchema);
                break;
            case TRUNCATE:
                emitTruncateRecord(receiver, tableSchema);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    @Override
    protected void emitCreateRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .create(
                                newValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        ConnectHeaders headers = new ConnectHeaders();
        headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            // This case can be hit on UPDATE / DELETE when there's no primary key defined while
            // using certain decoders
            LOGGER.warn(
                    "no new values found for table '{}' from create message at '{}'; skipping record",
                    tableSchema,
                    getOffset().getSourceInfo());
            return;
        }
        receiver.changeRecord(
                tableSchema, Operation.CREATE, newKey, envelope, getOffset(), headers);
    }

    @Override
    protected void emitReadRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .read(
                                newValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        ConnectHeaders headers = new ConnectHeaders();
        headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));

        receiver.changeRecord(tableSchema, Operation.READ, newKey, envelope, getOffset(), headers);
    }

    @Override
    protected void emitUpdateRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            LOGGER.warn(
                    "no new values found for table '{}' from update message at '{}'; skipping record",
                    tableSchema,
                    getOffset().getSourceInfo());
            return;
        }
        // some configurations does not provide old values in case of updates
        // in this case we handle all updates as regular ones
        if (oldKey == null || Objects.equals(oldKey, newKey)) {
            Struct envelope =
                    tableSchema
                            .getEnvelopeSchema()
                            .update(
                                    oldValue,
                                    newValue,
                                    getOffset().getSourceInfo(),
                                    getClock().currentTimeAsInstant());
            ConnectHeaders headers = new ConnectHeaders();
            headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));
            receiver.changeRecord(
                    tableSchema, Operation.UPDATE, newKey, envelope, getOffset(), headers);
        }
        // PK update -> emit as delete and re-insert with new key
        else {
            ConnectHeaders headers = new ConnectHeaders();
            headers.add(PK_UPDATE_NEWKEY_FIELD, newKey, tableSchema.keySchema());

            Struct envelope =
                    tableSchema
                            .getEnvelopeSchema()
                            .delete(
                                    oldValue,
                                    getOffset().getSourceInfo(),
                                    getClock().currentTimeAsInstant());
            receiver.changeRecord(
                    tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), headers);

            headers = new ConnectHeaders();
            headers.add(PK_UPDATE_OLDKEY_FIELD, oldKey, tableSchema.keySchema());

            headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));

            envelope =
                    tableSchema
                            .getEnvelopeSchema()
                            .create(
                                    newValue,
                                    getOffset().getSourceInfo(),
                                    getClock().currentTimeAsInstant());
            receiver.changeRecord(
                    tableSchema, Operation.CREATE, newKey, envelope, getOffset(), headers);
        }
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);
        ConnectHeaders headers = new ConnectHeaders();
        headers.add(ROWID.class.getSimpleName(), new SchemaAndValue(null, rowId));

        if (skipEmptyMessages() && (oldColumnValues == null || oldColumnValues.length == 0)) {
            LOGGER.warn(
                    "no old values found for table '{}' from delete message at '{}'; skipping record",
                    tableSchema,
                    getOffset().getSourceInfo());
            return;
        }

        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .delete(
                                oldValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        receiver.changeRecord(
                tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), headers);
    }

    @Override
    protected Object[] getOldColumnValues() {
        return oldValues;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return newValues;
    }
}
