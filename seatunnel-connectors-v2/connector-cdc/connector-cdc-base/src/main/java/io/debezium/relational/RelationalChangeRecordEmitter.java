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

package io.debezium.relational;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

import java.util.Objects;
import java.util.Optional;

/**
 * Copied from Debezium 1.9.8.Final.
 *
 * <p>Base class for {@link ChangeRecordEmitter} implementations based on a relational database.
 *
 * <p>This class overrides the emit methods to put some values in the header.
 *
 * <p>Line 59 ~ 257: add other headers and emit.
 */
public abstract class RelationalChangeRecordEmitter<P extends Partition>
        extends AbstractChangeRecordEmitter<P, TableSchema> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RelationalChangeRecordEmitter.class);

    public static final String PK_UPDATE_OLDKEY_FIELD = "__debezium.oldkey";
    public static final String PK_UPDATE_NEWKEY_FIELD = "__debezium.newkey";

    public RelationalChangeRecordEmitter(P partition, OffsetContext offsetContext, Clock clock) {
        super(partition, offsetContext, clock);
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver<P> receiver)
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
    protected void emitCreateRecord(Receiver<P> receiver, TableSchema tableSchema)
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
                getPartition(),
                tableSchema,
                Operation.CREATE,
                newKey,
                envelope,
                getOffset(),
                getEmitConnectHeaders().orElse(null));
    }

    @Override
    protected void emitReadRecord(Receiver<P> receiver, TableSchema tableSchema)
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

        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.READ,
                newKey,
                envelope,
                getOffset(),
                getEmitConnectHeaders().orElse(null));
    }

    @Override
    protected void emitUpdateRecord(Receiver<P> receiver, TableSchema tableSchema)
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
            receiver.changeRecord(
                    getPartition(),
                    tableSchema,
                    Operation.UPDATE,
                    newKey,
                    envelope,
                    getOffset(),
                    getEmitConnectHeaders().orElse(null));
        }
        // PK update -> emit as delete and re-insert with new key
        else {
            emitUpdateAsPrimaryKeyChangeRecord(
                    receiver, tableSchema, oldKey, newKey, oldValue, newValue);
        }
    }

    @Override
    protected void emitDeleteRecord(Receiver<P> receiver, TableSchema tableSchema)
            throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

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
                getPartition(),
                tableSchema,
                Operation.DELETE,
                oldKey,
                envelope,
                getOffset(),
                getEmitConnectHeaders().orElse(null));
    }

    protected void emitTruncateRecord(Receiver<P> receiver, TableSchema schema)
            throws InterruptedException {
        throw new UnsupportedOperationException("TRUNCATE not supported");
    }

    /** Returns the old row state in case of an UPDATE or DELETE. */
    protected abstract Object[] getOldColumnValues();

    /** Returns the new row state in case of a CREATE or READ. */
    protected abstract Object[] getNewColumnValues();

    /**
     * Whether empty data messages should be ignored.
     *
     * @return true if empty data messages coming from data source should be ignored. Typical use
     *     case are PostgreSQL changes without FULL replica identity.
     */
    protected boolean skipEmptyMessages() {
        return false;
    }

    protected void emitUpdateAsPrimaryKeyChangeRecord(
            Receiver<P> receiver,
            TableSchema tableSchema,
            Struct oldKey,
            Struct newKey,
            Struct oldValue,
            Struct newValue)
            throws InterruptedException {
        ConnectHeaders headers = getEmitConnectHeaders().orElse(new ConnectHeaders());
        headers.add(PK_UPDATE_NEWKEY_FIELD, newKey, tableSchema.keySchema());

        Struct envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .delete(
                                oldValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.DELETE,
                oldKey,
                envelope,
                getOffset(),
                headers);

        headers = getEmitConnectHeaders().orElse(new ConnectHeaders());
        headers.add(PK_UPDATE_OLDKEY_FIELD, oldKey, tableSchema.keySchema());

        envelope =
                tableSchema
                        .getEnvelopeSchema()
                        .create(
                                newValue,
                                getOffset().getSourceInfo(),
                                getClock().currentTimeAsInstant());
        receiver.changeRecord(
                getPartition(),
                tableSchema,
                Operation.CREATE,
                newKey,
                envelope,
                getOffset(),
                headers);
    }

    protected Optional<ConnectHeaders> getEmitConnectHeaders() {
        return Optional.empty();
    }
}
