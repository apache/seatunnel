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

package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.seatunnel.api.source.Collector;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/**
 * Abstract class for Debezium deserialization schema.
 *
 * <p>It provides the basic functionality to serialize the table changes struct and history table
 * changes.
 *
 * @param <T>
 */
public abstract class AbstractDebeziumDeserializationSchema<T>
        implements DebeziumDeserializationSchema<T> {

    // Runtime Debezium TableId may not be Serializable, so write table identifiers as strings.
    protected final Map<TableId, byte[]> tableChangesStructMap = new TableChangesStructMap();
    protected transient JsonConverter converter;

    public AbstractDebeziumDeserializationSchema(Map<TableId, Struct> tableIdTableChangeMap) {
        this.tableChangesStructMap.putAll(
                tableIdTableChangeMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> serializeStruct(entry.getValue()))));
    }

    @Override
    public Map<TableId, byte[]> getHistoryTableChanges() {
        synchronized (tableChangesStructMap) {
            return new HashMap<>(tableChangesStructMap);
        }
    }

    @Override
    public void restoreCheckpointHistoryTableChanges(
            Map<TableId, byte[]> checkpointHistoryTableChanges) {
        if (checkpointHistoryTableChanges == null || checkpointHistoryTableChanges.isEmpty()) {
            return;
        }
        synchronized (tableChangesStructMap) {
            tableChangesStructMap.clear();
            tableChangesStructMap.putAll(checkpointHistoryTableChanges);
        }
    }

    public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
        if (isSchemaChangeEvent(record)) {
            Struct recordValue = (Struct) record.value();
            List<Struct> tableChangesStruct =
                    (List<Struct>) recordValue.get(HistoryRecord.Fields.TABLE_CHANGES);
            synchronized (tableChangesStructMap) {
                tableChangesStruct.forEach(
                        tableChangeStruct -> {
                            tableChangesStructMap.put(
                                    TableId.parse(tableChangeStruct.getString("id")),
                                    serializeStruct(tableChangeStruct));
                        });
            }
        }
    }

    private byte[] serializeStruct(Struct struct) {
        if (converter == null) {
            converter = new JsonConverter();
            Map<String, ?> configs = Collections.singletonMap("schemas.enable", true);
            converter.configure(configs, false);
        }
        return converter.fromConnectData("topic", struct.schema(), struct);
    }

    /**
     * Holds table history while replacing runtime Debezium table identifiers during Java
     * serialization.
     */
    private static final class TableChangesStructMap extends AbstractMap<TableId, byte[]>
            implements Serializable {

        private static final long serialVersionUID = 1L;

        private transient Map<TableId, byte[]> entries = new HashMap<>();

        @Override
        public byte[] put(TableId tableId, byte[] tableChangesStruct) {
            return entries.put(tableId, tableChangesStruct);
        }

        @Override
        public Set<Entry<TableId, byte[]>> entrySet() {
            return entries.entrySet();
        }

        private void writeObject(ObjectOutputStream output) throws IOException {
            synchronized (this) {
                output.defaultWriteObject();
                output.writeInt(entries.size());
                for (Entry<TableId, byte[]> entry : entries.entrySet()) {
                    output.writeUTF(entry.getKey().identifier());
                    output.writeObject(entry.getValue());
                }
            }
        }

        private void readObject(ObjectInputStream input)
                throws IOException, ClassNotFoundException {
            input.defaultReadObject();
            entries = new HashMap<>();
            int entryCount = input.readInt();
            for (int index = 0; index < entryCount; index++) {
                entries.put(TableId.parse(input.readUTF()), (byte[]) input.readObject());
            }
        }
    }
}
