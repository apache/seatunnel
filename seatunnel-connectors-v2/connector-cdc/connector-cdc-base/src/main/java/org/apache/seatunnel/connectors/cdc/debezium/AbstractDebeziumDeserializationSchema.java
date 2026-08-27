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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    protected final Map<TableId, byte[]> tableChangesStructMap = new HashMap<>();
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
        return new HashMap<>(tableChangesStructMap);
    }

    public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
        if (isSchemaChangeEvent(record)) {
            Struct recordValue = (Struct) record.value();
            List<Struct> tableChangesStruct =
                    (List<Struct>) recordValue.get(HistoryRecord.Fields.TABLE_CHANGES);
            tableChangesStruct.forEach(
                    tableChangeStruct -> {
                        tableChangesStructMap.put(
                                TableId.parse(tableChangeStruct.getString("id")),
                                serializeStruct(tableChangeStruct));
                    });
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
}
