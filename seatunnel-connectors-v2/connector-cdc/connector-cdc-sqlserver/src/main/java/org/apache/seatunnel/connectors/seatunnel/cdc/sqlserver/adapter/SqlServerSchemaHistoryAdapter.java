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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.adapter;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumSchemaHistory;
import org.apache.seatunnel.connectors.cdc.base.debezium.TableChangeInfo;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

import io.debezium.relational.history.TableChanges;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlServerSchemaHistoryAdapter implements DebeziumSchemaHistory {

    private final String instanceName;
    private final Collection<TableChangeInfo> initialChanges;
    private final ConnectTableChangeSerializer tableChangeSerializer;
    private final JsonConverter jsonConverter;

    public SqlServerSchemaHistoryAdapter(String instanceName, Collection<TableChangeInfo> changes) {
        this.instanceName = instanceName;
        this.initialChanges = changes;
        this.tableChangeSerializer = new ConnectTableChangeSerializer();
        this.jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", true), false);
    }

    @Override
    public void registerHistory(String instanceName, Collection<TableChangeInfo> changes) {
        Collection<TableChanges.TableChange> debeziumChanges =
                changes.stream()
                        .map(this::convertToDebeziumTableChange)
                        .collect(Collectors.toList());

        EmbeddedDatabaseHistory.registerHistory(instanceName, debeziumChanges);
    }

    @Override
    public Collection<TableChangeInfo> removeHistory(String instanceName) {
        Collection<TableChanges.TableChange> debeziumChanges =
                EmbeddedDatabaseHistory.removeHistory(instanceName);

        return debeziumChanges.stream()
                .map(this::convertFromDebeziumTableChange)
                .collect(Collectors.toList());
    }

    @Override
    public void configure(Map<String, ?> config) {}

    @Override
    public void start() {
        if (initialChanges != null && !initialChanges.isEmpty()) {
            registerHistory(instanceName, initialChanges);
        }
    }

    @Override
    public void stop() {
        removeHistory(instanceName);
    }

    private TableChanges.TableChange convertToDebeziumTableChange(TableChangeInfo info) {
        SchemaAndValue schemaAndValue =
                jsonConverter.toConnectData("topic", info.getSerializedTableSchema());
        Struct deserializedStruct = (Struct) schemaAndValue.value();

        TableChanges tableChanges =
                tableChangeSerializer.deserialize(
                        Collections.singletonList(deserializedStruct), false);

        Iterator<TableChanges.TableChange> iterator = tableChanges.iterator();
        TableChanges.TableChange tableChange = null;
        while (iterator.hasNext()) {
            if (tableChange != null) {
                throw new IllegalStateException("The table changes should only have one element");
            }
            tableChange = iterator.next();
        }

        if (tableChange == null) {
            throw new IllegalStateException("No table change found in deserialized data");
        }

        return tableChange;
    }

    private TableChangeInfo convertFromDebeziumTableChange(TableChanges.TableChange change) {
        TableChangeInfo.TableChangeType type = convertChangeType(change.getType());

        TableChanges tableChanges = new TableChanges();
        tableChanges.create(change.getTable());
        List<Struct> serializedStructs = tableChangeSerializer.serialize(tableChanges);

        byte[] serialized;
        if (serializedStructs.isEmpty()) {
            serialized = new byte[0];
        } else {
            Struct struct = serializedStructs.get(0);
            serialized = jsonConverter.fromConnectData("topic", struct.schema(), struct);
        }

        return new TableChangeInfo(change.getId(), type, serialized);
    }

    private TableChangeInfo.TableChangeType convertChangeType(TableChanges.TableChangeType type) {
        switch (type) {
            case CREATE:
                return TableChangeInfo.TableChangeType.CREATE;
            case ALTER:
                return TableChangeInfo.TableChangeType.ALTER;
            case DROP:
                return TableChangeInfo.TableChangeType.DROP;
            default:
                throw new IllegalArgumentException("Unknown table change type: " + type);
        }
    }
}
