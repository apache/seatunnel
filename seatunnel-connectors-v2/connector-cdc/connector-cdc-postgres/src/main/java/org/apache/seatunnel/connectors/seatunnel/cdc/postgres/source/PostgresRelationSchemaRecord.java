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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.Table;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;

import java.util.List;
import java.util.Map;

/** Creates an in-memory schema record from a PostgreSQL pgoutput RELATION message. */
public final class PostgresRelationSchemaRecord {

    public static final String KEY_SCHEMA_NAME = "io.debezium.connector.postgresql.SchemaChangeKey";

    private static final String TABLE_ID = "table";

    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct()
                    .name(KEY_SCHEMA_NAME)
                    .field(TABLE_ID, Schema.STRING_SCHEMA)
                    .build();

    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .name("io.debezium.connector.postgresql.SchemaChangeValue")
                    .field(
                            HistoryRecord.Fields.TABLE_CHANGES,
                            SchemaBuilder.array(ConnectTableChangeSerializer.CHANGE_SCHEMA).build())
                    .build();

    private PostgresRelationSchemaRecord() {}

    /** Serialize a pgoutput RELATION table as a Debezium-compatible in-memory schema record. */
    public static SourceRecord create(
            Table table,
            Map<String, ?> sourcePartition,
            Map<String, ?> sourceOffset,
            String topic) {
        TableChanges tableChanges = new TableChanges();
        tableChanges.alter(table);
        List<Struct> serializedChanges = new ConnectTableChangeSerializer().serialize(tableChanges);

        Struct key = new Struct(KEY_SCHEMA).put(TABLE_ID, table.id().toDoubleQuotedString());
        Struct value =
                new Struct(VALUE_SCHEMA).put(HistoryRecord.Fields.TABLE_CHANGES, serializedChanges);

        return new SourceRecord(
                sourcePartition, sourceOffset, topic, KEY_SCHEMA, key, VALUE_SCHEMA, value);
    }
}
