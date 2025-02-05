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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchemaDispatcher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions.TableIdentifierOptions.DATABASE_NAME;
import static org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions.TableIdentifierOptions.SCHEMA_NAME;
import static org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions.TableIdentifierOptions.TABLE_NAME;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.DEBEZIUM_RECORD_TABLE_FILTER;

public class KafkaSourceConfigTest {

    @Test
    void testDebeziumJsonDeserializationSchemaDispatcher() {
        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("id", "int");
        schemaFields.put("name", "string");
        schemaFields.put("description", "string");
        schemaFields.put("weight", "string");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);

        Map<String, Object> debeziumRecordTableFilter = new HashMap<>();
        debeziumRecordTableFilter.put(DATABASE_NAME.key(), "test");
        debeziumRecordTableFilter.put(SCHEMA_NAME.key(), "test");
        debeziumRecordTableFilter.put(TABLE_NAME.key(), "test");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("group.id", "test");
        configMap.put("topic", "test");
        configMap.put("schema", schema);
        configMap.put("format", "debezium_json");
        configMap.put(DEBEZIUM_RECORD_TABLE_FILTER.key(), debeziumRecordTableFilter);

        KafkaSourceConfig sourceConfig = new KafkaSourceConfig(ReadonlyConfig.fromMap(configMap));

        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                sourceConfig.getMapMetadata().get(TablePath.of("test")).getDeserializationSchema();
        Assertions.assertTrue(
                deserializationSchema instanceof DebeziumJsonDeserializationSchemaDispatcher);
        Assertions.assertNotNull(
                ((DebeziumJsonDeserializationSchemaDispatcher) deserializationSchema)
                        .getTableDeserializationMap()
                        .get(TablePath.of("test.test.test")));
    }
}
