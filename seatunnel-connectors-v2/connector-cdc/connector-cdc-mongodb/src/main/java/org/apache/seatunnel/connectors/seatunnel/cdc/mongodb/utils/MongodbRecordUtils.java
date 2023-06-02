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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.json.JsonWriterSettings;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;

import com.mongodb.kafka.connect.source.json.formatter.DefaultJson;
import com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults;
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue;
import io.debezium.relational.TableId;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.kafka.connect.source.schema.AvroSchema.fromJson;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COLL_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DB_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;

public class MongodbRecordUtils {

    public static final String OUTPUT_SCHEMA =
            "{"
                    + "  \"name\": \"ChangeStream\","
                    + "  \"type\": \"record\","
                    + "  \"fields\": ["
                    + "    { \"name\": \"_id\", \"type\": \"string\" },"
                    + "    { \"name\": \"operationType\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"fullDocument\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"source\","
                    + "      \"type\": [{\"name\": \"source\", \"type\": \"record\", \"fields\": ["
                    + "                {\"name\": \"ts_ms\", \"type\": \"long\"},"
                    + "                {\"name\": \"snapshot\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"ts_ms\", \"type\": [\"long\", \"null\"]},"
                    + "    { \"name\": \"ns\","
                    + "      \"type\": [{\"name\": \"ns\", \"type\": \"record\", \"fields\": ["
                    + "                {\"name\": \"db\", \"type\": \"string\"},"
                    + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"to\","
                    + "      \"type\": [{\"name\": \"to\", \"type\": \"record\",  \"fields\": ["
                    + "                {\"name\": \"db\", \"type\": \"string\"},"
                    + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
                    + "               }, \"null\" ] },"
                    + "    { \"name\": \"documentKey\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"updateDescription\","
                    + "      \"type\": [{\"name\": \"updateDescription\",  \"type\": \"record\", \"fields\": ["
                    + "                 {\"name\": \"updatedFields\", \"type\": [\"string\", \"null\"]},"
                    + "                 {\"name\": \"removedFields\","
                    + "                  \"type\": [{\"type\": \"array\", \"items\": \"string\"}, \"null\"]"
                    + "                  }] }, \"null\"] },"
                    + "    { \"name\": \"clusterTime\", \"type\": [\"string\", \"null\"] },"
                    + "    { \"name\": \"txnNumber\", \"type\": [\"long\", \"null\"]},"
                    + "    { \"name\": \"lsid\", \"type\": [{\"name\": \"lsid\", \"type\": \"record\","
                    + "               \"fields\": [ {\"name\": \"id\", \"type\": \"string\"},"
                    + "                             {\"name\": \"uid\", \"type\": \"string\"}] }, \"null\"] }"
                    + "  ]"
                    + "}";

    public static boolean isHeartbeatEvent(SourceRecord sourceRecord) {
        return "true".equals(getOffsetValue(sourceRecord, "copy"));
    }

    public static boolean isDataChangeRecord(SourceRecord sourceRecord) {
        return !isWatermarkEvent(sourceRecord) && !isHeartbeatEvent(sourceRecord);
    }

    public static BsonDocument getResumeToken(SourceRecord sourceRecord) {
        return BsonDocument.parse(getOffsetValue(sourceRecord, ID_FIELD));
    }

    public static BsonDocument getDocumentKey(@NotNull SourceRecord sourceRecord) {
        Struct value = (Struct) sourceRecord.value();
        return BsonDocument.parse(value.getString(DOCUMENT_KEY));
    }

    public static String getOffsetValue(@NotNull SourceRecord sourceRecord, String key) {
        return (String) sourceRecord.sourceOffset().get(key);
    }

    public static @NotNull TableId getTableId(@NotNull SourceRecord dataRecord) {
        Struct value = (Struct) dataRecord.value();
        Struct source = value.getStruct(NS_FIELD);
        String dbName = source.getString(DB_FIELD);
        String collName = source.getString(COLL_FIELD);
        return new TableId(dbName, null, collName);
    }

    @Contract(" -> new")
    public static @NotNull BsonTimestamp currentBsonTimestamp() {
        return bsonTimestampFromEpochMillis(System.currentTimeMillis());
    }

    @Contract(value = " -> new", pure = true)
    public static @NotNull BsonTimestamp maximumBsonTimestamp() {
        return new BsonTimestamp(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Contract("_ -> new")
    public static @NotNull BsonTimestamp bsonTimestampFromEpochMillis(long epochMillis) {
        return new BsonTimestamp((int) Instant.ofEpochMilli(epochMillis).getEpochSecond(), 1);
    }

    public static @NotNull SourceRecord buildSourceRecord(
            final Map<String, String> partition,
            final Map<String, String> sourceOffset,
            final String topicName,
            final BsonDocument keyDocument,
            final BsonDocument valueDocument) {
        return buildSourceRecord(
                partition,
                sourceOffset,
                topicName,
                keyDocument,
                valueDocument,
                new DefaultJson().getJsonWriterSettings());
    }

    public static @NotNull SourceRecord buildSourceRecord(
            Map<String, String> partition,
            Map<String, String> sourceOffset,
            String topicName,
            BsonDocument keyDocument,
            BsonDocument valueDocument,
            JsonWriterSettings jsonWriterSettings) {
        BsonValueToSchemaAndValue schemaAndValue =
                new BsonValueToSchemaAndValue(jsonWriterSettings);
        SchemaAndValue keySchemaAndValue =
                schemaAndValue.toSchemaAndValue(
                        fromJson(AvroSchemaDefaults.DEFAULT_AVRO_KEY_SCHEMA), keyDocument);
        SchemaAndValue valueSchemaAndValue =
                schemaAndValue.toSchemaAndValue(fromJson(OUTPUT_SCHEMA), valueDocument);

        return new SourceRecord(
                partition,
                sourceOffset,
                topicName,
                keySchemaAndValue.schema(),
                keySchemaAndValue.value(),
                valueSchemaAndValue.schema(),
                valueSchemaAndValue.value());
    }

    public static @NotNull Map<String, String> createSourceOffsetMap(
            @NotNull BsonDocument idDocument, boolean isSnapshotRecord) {
        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, idDocument.toJson());
        sourceOffset.put("copy", String.valueOf(isSnapshotRecord));
        return sourceOffset;
    }

    public static @NotNull @Unmodifiable Map<String, String> createPartitionMap(
            String hosts, String database, String collection) {
        StringBuilder builder = new StringBuilder();
        builder.append("mongodb://");
        builder.append(hosts);
        builder.append("/");
        if (StringUtils.isNotEmpty(database)) {
            builder.append(database);
        }
        if (StringUtils.isNotEmpty(collection)) {
            builder.append(".");
            builder.append(collection);
        }
        return Collections.singletonMap(NS_FIELD, builder.toString());
    }

    @Contract(pure = true)
    public static @NotNull @Unmodifiable Map<String, Object> createHeartbeatPartitionMap(
            String hosts) {
        String builder = "mongodb://" + hosts + "/" + "__mongodb_heartbeats";
        return Collections.singletonMap(NS_FIELD, builder);
    }

    @Contract(value = "_ -> new", pure = true)
    public static @NotNull @Unmodifiable Map<String, String> createWatermarkPartitionMap(
            String partition) {
        return Collections.singletonMap(NS_FIELD, partition);
    }
}
