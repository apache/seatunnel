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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import org.bson.BsonDouble;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongodbSourceOptions extends SourceOptions {

    public static final String ENCODE_VALUE_FIELD = "_value";

    public static final String CLUSTER_TIME_FIELD = "clusterTime";

    public static final String TS_MS_FIELD = "ts_ms";

    public static final String SOURCE_FIELD = "source";

    public static final String SNAPSHOT_FIELD = "snapshot";

    public static final String FALSE_FALSE = "false";

    public static final String OPERATION_TYPE_INSERT = "insert";

    public static final String SNAPSHOT_TRUE = "true";

    public static final String ID_FIELD = "_id";

    public static final String DOCUMENT_KEY = "documentKey";

    public static final String NS_FIELD = "ns";

    public static final String OPERATION_TYPE = "operationType";

    public static final String TIMESTAMP_FIELD = "timestamp";

    public static final String RESUME_TOKEN_FIELD = "resumeToken";

    public static final String FULL_DOCUMENT = "fullDocument";

    public static final String DB_FIELD = "db";

    public static final String COLL_FIELD = "coll";

    public static final int FAILED_TO_PARSE_ERROR = 9;

    public static final int UNAUTHORIZED_ERROR = 13;

    public static final int ILLEGAL_OPERATION_ERROR = 20;

    public static final int UNKNOWN_FIELD_ERROR = 40415;

    public static final String DROPPED_FIELD = "dropped";

    public static final String MAX_FIELD = "max";

    public static final String MIN_FIELD = "min";

    public static final String ADD_NS_FIELD_NAME = "_ns_";

    public static final String UUID_FIELD = "uuid";

    public static final String SHARD_FIELD = "shard";

    public static final String DIALECT_NAME = "MongoDB";

    public static final BsonDouble COMMAND_SUCCEED_FLAG = new BsonDouble(1.0d);

    public static final JsonWriterSettings DEFAULT_JSON_WRITER_SETTINGS =
            JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

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
                    + "                {\"name\": \"table\", \"type\": [\"string\", \"null\"]},"
                    + "                {\"name\": \"db\", \"type\": [\"string\", \"null\"]},"
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

    public static final Option<String> HOSTS =
            Options.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The comma-separated list of hostname and port pairs of the MongoDB servers. "
                                    + "eg. localhost:27017,localhost:27018");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database user to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to be used when connecting to MongoDB. "
                                    + "This is required only when MongoDB is configured to use authentication.");

    public static final Option<List<String>> DATABASE =
            Options.key("database")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Name of the database to watch for changes.");

    public static final Option<List<String>> COLLECTION =
            Options.key("collection")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the collection in the database to watch for changes.");

    public static final Option<String> CONNECTION_OPTIONS =
            Options.key("connection.options")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The ampersand-separated MongoDB connection options. "
                                    + "eg. replicaSet=test&connectTimeoutMS=300000");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The cursor batch size. Defaults to 1024.");

    public static final Option<Integer> POLL_MAX_BATCH_SIZE =
            Options.key("poll.max.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Maximum number of change stream documents "
                                    + "to include in a single batch when polling for new data. "
                                    + "This setting can be used to limit the amount of data buffered internally in the connector. "
                                    + "Defaults to 1024.");

    public static final Option<Integer> POLL_AWAIT_TIME_MILLIS =
            Options.key("poll.await.time.ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The amount of time to wait before checking for new results on the change stream."
                                    + "Defaults: 1000.");

    public static final Option<Integer> HEARTBEAT_INTERVAL_MILLIS =
            Options.key("heartbeat.interval.ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The length of time in milliseconds between sending heartbeat messages."
                                    + "Heartbeat messages contain the post batch resume token and are sent when no source records "
                                    + "have been published in the specified interval. This improves the resumability of the connector "
                                    + "for low volume namespaces. Use 0 to disable. Defaults to 0.");

    public static final Option<Integer> INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB =
            Options.key("incremental.snapshot.chunk.size.mb")
                    .intType()
                    .defaultValue(64)
                    .withDescription(
                            "The chunk size mb of incremental snapshot. Defaults to 64mb.");

    public static final Option<Map<String, String>> DEBEZIUM_PROPERTIES =
            Options.key("debezium")
                    .mapType()
                    .defaultValue(
                            new HashMap<String, String>() {
                                {
                                    put("key.converter.schemas.enable", "false");
                                    put("value.converter.schemas.enable", "false");
                                }
                            })
                    .withDescription(
                            "Decides if the table options contains Debezium client properties that start with prefix 'debezium'.");

    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class,
                            Arrays.asList(
                                    StartupMode.INITIAL, StartupMode.EARLIEST, StartupMode.LATEST))
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "Optional startup mode for CDC source, valid enumerations are "
                                    + "\"initial\", \"earliest\", \"latest\", \"timestamp\"\n or \"specific\"");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .singleChoice(StopMode.class, Collections.singletonList(StopMode.NEVER))
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "Optional stop mode for CDC source, valid enumerations are "
                                    + "\"never\", \"latest\", \"timestamp\"\n or \"specific\"");
}
