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

import org.bson.BsonDouble;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

public class MongodbSourceConstants {

    public static final String ENCODE_VALUE_FIELD = "_value";

    public static final String CLUSTER_TIME_FIELD = "clusterTime";

    public static final String TS_MS_FIELD = "ts_ms";

    public static final String SOURCE_FIELD = "source";

    public static final String SNAPSHOT_FIELD = "snapshot";

    public static final String FALSE_FALSE = "false";

    public static final String OPERATION_TYPE_INSERT = "insert";

    public static final String SNAPSHOT_TRUE = "true";

    public static final String ID_FIELD = "_id";

    public static final String HEARTBEAT_KEY_FIELD = "HEARTBEAT";

    public static final String COPY_KEY_FIELD = "copy";

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

    public static final int INVALIDATED_RESUME_TOKEN_ERROR = 260;
    public static final int CHANGE_STREAM_FATAL_ERROR = 280;
    public static final int CHANGE_STREAM_HISTORY_LOST = 286;
    public static final int BSON_OBJECT_TOO_LARGE = 10334;

    public static final Set<Integer> INVALID_CHANGE_STREAM_ERRORS =
            new HashSet<>(
                    asList(
                            INVALIDATED_RESUME_TOKEN_ERROR,
                            CHANGE_STREAM_FATAL_ERROR,
                            CHANGE_STREAM_HISTORY_LOST,
                            BSON_OBJECT_TOO_LARGE));

    public static final String RESUME_TOKEN = "resume token";
    public static final String NOT_FOUND = "not found";
    public static final String DOES_NOT_EXIST = "does not exist";
    public static final String INVALID_RESUME_TOKEN = "invalid resume token";
    public static final String NO_LONGER_IN_THE_OPLOG = "no longer be in the oplog";

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
}
