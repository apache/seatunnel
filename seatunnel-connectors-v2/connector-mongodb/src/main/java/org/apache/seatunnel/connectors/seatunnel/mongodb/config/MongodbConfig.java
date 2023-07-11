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

package org.apache.seatunnel.connectors.seatunnel.mongodb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.List;

public class MongodbConfig {

    public static final String CONNECTOR_IDENTITY = "MongoDB";

    public static final String ENCODE_VALUE_FIELD = "_value";

    public static final JsonWriterSettings DEFAULT_JSON_WRITER_SETTINGS =
            JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The MongoDB connection uri.");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of MongoDB database to read or write.");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of MongoDB collection to read or write.");

    public static final Option<String> MATCH_QUERY =
            Options.key("match.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Mongodb's query syntax.")
                    .withFallbackKeys("matchQuery");

    public static final Option<String> PROJECTION =
            Options.key("match.projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fields projection by Mongodb.");

    public static final Option<String> SPLIT_KEY =
            Options.key("partition.split-key")
                    .stringType()
                    .defaultValue("_id")
                    .withDescription("The key of Mongodb fragmentation.");

    public static final Option<Long> SPLIT_SIZE =
            Options.key("partition.split-size")
                    .longType()
                    .defaultValue(64 * 1024 * 1024L)
                    .withDescription("The size of Mongodb fragment.");

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch.size")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Set the number of documents obtained from the server for each batch. Setting the appropriate batch size can improve query performance and avoid the memory pressure caused by obtaining a large amount of data at one time.");

    public static final Option<Boolean> CURSOR_NO_TIMEOUT =
            Options.key("cursor.no-timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to true to prevent that. However, if the application takes longer than 30 minutes to process the current batch of documents, the session is marked as expired and closed.");

    public static final Option<Long> MAX_TIME_MIN =
            Options.key("max.time-min")
                    .longType()
                    .defaultValue(600L)
                    .withDescription(
                            "This parameter is a MongoDB query option that limits the maximum execution time for query operations. The value of maxTimeMS is in milliseconds. If the execution time of the query exceeds the specified time limit, MongoDB will terminate the operation and return an error.");

    public static final Option<Boolean> FLAT_SYNC_STRING =
            Options.key("flat.sync-string")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "By utilizing flatSyncString, only one field attribute value can be set, and the field type must be a String. This operation will perform a string mapping on a single MongoDB data entry.");

    // --------------------------------------------------------------------
    // The following are the sink parameters.
    // --------------------------------------------------------------------

    public static final Option<Integer> BUFFER_FLUSH_MAX_ROWS =
            Options.key("buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Specifies the maximum number of buffered rows per batch request.");

    public static final Option<Long> BUFFER_FLUSH_INTERVAL =
            Options.key("buffer-flush.interval")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Specifies the maximum interval of buffered rows per batch request, the unit is millisecond.");

    public static final Option<Integer> RETRY_MAX =
            Options.key("retry.max")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Specifies the max number of retry if writing records to database failed.");

    public static final Option<Long> RETRY_INTERVAL =
            Options.key("retry.interval")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Specifies the retry time interval if writing records to database failed.");

    public static final Option<Boolean> UPSERT_ENABLE =
            Options.key("upsert-enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to write documents via upsert mode.");

    public static final Option<List<String>> PRIMARY_KEY =
            Options.key("primary-key")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The primary keys for upsert/update. Keys are in csv format for properties.")
                    .withFallbackKeys("upsert-key");

    public static final Option<Boolean> TRANSACTION =
            Options.key("transaction").booleanType().defaultValue(false).withDescription(".");
}
