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

public class MongodbSourceOptions extends MongodbBaseOptions {

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
                    .defaultValue(10L)
                    .withDescription(
                            "This parameter is a MongoDB query option that limits the maximum execution time for query operations. The value of maxTimeMin is in minutes. If the execution time of the query exceeds the specified time limit, MongoDB will terminate the operation and return an error.");

    public static final Option<Boolean> FLAT_SYNC_STRING =
            Options.key("flat.sync-string")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "By utilizing flatSyncString, only one field attribute value can be set, and the field type must be a String. This operation will perform a string mapping on a single MongoDB data entry.");
}
