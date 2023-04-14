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

import java.time.Duration;

public class MongodbConfig {

    public static final String CONNECTOR_IDENTITY = "Mongodb";

    public static final Option<String> CONNECTION =
            Options.key("connection")
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

    public static final Option<String> MATCHQUERY =
            Options.key("matchquery")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Mongodb's query syntax");

    public static final Option<String> SPLIT_KEY =
            Options.key("split.key")
                    .stringType()
                    .defaultValue("_id")
                    .withDescription("The key of Mongodb fragmentation");

    public static final Option<Long> SPLIT_SIZE =
            Options.key("split.size")
                    .longType()
                    .defaultValue(64 * 1024 * 1024L)
                    .withDescription("The size of Mongodb fragment");

    public static final Option<Integer> PARTITION_SAMPLES =
            Options.key("partition.samples")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Fields projection by Mongodb");

    public static final Option<String> PROJECTION =
            Options.key("projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fields projection by Mongodb");

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch.size")
                    .intType()
                    .defaultValue(2048)
                    .withDescription(
                            "Set the number of documents obtained from the server for each batch. Setting the appropriate batch size can improve query performance and avoid the memory pressure caused by obtaining a large amount of data at one time.");

    public static final Option<Boolean> CURSO_NO_TIMEOUT =
            Options.key("cursor.no-timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "MongoDB server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to true to prevent that. However, if the application takes longer than 30 minutes to process the current batch of documents, the session is marked as expired and closed.");

    // --------------------------------------------------------------------
    // The following are the sink parameters.
    // --------------------------------------------------------------------

    public static final Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Specifies the max retry times if writing records to database failed.");

    public static final Option<Duration> SINK_RETRY_INTERVAL =
            Options.key("sink.retry.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000L))
                    .withDescription(
                            "Specifies the retry time interval if writing records to database failed.");
}
