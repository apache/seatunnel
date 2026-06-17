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
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongodbIncrementalSourceOptions extends SourceOptions implements TableSchemaOptions {

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
                                    StartupMode.INITIAL, StartupMode.LATEST, StartupMode.TIMESTAMP))
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "Optional startup mode for MongoDB CDC source, valid enumerations are "
                                    + "\"initial\", \"latest\" or \"timestamp\". "
                                    + "\"initial\": reads a snapshot of the monitored collections first and then switches to the change stream. "
                                    + "\"latest\": skips the snapshot entirely and starts from the latest change-stream position, so only changes made after the job starts are captured. "
                                    + "\"timestamp\": skips the snapshot and starts reading the change stream from the position given by \"startup.timestamp\".");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .singleChoice(StopMode.class, Collections.singletonList(StopMode.NEVER))
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "Optional stop mode for CDC source, valid enumerations are "
                                    + "\"never\", \"latest\", \"timestamp\"\n or \"specific\"");
}
