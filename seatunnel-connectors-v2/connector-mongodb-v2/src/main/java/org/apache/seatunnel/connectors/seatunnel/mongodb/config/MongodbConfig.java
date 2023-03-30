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
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.List;
import java.util.Map;

public class MongodbConfig {

    public static final String CONNECTOR_IDENTITY = "Mongodb";

    public static final Option<String> CONNECTION =
            Options.key("connection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "In addition to the above parameters that must be specified by the Kafka producer or consumer client, "
                                    + "the user can also specify multiple non-mandatory parameters for the producer or consumer client, "
                                    + "covering all the producer parameters specified in the official Kafka document.");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kafka topic name. If there are multiple topics, use , to split, for example: \"tpc1,tpc2\".");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If pattern is set to true,the regular expression for a pattern of topic names to read from."
                                    + " All topics in clients with names that match the specified regular expression will be subscribed by the consumer.");

    public static final Option<String> MATCHQUERY =
            Options.key("matchquery")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka cluster address, separated by \",\".");

    public static final Option<Config> SCHEMA =
            Options.key("schema")
                    .objectType(Config.class)
                    .noDefaultValue()
                    .withDescription(
                            "The structure of the data, including field names and field types.");

    public static final Option<String> SPLIT_KEY =
            Options.key("split.key")
                    .stringType()
                    .defaultValue("_id")
                    .withDescription(
                            "The structure of the data, including field names and field types.");

    public static final Option<Long> SPLIT_SIZE =
            Options.key("split.size")
                    .longType()
                    .defaultValue(64 * 1024 * 1024L)
                    .withDescription(
                            "The structure of the data, including field names and field types.");

    public static final Option<String> PROJECTION =
            Options.key("projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The structure of the data, including field names and field types.");
}
