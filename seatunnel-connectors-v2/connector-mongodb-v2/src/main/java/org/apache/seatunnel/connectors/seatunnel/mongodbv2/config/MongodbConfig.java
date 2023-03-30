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

package org.apache.seatunnel.connectors.seatunnel.mongodbv2.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class MongodbConfig {

    public static final String CONNECTOR_IDENTITY = "MongodbV2";

    public static final Option<String> CONNECTION =
            Options.key("connection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Link string of Mongodb");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Mongodb's database name");

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The collection name of Mongodb");

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

    public static final Option<String> PROJECTION =
            Options.key("projection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Fields projection by Mongodb");
}
