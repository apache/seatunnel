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

package org.apache.seatunnel.connectors.seatunnel.deeplake.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

public final class DeepLakeSinkOptions {

    public static final String CONNECTOR_IDENTITY = "DeepLake";

    public static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://api.deeplake.ai")
                    .withDescription("Deep Lake REST API base URL.");

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Deep Lake API key.");

    public static final Option<String> ORG_ID =
            Options.key("org_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Activeloop organization ID.");

    public static final Option<String> WORKSPACE =
            Options.key("workspace")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Deep Lake workspace containing the destination table.");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Destination table. Defaults to the input table name.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum number of rows sent in one batch request.");

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("HTTP connection timeout in milliseconds.");

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("HTTP socket timeout in milliseconds.");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("Controls whether the destination table is created.");

    private DeepLakeSinkOptions() {}
}
