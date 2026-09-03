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

package org.apache.seatunnel.connectors.seatunnel.firebase.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.util.Map;

public class FirebaseSourceOptions {
    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The base URL of the Firebase Realtime Database.");
    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JSON node path to read from.");
    public static final Option<Map<String, Object>> SCHEMA = ConnectorCommonOptions.SCHEMA;
    public static final Option<String> SERVICE_ACCOUNT_PATH =
            Options.key("service_account_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to the Google Service Account JSON key file.");
    public static final Option<String> CREDENTIALS =
            Options.key("credentials")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Base64-encoded Service Account JSON credentials.");
    public static final Option<String> DATABASE_SECRET =
            Options.key("database_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Legacy Firebase database secret key or Web API token.");

    public static final Option<Integer> TIMEOUT_MS =
            Options.key("timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "HTTP connection and read timeout in milliseconds (must be > 0).");

    public static final Option<Map<String, String>> QUERY_PARAMS =
            Options.key("query_params")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Additional REST API query parameters.");
}
