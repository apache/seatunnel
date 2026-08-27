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

package org.apache.seatunnel.connectors.seatunnel.graphql.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

import java.util.Map;

public class GraphQLSourceOptions extends HttpCommonOptions {
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final int DEFAULT_RETRY_DELAY_MS = 5000;

    public static final Option<String> QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("GraphQL query");

    public static final Option<Map<String, Object>> VARIABLES =
            Options.key("variables")
                    .mapObjectType()
                    .defaultValue(null)
                    .withDescription("GraphQL variables");

    public static final Option<Boolean> ENABLE_SUBSCRIPTION =
            Options.key("enable_subscription")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable subscription mode");

    public static final Option<Long> TIMEOUT =
            Options.key("timeout").longType().noDefaultValue().withDescription("Time-out Period");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(DEFAULT_MAX_RETRIES)
                    .withDescription("default value is " + DEFAULT_MAX_RETRIES + ", max retries");

    public static final Option<Integer> RETRY_DELAY_MS =
            Options.key("retry_delay_ms")
                    .intType()
                    .defaultValue(DEFAULT_RETRY_DELAY_MS)
                    .withDescription(
                            "default value is "
                                    + DEFAULT_RETRY_DELAY_MS
                                    + ", retry delay in milliseconds");
}
