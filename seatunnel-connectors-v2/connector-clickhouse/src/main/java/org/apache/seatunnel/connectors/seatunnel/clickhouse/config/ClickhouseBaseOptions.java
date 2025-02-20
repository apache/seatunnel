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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

public class ClickhouseBaseOptions {

    /** Clickhouse server host */
    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server host");

    /** Clickhouse database name */
    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse database name");

    /** Clickhouse server username */
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server username");

    /** Clickhouse server password */
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server password");

    /** Clickhouse server timezone */
    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server_time_zone")
                    .stringType()
                    .defaultValue(ZoneId.systemDefault().getId())
                    .withDescription(
                            "The session time zone in database server."
                                    + "If not set, then ZoneId.systemDefault() is used to determine the server time zone");

    public static final Option<Map<String, String>> CLICKHOUSE_CONFIG =
            Options.key("clickhouse.config")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Clickhouse custom config");
}
