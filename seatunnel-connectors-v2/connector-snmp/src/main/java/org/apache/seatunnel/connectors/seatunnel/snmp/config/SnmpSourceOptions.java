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

package org.apache.seatunnel.connectors.seatunnel.snmp.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public final class SnmpSourceOptions {

    public static final String CONNECTOR_IDENTITY = SnmpOptions.CONNECTOR_IDENTITY;

    public static final Option<String> HOST = SnmpOptions.HOST;

    public static final Option<Integer> PORT = SnmpOptions.PORT;

    public static final Option<String> COMMUNITY = SnmpOptions.COMMUNITY;

    public static final Option<List<String>> OIDS =
            Options.key("oids")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Numeric OIDs to retrieve with SNMP GET");

    public static final Option<Long> TIMEOUT_MILLIS = SnmpOptions.TIMEOUT_MILLIS;

    public static final Option<Integer> RETRIES = SnmpOptions.RETRIES;

    public static final Option<Long> POLL_INTERVAL_MILLIS =
            Options.key("poll_interval_millis")
                    .longType()
                    .defaultValue(60000L)
                    .withDescription("Interval in milliseconds between streaming polls");

    private SnmpSourceOptions() {}
}
