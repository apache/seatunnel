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

    public static final String CONNECTOR_IDENTITY = "SNMP";

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SNMP agent host name or IP address");

    public static final Option<Integer> PORT =
            Options.key("port").intType().defaultValue(161).withDescription("SNMP agent UDP port");

    public static final Option<String> COMMUNITY =
            Options.key("community")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SNMPv2c community credential");

    public static final Option<List<String>> OIDS =
            Options.key("oids")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Numeric OIDs to retrieve with SNMP GET");

    public static final Option<Long> TIMEOUT_MILLIS =
            Options.key("timeout_millis")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("Timeout in milliseconds for each SNMP request attempt");

    public static final Option<Integer> RETRIES =
            Options.key("retries")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Number of retries after the initial SNMP request attempt");

    public static final Option<Long> POLL_INTERVAL_MILLIS =
            Options.key("poll_interval_millis")
                    .longType()
                    .defaultValue(60000L)
                    .withDescription("Interval in milliseconds between streaming polls");

    private SnmpSourceOptions() {}
}
