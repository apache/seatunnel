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

package org.apache.seatunnel.connectors.seatunnel.udp;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

/** User-facing listener, datagram format and resource limits. */
public final class UdpSourceOptions {
    public static final Option<List<Integer>> PORTS =
            Options.key("ports")
                    .listType(Integer.class)
                    .noDefaultValue()
                    .withDescription(
                            "Distinct listener ports. Each port is owned by one source reader.");
    public static final Option<String> BIND_HOST =
            Options.key("bind_host")
                    .stringType()
                    .defaultValue("0.0.0.0")
                    .withDescription("Numeric local IPv4 or IPv6 address on which readers listen.");
    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .defaultValue("TEXT")
                    .withDescription("TEXT, BINARY, SYSLOG, COLLECTD, or NETFLOW_V5.");
    public static final Option<String> SEPARATOR =
            Options.key("separator")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Optional literal UTF-8 delimiter inside each TEXT or BINARY datagram.");
    public static final Option<Integer> MAX_DATAGRAM_BYTES =
            Options.key("max_datagram_bytes")
                    .intType()
                    .defaultValue(65507)
                    .withDescription(
                            "Maximum accepted UDP payload bytes. Larger datagrams are discarded or fail the job.");
    public static final Option<Integer> RECEIVE_BUFFER_BYTES =
            Options.key("receive_buffer_bytes")
                    .intType()
                    .defaultValue(262144)
                    .withDescription("Requested OS receive-queue size in bytes for each port.");
    public static final Option<Integer> MAX_DATAGRAMS_PER_POLL =
            Options.key("max_datagrams_per_poll")
                    .intType()
                    .defaultValue(32)
                    .withDescription(
                            "Maximum datagrams one source reader can consume in one engine poll.");
    public static final Option<Integer> MAX_RECORDS_PER_DATAGRAM =
            Options.key("max_records_per_datagram")
                    .intType()
                    .defaultValue(256)
                    .withDescription("Maximum decoded records from one datagram.");
    public static final Option<Integer> IDLE_TIMEOUT_MS =
            Options.key("idle_timeout_ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Inactivity in milliseconds after which a port finishes. Zero keeps listening.");
    public static final Option<String> ON_MALFORMED =
            Options.key("on_malformed")
                    .stringType()
                    .defaultValue("DROP")
                    .withDescription(
                            "DROP or FAIL for a malformed, oversized, or over-expanded datagram.");

    private UdpSourceOptions() {}
}
