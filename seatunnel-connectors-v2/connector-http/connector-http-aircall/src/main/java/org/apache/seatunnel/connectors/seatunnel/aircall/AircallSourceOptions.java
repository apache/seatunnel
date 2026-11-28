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
package org.apache.seatunnel.connectors.seatunnel.aircall;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;

public final class AircallSourceOptions {
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aircall Dashboard API ID");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Aircall Dashboard API token");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .defaultValue("https://api.aircall.io")
                    .withDescription("HTTPS origin before /v1");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription("Resources to read; empty selects all enabled resources");
    public static final Option<Boolean> INCLUDE_CONVERSATION_INTELLIGENCE =
            Options.key("include_conversation_intelligence")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable the six conversation intelligence resources");
    public static final Option<String> START_TIME =
            Options.key("start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional UTC history start for calls, contacts and conversations");
    public static final Option<String> END_TIME =
            Options.key("end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional exclusive UTC history end; default is the coordinator start");
    public static final Option<Integer> LOOKBACK_DAYS =
            Options.key("lookback_days")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Re-read overlap for calls and conversations");
    public static final Option<Integer> CALL_WINDOW_DAYS =
            Options.key("call_window_days")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Initial acquisition window for calls");
    public static final Option<Integer> CONTACT_WINDOW_DAYS =
            Options.key("contact_window_days")
                    .intType()
                    .defaultValue(30)
                    .withDescription("Initial acquisition window for contacts");
    public static final Option<Integer> CONVERSATION_WINDOW_DAYS =
            Options.key("conversation_window_days")
                    .intType()
                    .defaultValue(30)
                    .withDescription("Initial acquisition window for conversations");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription("Standard and campaign pages, up to 50 records");
    public static final Option<Integer> SMS_PAGE_SIZE =
            Options.key("sms_page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("SMS templates per page, up to 100");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum pages per acquisition partition");
    public static final Option<Integer> REQUEST_INTERVAL_MS =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(550)
                    .withDescription("Minimum time between requests from this source reader");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per page including the initial request");
    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription("Per-attempt connect and read inactivity timeout");
    public static final Option<Integer> PAGE_DEADLINE_MS =
            Options.key("page_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Elapsed page budget including retries and decoding");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Decompressed response byte limit");

    private AircallSourceOptions() {}
}
