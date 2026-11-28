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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Arrays;
import java.util.List;

/** Options for reading bounded sets of 100ms resources. */
public final class OneHundredMsSourceOptions {
    public static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://api.100ms.live")
                    .withDescription("Trusted HTTPS API origin.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An existing 100ms management token, without the Bearer prefix.");
    public static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("100ms app access key for generating management tokens.");
    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("100ms app secret for generating management tokens.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(
                            Arrays.asList(
                                    "sessions",
                                    "rooms",
                                    "active_room_peers",
                                    "templates",
                                    "template_settings",
                                    "templates_destinations",
                                    "analytics_events",
                                    "recordings"))
                    .withDescription(
                            "Ordered resource names. Defaults to all eight provider resources.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive ISO-8601 UTC update-time lower bound for rooms, templates and recordings.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Page size for list resources (10 to 100).");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Maximum pages per list, parent discovery scan, or event snapshot.");
    public static final Option<Integer> MAX_PARENTS =
            Options.key("max_parents")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum distinct parents traversed per child resource.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription("Maximum bytes of either wire or decoded response body.");
    public static final Option<Integer> MAX_RECORDS =
            Options.key("max_records_per_response")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "Maximum records accepted in one response, including peer maps.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TCP connection timeout, in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TLS and response inactivity timeout, in milliseconds.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Total time budget across a request and all retries.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum HTTP attempts per response, including the first.");

    private OneHundredMsSourceOptions() {}
}
