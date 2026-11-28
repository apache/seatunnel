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
package org.apache.seatunnel.connectors.seatunnel.algolia;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;

/** Stable configuration contract for an Algolia batch export. */
public final class AlgoliaSourceOptions {
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "HTTPS origin override, otherwise application_id.algolia.net.");
    public static final Option<String> APPLICATION_ID =
            Options.key("application_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Algolia application ID used for API authentication.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Algolia API key with access to the selected resources.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription("Unique names of resources, all five when empty.");
    public static final Option<String> SEARCH_QUERY =
            Options.key("search_query")
                    .stringType()
                    .defaultValue("hitsPerPage=2&getRankingInfo=1")
                    .withDescription(
                            "Form-encoded parameters in the search API's JSON params field.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive UTC filter for the index updatedAt and log timestamp fields.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive UTC cutoff, frozen at job start when unspecified.");
    public static final Option<Integer> INDICES_PAGE_SIZE =
            Options.key("indices_page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Number of indices per request, from 1 to 1000.");
    public static final Option<Integer> LOGS_PAGE_SIZE =
            Options.key("logs_page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum log rows per request, from 1 to 1000.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Page limit per collection or index search, from 1 to 20000.");
    public static final Option<Integer> MIN_INTERVAL_MS =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(25)
                    .withDescription(
                            "Minimum time between requests, from 10 to 60000 milliseconds.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum read attempts per page, from 1 to 8.");
    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription(
                            "Connection and read idle timeout, from 100 to 120000 milliseconds.");
    public static final Option<Integer> PAGE_DEADLINE_MS =
            Options.key("page_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Overall elapsed page deadline, from 500 to 600000 milliseconds.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription(
                            "Maximum decompressed response size, from 1024 to 33554432 bytes.");

    private AlgoliaSourceOptions() {}
}
