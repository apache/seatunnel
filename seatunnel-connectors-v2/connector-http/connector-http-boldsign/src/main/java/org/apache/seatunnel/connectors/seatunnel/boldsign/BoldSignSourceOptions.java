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

package org.apache.seatunnel.connectors.seatunnel.boldsign;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Options for the bounded BoldSign source.
 *
 * <p>One configured source instance reads one list resource from one assigned region.
 */
public final class BoldSignSourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BoldSign API key for the X-API-KEY request header.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One of the supported BoldSign read-only resource names.");
    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("us")
                    .withDescription("Region of the BoldSign account: us, eu, ca, or au.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional inclusive local creation-time filter, in whole-second ISO 8601.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional inclusive local creation-time upper bound, in whole-second ISO 8601.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Requested numbered page size. Document pages must divide 10000 evenly.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "Maximum pages per resource traversal, including cursor-anchored pages.");
    public static final Option<Integer> MAX_UNPAGED_RECORDS =
            Options.key("max_unpaged_records")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum rows returned by one unpaginated list.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum TCP connection timeout in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Maximum socket read timeout in milliseconds.");
    public static final Option<Integer> REQUEST_DEADLINE_MS =
            Options.key("request_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Total request and retry budget in milliseconds.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum request attempts, including the first.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8 * 1024 * 1024)
                    .withDescription("Maximum encoded and decoded response size.");

    private BoldSignSourceOptions() {}
}
