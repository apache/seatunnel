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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Configuration contract for the Appfigures source.
 *
 * <p>Each source selects one API resource and uses a personal access token.
 */
final class AppfiguresOptions {
    static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Appfigures resource to read.");
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Appfigures personal access token.");
    static final Option<String> SEARCH_STORE =
            Options.key("search_store")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Store identifier for account products or optional report filtering.");
    static final Option<String> GROUP_BY =
            Options.key("group_by")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Comma-separated grouping pivots for a report.");
    static final Option<String> REPORT_START_DATE =
            Options.key("report_start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive report start date in YYYY-MM-DD form.");
    static final Option<String> REPORT_END_DATE =
            Options.key("report_end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive report end date in YYYY-MM-DD form.");
    static final Option<String> USER_START_TIME =
            Options.key("user_start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive local last_login cutoff for user records.");
    static final Option<String> USER_END_TIME =
            Options.key("user_end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive local last_login cutoff for user records.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Page size for users and paginated account products.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum pages in a paginated resource.");
    static final Option<Integer> MAX_ROWS =
            Options.key("max_rows")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum records in a response or paginated resource.");
    static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded response body in bytes.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum HTTP attempts per request.");
    static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Time budget in milliseconds for a request and its retries.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom HTTPS origin paired with a custom trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Worker-local PEM trust anchor for the custom origin.");

    private AppfiguresOptions() {}
}
