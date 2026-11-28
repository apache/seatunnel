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

package org.apache.seatunnel.connectors.seatunnel.aha;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Arrays;
import java.util.List;

/** Options for reading bounded v1 Aha collections with tenant-scoped authentication. */
public final class AhaSourceOptions {
    public static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HTTPS origin of the account-specific Aha tenant.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Aha API key or OAuth2 access token, without the Bearer prefix.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(
                            Arrays.asList(
                                    "features",
                                    "products",
                                    "idea_categories",
                                    "ideas",
                                    "idea_endorsements",
                                    "idea_comments",
                                    "users",
                                    "goals"))
                    .withDescription("Ordered, distinct Aha resource names.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(30)
                    .withDescription("Requested records per page, up to 200.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Maximum pages per resource, parent scan or child collection.");
    public static final Option<Integer> MAX_PARENTS =
            Options.key("max_parents")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum parents per selected child resource.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(4194304)
                    .withDescription("Separate compressed and decoded response size limits.");
    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TCP connect timeout in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TLS and response read inactivity timeout in milliseconds.");
    public static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Total request deadline including retry waits.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum HTTP attempts per page including the first.");

    private AhaSourceOptions() {}
}
