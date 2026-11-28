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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public final class AdobeCommerceSourceOptions {
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Traditional Commerce HTTPS deployment URL before /rest");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bearer token for the selected resource permissions");
    public static final Option<String> STORE_CODE =
            Options.key("store_code")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Optional store view code in the REST path");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(AdobeCommerceResource.names())
                    .withDescription("Resources in scan order");
    public static final Option<String> START_TIME =
            Options.key("start_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive UTC window start; omit for a full scan");
    public static final Option<String> END_TIME =
            Options.key("end_time")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive UTC window end; defaults to coordinator time");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum rows per paginated request");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum pages per paginated resource");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per page including the first");
    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription("Per-attempt connect and inactivity timeout");
    public static final Option<Integer> PAGE_DEADLINE_MS =
            Options.key("page_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Page deadline including retry and decoding");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decompressed response size");

    private AdobeCommerceSourceOptions() {}
}
