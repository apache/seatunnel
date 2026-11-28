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

package org.apache.seatunnel.connectors.seatunnel.blogger;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Configuration contract for the bounded Blogger source.
 *
 * <p>Each instance selects one output resource and traverses that resource's parent collections.
 */
public final class BloggerSourceOptions {
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google OAuth application client ID.");
    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google OAuth application client secret.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Google OAuth refresh token with the Blogger read-only scope.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One of users, blogs, posts, pages, or comments.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Requested items for each paginated Google API call.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(2000)
                    .withDescription("Maximum pages for any individual Blogger list.");
    public static final Option<Integer> MAX_BLOGS =
            Options.key("max_blogs")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("Maximum entries in the non-paginated account blog response.");
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
                    .withDescription("Total page request budget in milliseconds, including OAuth.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per OAuth or data request.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8 * 1024 * 1024)
                    .withDescription("Maximum encoded and decoded size per Blogger response.");

    private BloggerSourceOptions() {}
}
