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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public final class ConfluenceSourceOptions {
    public static final Option<String> SITE_URL =
            Options.key("site_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Trusted HTTPS origin of a Confluence Cloud site.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Atlassian account login email.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Classic Atlassian API token for the specified account.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One of pages, blog_posts, space, group, or audit.");
    public static final Option<String> BODY_FORMAT =
            Options.key("body_format")
                    .stringType()
                    .defaultValue("storage")
                    .withDescription(
                            "Body representation for pages and blog posts: storage or atlas_doc_format.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(25)
                    .withDescription("Requested number of results per page (1 to 100).");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum pages per resource scan (1 to 50000).");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum HTTP attempts for a page, including the first (1 to 10).");
    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Connection timeout (1 to 300000 milliseconds).");
    public static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Socket inactivity timeout (1 to 300000 milliseconds).");
    public static final Option<Integer> PAGE_TIMEOUT =
            Options.key("page_timeout_ms")
                    .intType()
                    .defaultValue(120000)
                    .withDescription(
                            "Total page deadline including retries (1 to 600000 milliseconds).");
    public static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(16777216)
                    .withDescription("Maximum decompressed bytes per page (1 to 33554432).");

    private ConfluenceSourceOptions() {}
}
