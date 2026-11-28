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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;

public final class AkeneoSourceOptions {
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HTTPS origin of the Akeneo PIM instance.");
    public static final Option<String> CLIENT_ID =
            Options.key("client_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Client ID of the Akeneo Connection.");
    public static final Option<String> CLIENT_SECRET =
            Options.key("client_secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Client secret of the Akeneo Connection.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("API username of the Akeneo Connection.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("API password of the Akeneo Connection.");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription("Resource names, or all when empty.");
    public static final Option<String> MEASUREMENT_API =
            Options.key("measurement_api")
                    .stringType()
                    .defaultValue("current")
                    .withDescription(
                            "current uses measurement-families; legacy uses the paginated measure-families API.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum results per paged request, from 1 to 100.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum pages per collection or family, from 1 to 100000.");
    public static final Option<Integer> REQUEST_INTERVAL_MS =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(25)
                    .withDescription(
                            "Minimum interval between requests, from 10 to 60000 milliseconds.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per operation, from 1 to 8.");
    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription(
                            "Connect/read inactivity timeout, from 100 to 120000 milliseconds.");
    public static final Option<Integer> PAGE_DEADLINE_MS =
            Options.key("page_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Deadline for a complete page including authorization and retries, from 500 to 600000 milliseconds.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription(
                            "Maximum decompressed response size, from 1024 to 33554432 bytes.");

    private AkeneoSourceOptions() {}
}
