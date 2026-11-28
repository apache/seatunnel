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

package org.apache.seatunnel.connectors.seatunnel.braze;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Options for a bounded Braze export.
 *
 * <p>Each source block reads one resource with one workspace REST API key.
 */
public final class BrazeSourceOptions {
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Braze REST API key with permissions for the selected export.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Braze export resource name.");
    public static final Option<String> INSTANCE =
            Options.key("instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Braze REST instance associated with the workspace dashboard, for example us-01.");
    public static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive first UTC analytics day or exclusive last-edit threshold for campaigns, canvases and cards.");
    public static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive last UTC analytics day. Defaults to the current UTC day at scan start.");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "Maximum numbered pages in the selected list or its parent inventory.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum TCP connect time in milliseconds.");
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Maximum socket read timeout in milliseconds.");
    public static final Option<Integer> REQUEST_DEADLINE_MS =
            Options.key("request_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Total request budget in milliseconds, including retry delays.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum request attempts, including the first.");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8 * 1024 * 1024)
                    .withDescription("Maximum encoded and decoded response size in bytes.");

    private BrazeSourceOptions() {}
}
