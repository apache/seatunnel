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

package org.apache.seatunnel.connectors.seatunnel.mixpanel;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public final class MixpanelSourceOptions {
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Mixpanel service account username.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Mixpanel service account secret, sent only in the Basic authorization header.");
    public static final Option<Long> PROJECT_ID =
            Options.key("project_id")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Positive project ID accessible to the service account.");
    public static final Option<String> FROM_DATE =
            Options.key("from_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive export start date in strict yyyy-MM-dd format.");
    public static final Option<String> TO_DATE =
            Options.key("to_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive export end date in strict yyyy-MM-dd format.");
    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("US")
                    .withDescription("Mixpanel data residency: US, EU or IN.");
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Trusted HTTPS origin overriding the region endpoint. Receives credentials. No path, userinfo, query or fragment.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow HTTP only for trusted offline fixtures. Never enable for real credentials.");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Connection timeout in milliseconds, 1 to 300000.");
    public static final Option<Integer> READ_TIMEOUT_MS =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(120000)
                    .withDescription("Socket inactivity timeout in milliseconds, 1 to 600000.");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Retries for HTTP 429 and 5xx before consuming events, 0 to 10.");
    public static final Option<Integer> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Initial exponential retry delay in milliseconds, 1 to 60000.");
    public static final Option<Integer> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription(
                            "Maximum retry delay in milliseconds, 1 to 600000. Caps Retry-After.");
    public static final Option<Integer> MAX_EVENT_BYTES =
            Options.key("max_event_bytes")
                    .intType()
                    .defaultValue(1048576)
                    .withDescription(
                            "Maximum bytes per JSONL record, 1 to 16777216. Oversized events fail the task, never truncate.");
    public static final Option<Map<String, String>> FIELD_MAPPING =
            Options.key("field_mapping")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Schema column to RFC 6901 JSON Pointer, e.g. time = /properties/time. Empty pointer selects the raw event.");

    private MixpanelSourceOptions() {}
}
