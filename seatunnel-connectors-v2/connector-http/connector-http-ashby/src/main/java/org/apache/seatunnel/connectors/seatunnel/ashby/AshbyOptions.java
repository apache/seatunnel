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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Configuration for one Ashby source.
 *
 * <p>Native pagination and the independently scoped synchronization token are separate inputs.
 */
final class AshbyOptions {
    static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Ashby resource name.");
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Ashby read-authorized API key.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive RFC 3339 bound for applications, feedback, interview schedules or application history.");
    // The nested token leaf is masked by SeaTunnel when rendering a job configuration.
    static final Option<String> SYNC_TOKEN =
            Options.key("sync.token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Opaque final token from a previous sync for this resource and scope.");
    static final Option<Integer> POLL_INTERVAL_MS =
            Options.key("poll_interval_ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Milliseconds between completed sync passes. Zero runs one bounded pass.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum requested records per page for paginated methods.");
    static final Option<Integer> MAX_REQUESTS_PER_PASS =
            Options.key("max_requests_per_pass")
                    .intType()
                    .defaultValue(100000)
                    .withDescription(
                            "Maximum logical provider requests per pass, including parent and child requests.");
    static final Option<Integer> APPLICATION_CHILD_INTERVAL_MS =
            Options.key("application_child_interval_ms")
                    .intType()
                    .defaultValue(600)
                    .withDescription(
                            "Minimum milliseconds between history or criteria evaluation requests.");
    static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes per response.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts for a retryable read.");
    static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Overall request and retry budget in milliseconds.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Explicit custom HTTPS origin paired with a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Worker-local PEM trust anchor for a custom origin.");

    private AshbyOptions() {}
}
