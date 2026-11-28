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

package org.apache.seatunnel.connectors.seatunnel.jira.sink.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Defines the Jira Cloud command contract and finite transport/retry budgets for each mutation.
 * Options are independent of the existing Source and do not imply transactional checkpoint commits.
 */
public final class JiraSinkOptions {

    private JiraSinkOptions() {}

    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Trusted Jira Cloud origin, for example https://example.atlassian.net.");
    public static final Option<Integer> API_VERSION =
            Options.key("api_version")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Jira Cloud REST API version. Only version 3 is supported.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Atlassian account email for HTTP Basic authentication.");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Atlassian API token for the configured Jira Cloud site.");
    public static final Option<Operation> OPERATION =
            Options.key("operation")
                    .enumType(Operation.class)
                    .noDefaultValue()
                    .withDescription("Issue mutation for every row: CREATE, UPDATE, or DELETE.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow HTTP only for explicitly trusted, isolated development endpoints.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total milliseconds per row, including connection, I/O, and throttling delays (1 to 300000).");
    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Maximum retries for UPDATE explicitly rejected with HTTP 429 and Retry-After (0 to 10).");

    /**
     * Selects the single mutation applied to each INSERT command row, with no cascading deletion.
     */
    public enum Operation {
        CREATE,
        UPDATE,
        DELETE
    }
}
