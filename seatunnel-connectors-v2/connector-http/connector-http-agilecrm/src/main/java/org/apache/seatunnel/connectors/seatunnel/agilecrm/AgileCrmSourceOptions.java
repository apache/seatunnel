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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Agile CRM tenant, credential, resource and request options. */
public final class AgileCrmSourceOptions {
    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tenant origin, such as https://example.agilecrm.com.");
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Agile CRM account email used in HTTP Basic authentication.");
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Agile CRM API key used in HTTP Basic authentication.");
    public static final Option<String> RESOURCE =
            Options.key("resource")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Agile CRM list resource to scan.");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum requested page size for paginated resources (1 to 100). Ticket requests are capped at 25.");
    public static final Option<Long> EVENT_START =
            Options.key("event_start")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Start of the events range, in Unix seconds.");
    public static final Option<Long> EVENT_END =
            Options.key("event_end")
                    .longType()
                    .noDefaultValue()
                    .withDescription("End of the events range, in Unix seconds.");
    public static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Permit HTTP for an explicitly trusted and isolated endpoint.");
    public static final Option<Integer> REQUEST_TIMEOUT_MS =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total request deadline, including retries and waits, from 1 to 300000 milliseconds.");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum HTTP attempts for one request, from 1 to 8.");

    private AgileCrmSourceOptions() {}
}
