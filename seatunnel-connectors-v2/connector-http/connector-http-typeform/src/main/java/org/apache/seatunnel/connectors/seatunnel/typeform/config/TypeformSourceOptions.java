/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.typeform.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

/** Config options specific to the Typeform source, on top of the shared HTTP options. */
public class TypeformSourceOptions extends HttpCommonOptions {
    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Typeform personal access token with the responses:read scope (raw"
                                    + " token, no Bearer prefix)");

    public static final Option<String> FORM_ID =
            Options.key("form_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ID of the form whose responses are read");

    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .defaultValue("https://api.typeform.com")
                    .withDescription(
                            "Typeform API base URL. Accounts on Typeform's EU data center use"
                                    + " https://api.eu.typeform.com or https://api.typeform.eu.");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Responses requested per page (1 to 1000)");

    public static final Option<String> SINCE =
            Options.key("since")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Only read responses since this time, inclusive. Unix seconds or ISO"
                                    + " 8601 UTC, passed to Typeform as is.");

    public static final Option<String> UNTIL =
            Options.key("until")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Only read responses until this time, inclusive. Unix seconds or ISO"
                                    + " 8601 UTC, passed to Typeform as is.");

    public static final Option<String> RESPONSE_TYPE =
            Options.key("response_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated response types, for example partial,completed."
                                    + " Typeform returns completed responses when this is not set.");
}
