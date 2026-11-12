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

package org.apache.seatunnel.connectors.seatunnel.intercom.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

/** Config options specific to the Intercom source, on top of the shared HTTP options. */
public class IntercomSourceOptions extends HttpCommonOptions {
    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Intercom access token (raw token, no Bearer prefix)");

    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .defaultValue("https://api.intercom.io")
                    .withDescription(
                            "Intercom API base URL. Use https://api.eu.intercom.io or"
                                    + " https://api.au.intercom.io for workspaces hosted in those"
                                    + " regions.");

    public static final Option<String> API_VERSION =
            Options.key("api_version")
                    .stringType()
                    .defaultValue("2.17")
                    .withDescription("Value sent in the Intercom-Version header");

    public static final Option<Integer> PER_PAGE =
            Options.key("per_page")
                    .intType()
                    .defaultValue(150)
                    .withDescription("Contacts requested per page (1 to 150)");
}
