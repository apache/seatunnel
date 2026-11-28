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

package org.apache.seatunnel.connectors.seatunnel.matomo;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Defines the stable configuration keys and defaults for the bounded Matomo source. Source
 * construction and factory validation both reference these same option definitions.
 */
final class MatomoSourceOptions {
    private MatomoSourceOptions() {}

    static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "HTTP(S) Matomo Reporting API URL, including index.php when needed");
    static final Option<Integer> SITE_ID =
            Options.key("site_id").intType().noDefaultValue().withDescription("Matomo website ID");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("First report date, inclusive, YYYY-MM-DD");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Last report date, inclusive, YYYY-MM-DD");
    static final Option<String> TOKEN_AUTH =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional private Matomo token; only sent in an HTTPS form body");
    static final Option<Integer> TIMEOUT_MS =
            Options.key("timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription("HTTP connect/read timeout, 1 to 120000 milliseconds");
}
