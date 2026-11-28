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

package org.apache.seatunnel.connectors.seatunnel.opendatadc;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * Defines the stable configuration keys and defaults for the bounded Open Data DC source. Source
 * construction and factory validation both reference these same option definitions.
 */
final class OpenDataDcSourceOptions {
    private OpenDataDcSourceOptions() {}

    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(500)
                    .withDescription("Number of crash records to request per page (1–1000)");
    static final Option<Long> MAX_ROWS =
            Options.key("max_rows")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Maximum number of rows to read; zero reads the full scan");

    static final Option<String> QUERY_URL =
            Options.key("query_url")
                    .stringType()
                    .defaultValue(CrashesApi.ENDPOINT)
                    .withDescription(
                            "HTTPS ArcGIS Crashes layer query URL; use the public City endpoint by default");
    static final Option<Boolean> ALLOW_INSECURE_HTTP =
            Options.key("allow_insecure_http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable plaintext HTTP for a trusted local test fixture only");
}
