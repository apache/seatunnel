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

package org.apache.seatunnel.connectors.seatunnel.splunk.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpCommonOptions;

public class SplunkSourceOptions extends HttpCommonOptions {
    public static final Option<String> API_KEY =
            Options.key("api_key").stringType().noDefaultValue().withDescription("Splunk API Key");

    public static final Option<Boolean> KEEP_PARAMS_AS_FORM =
            Options.key("keep_params_as_form")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Keep params as form urlencoded");

    public static final Option<Long> MAX_RESPONSE_SIZE_BYTES =
            Options.key("max_response_size_bytes")
                    .longType()
                    .defaultValue(50L * 1024 * 1024) // 50MB?
                    .withDescription(
                            "Maximum allowed HTTP response size in bytes before failing fast, to avoid unbounded in-memory buffering on large Splunk exports. "
                                    + "Narrow the search's time window or result count if you hit this limit!");
}
