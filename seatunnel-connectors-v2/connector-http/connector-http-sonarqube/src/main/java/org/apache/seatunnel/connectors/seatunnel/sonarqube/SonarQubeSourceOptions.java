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

package org.apache.seatunnel.connectors.seatunnel.sonarqube;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

/**
 * Defines stable configuration keys for the bounded SonarQube source. Both factory validation and
 * source construction use these definitions without changing defaults.
 */
final class SonarQubeSourceOptions {
    private SonarQubeSourceOptions() {}

    static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SonarQube Community Build base URL including its context path, if any.");
    static final Option<String> PROJECT_KEY =
            Options.key("project_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Key of the project whose current main-branch measures are read.");
    static final Option<List<String>> METRIC_KEYS =
            Options.key("metric_keys")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Between 1 and 50 distinct SonarQube metric keys.");
    static final Option<String> TOKEN_ENV =
            Options.key("token_env")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional worker environment variable with a SonarQube user token (HTTPS only).");
}
