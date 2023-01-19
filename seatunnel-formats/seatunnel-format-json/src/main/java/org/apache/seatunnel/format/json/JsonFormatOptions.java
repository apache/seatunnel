/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class JsonFormatOptions {

    public static final Option<Boolean> IGNORE_PARSE_ERRORS =
            Options.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static final Option<Boolean> FAIL_ON_MISSING_FIELD =
            Options.key("fail-on-missing-field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static boolean getFailOnMissingField(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(
                        FAIL_ON_MISSING_FIELD.key(), FAIL_ON_MISSING_FIELD.toString()));
    }

    public static boolean getIgnoreParseErrors(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(IGNORE_PARSE_ERRORS.key(), IGNORE_PARSE_ERRORS.toString()));
    }

    public static final int GENERATE_ROW_SIZE = 3;

    public static final Option<Boolean> SCHEMA_INCLUDE =
            Options.key("schema-include")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When setting up a Debezium Kafka Connect, users can enable "
                                    + "a Kafka configuration 'value.converter.schemas.enable' to include schema in the message. "
                                    + "This option indicates the Debezium JSON data include the schema in the message or not. "
                                    + "Default is false.");
}
