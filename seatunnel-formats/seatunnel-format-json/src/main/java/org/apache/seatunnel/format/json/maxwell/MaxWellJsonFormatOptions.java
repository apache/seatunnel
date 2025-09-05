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

package org.apache.seatunnel.format.json.maxwell;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.format.json.JsonFormatOptions;

import java.util.Map;

/** Option utils for MaxWell_json format. */
public class MaxWellJsonFormatOptions {

    public static final Option<Boolean> IGNORE_PARSE_ERRORS = JsonFormatOptions.IGNORE_PARSE_ERRORS;

    public static final Option<String> DATABASE_INCLUDE =
            Options.key("database.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific databases changelog rows by regular matching the \"database\" meta field in the MaxWell record."
                                    + "The pattern string is compatible with Java's Pattern.");

    public static final Option<String> TABLE_INCLUDE =
            Options.key("table.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific tables changelog rows by regular matching the \"table\" meta field in the MaxWell record."
                                    + "The pattern string is compatible with Java's Pattern.");

    public static String getTableInclude(Map<String, String> options) {
        return options.getOrDefault(TABLE_INCLUDE.key(), null);
    }

    public static String getDatabaseInclude(Map<String, String> options) {
        return options.getOrDefault(DATABASE_INCLUDE.key(), null);
    }

    public static boolean getIgnoreParseErrors(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(IGNORE_PARSE_ERRORS.key(), IGNORE_PARSE_ERRORS.toString()));
    }
}
