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

import java.util.Map;

public class JsonFormatOptions {

    public static final String FAIL_ON_MISSING_FIELD = "fail_on_missing_field";

    public static final String IGNORE_PARSE_ERRORS = "ignore_parse_errors";

    public static boolean getFailOnMissingField(Map<String, String> options) {
        return Boolean.parseBoolean(options.getOrDefault(FAIL_ON_MISSING_FIELD, Boolean.FALSE.toString()));
    }

    public static boolean getIgnoreParseErrors(Map<String, String> options) {
        return Boolean.parseBoolean(options.getOrDefault(IGNORE_PARSE_ERRORS, Boolean.FALSE.toString()));
    }
}
