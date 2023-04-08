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

package org.apache.seatunnel.transform.replace;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

public class ReplaceTransformConfig implements Serializable {

    public static final Option<String> KEY_REPLACE_FIELD =
            Options.key("replace_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field you want to replace");

    public static final Option<String> KEY_PATTERN =
            Options.key("pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The old string that will be replaced");

    public static final Option<String> KEY_REPLACEMENT =
            Options.key("replacement")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The new string for replace");

    public static final Option<Boolean> KEY_IS_REGEX =
            Options.key("is_regex")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Use regex for string match");

    public static final Option<Boolean> KEY_REPLACE_FIRST =
            Options.key("replace_first")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Replace the first match string");
}
