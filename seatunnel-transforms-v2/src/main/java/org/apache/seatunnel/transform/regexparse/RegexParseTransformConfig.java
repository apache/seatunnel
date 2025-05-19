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
package org.apache.seatunnel.transform.regexparse;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.Map;

public class RegexParseTransformConfig implements Serializable {
    private static final long serialVersionUID = -930897758226053570L;
    public static final Option<String> REGEX_PARSE_FIELD =
            Options.key("regex_parse_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Upstream field that requires parsing");
    public static final Option<String> REGEX =
            Options.key("regex")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("regular expression");
    public static final Option<Map<String, String>> GROUP_MAP =
            Options.key("groupMap")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The correspondence between result fields and regular capture group indexes");
}
