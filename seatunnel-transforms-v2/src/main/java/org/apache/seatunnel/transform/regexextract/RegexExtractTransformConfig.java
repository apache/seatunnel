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

package org.apache.seatunnel.transform.regexextract;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class RegexExtractTransformConfig implements Serializable {
    public static final String PLUGIN_NAME = "RegexExtract";

    public static final Option<String> KEY_REGEX_PATTERN =
            Options.key("regex_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Regex pattern with capture groups");

    public static final Option<String> KEY_SOURCE_FIELD =
            Options.key("source_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Source field to extract from");

    public static final Option<List<String>> KEY_OUTPUT_FIELDS =
            Options.key("output_fields")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("Output field names for extracted groups");

    public static final Option<List<String>> KEY_DEFAULT_VALUES =
            Options.key("default_values")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription(
                            "Default values for output fields when regex pattern does not match");

    private String regexPattern;
    private String sourceField;
    private List<String> outputFields;
    private final List<String> defaultValues;

    public RegexExtractTransformConfig(
            String sourceField,
            String regexPattern,
            List<String> outputFields,
            List<String> defaultValues) {
        this.sourceField = sourceField;
        this.regexPattern = regexPattern;
        this.outputFields = outputFields;
        this.defaultValues = defaultValues;
    }

    public static RegexExtractTransformConfig of(ReadonlyConfig config) {
        return new RegexExtractTransformConfig(
                config.get(KEY_SOURCE_FIELD),
                config.get(KEY_REGEX_PATTERN),
                config.get(KEY_OUTPUT_FIELDS),
                config.get(KEY_DEFAULT_VALUES));
    }
}
