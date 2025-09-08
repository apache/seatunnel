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

package org.apache.seatunnel.transform.rename;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
public class FieldRenameConfig implements Serializable {

    public static final Option<ConvertCase> CONVERT_CASE =
            Options.key("convert_case")
                    .enumType(ConvertCase.class)
                    .noDefaultValue()
                    .withDescription("Convert to uppercase or lowercase");

    public static final Option<String> PREFIX =
            Options.key("prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Add prefix for field name");

    public static final Option<String> SUFFIX =
            Options.key("suffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Add suffix for field name");

    public static final Option<List<ReplacementsWithRegex>> REPLACEMENTS_WITH_REGEX =
            Options.key("replacements_with_regex")
                    .listType(ReplacementsWithRegex.class)
                    .noDefaultValue()
                    .withDescription("The regex of replace fields name to ");

    public static final Option<List<SpecificModify>> SPECIFIC =
            Options.key("specific")
                    .listType(SpecificModify.class)
                    .noDefaultValue()
                    .withDescription("The specific modify field name");

    @JsonAlias("table_match_regex")
    private String tableMatchRegex;

    @JsonAlias("is_table_match_regex")
    private Boolean isTableMatchRegex;

    @JsonAlias("match_tables")
    private List<String> matchTables;

    @JsonAlias("convert_case")
    private ConvertCase convertCase;

    @JsonAlias("prefix")
    private String prefix;

    @JsonAlias("suffix")
    private String suffix;

    @JsonAlias("replacements_with_regex")
    private List<ReplacementsWithRegex> replacementsWithRegex;

    @JsonAlias("specific")
    private List<SpecificModify> specific;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SpecificModify implements Serializable {
        @JsonAlias("field_name")
        private String fieldName;

        @JsonAlias("target_name")
        private String targetName;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ReplacementsWithRegex implements Serializable {
        @JsonAlias("replace_from")
        private String replaceFrom;

        @JsonAlias("replace_to")
        private String replaceTo;

        @JsonAlias("is_regex")
        private Boolean isRegex;
    }

    public static FieldRenameConfig of(ReadonlyConfig config) {
        FieldRenameConfig renameConfig = new FieldRenameConfig();
        renameConfig.setConvertCase(config.get(CONVERT_CASE));
        renameConfig.setPrefix(config.get(PREFIX));
        renameConfig.setSuffix(config.get(SUFFIX));
        renameConfig.setReplacementsWithRegex(config.get(REPLACEMENTS_WITH_REGEX));
        renameConfig.setSpecific(config.get(SPECIFIC));
        return renameConfig;
    }
}
