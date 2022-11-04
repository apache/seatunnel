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

package org.apache.seatunnel.connectors.seatunnel.assertion.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;

import java.util.List;
import java.util.Map;

public class Config {

    public static final Option<AssertFieldRule.AssertRuleType> RULE_TYPE = Options.key("rule_type").enumType(AssertFieldRule.AssertRuleType.class)
        .noDefaultValue().withDescription("The rule type of the rule");

    public static final Option<Double> RULE_VALUE = Options.key("rule_value").doubleType()
        .noDefaultValue().withDescription("The value related to rule type");

    public static final Option<Map<String, Object>> ROW_RULES_MAP = Options.key("row_rules_map").mapType(RULE_TYPE, RULE_VALUE)
        .noDefaultValue();

    public static final Option<List<Map<String, Object>>> ROW_RULES = Options.key("row_rules").listType(ROW_RULES_MAP)
        .noDefaultValue().withDescription("row rules for row validation");

    public static final Option<String> FIELD_NAME = Options.key("filed_name").stringType()
        .noDefaultValue().withDescription("field name");

    public static final Option<String> FIELD_TYPE = Options.key("field_type").stringType()
        .noDefaultValue().withDescription("field type");

    public static final Option<List<Map<String, Object>>> FIELD_VALUE = Options.key("filed_value").listType(ROW_RULES_MAP)
        .noDefaultValue().withDescription("A list value rule define the data value validation");


    public static final Option<Map<String, Object>> FIELD_RULES_MAP = Options.key("field_rules_map").mapType(FIELD_NAME, FIELD_TYPE, FIELD_VALUE)
        .noDefaultValue();

    public static final Option<List<Map<String, Object>>> FIELD_RULES = Options.key("field_rules").listType(FIELD_RULES_MAP)
        .noDefaultValue().withDescription("field rules for field validation");

    public static final Option<Map<String, Object>> RULES = Options.key("rules").mapType(ROW_RULES, FIELD_RULES)
        .noDefaultValue().withDescription("Rule definition of user's available data. Each rule represents one field validation or row num validation.");


}
