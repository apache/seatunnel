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

package org.apache.seatunnel.engine.server.rest.response;

import lombok.Getter;

import java.util.List;

@Getter
public class OptionRuleResponse {

    private final String engineType;
    private final String pluginType;
    private final String pluginName;
    private final OptionRuleMetadata optionRule;

    public OptionRuleResponse(
            String engineType,
            String pluginType,
            String pluginName,
            OptionRuleMetadata optionRule) {
        this.engineType = engineType;
        this.pluginType = pluginType;
        this.pluginName = pluginName;
        this.optionRule = optionRule;
    }

    @Getter
    public static class OptionRuleMetadata {

        private final List<OptionMetadata> optionalOptions;
        private final List<RequiredOptionMetadata> requiredOptions;
        private final List<ConditionRuleMetadata> conditionRules;
        private final List<ValueConstraintMetadata> valueConstraints;

        public OptionRuleMetadata(
                List<OptionMetadata> optionalOptions,
                List<RequiredOptionMetadata> requiredOptions,
                List<ConditionRuleMetadata> conditionRules) {
            this(optionalOptions, requiredOptions, conditionRules, null);
        }

        public OptionRuleMetadata(
                List<OptionMetadata> optionalOptions,
                List<RequiredOptionMetadata> requiredOptions,
                List<ConditionRuleMetadata> conditionRules,
                List<ValueConstraintMetadata> valueConstraints) {
            this.optionalOptions = optionalOptions;
            this.requiredOptions = requiredOptions;
            this.conditionRules = conditionRules;
            this.valueConstraints = valueConstraints;
        }
    }

    @Getter
    public static class ValueConstraintMetadata {

        private final String expression;
        private final ConditionNode conditionTree;

        public ValueConstraintMetadata(String expression, ConditionNode conditionTree) {
            this.expression = expression;
            this.conditionTree = conditionTree;
        }
    }

    @Getter
    public static class ConditionRuleMetadata {

        private final String expression;
        private final ExpressionNode expressionTree;
        private final OptionRuleMetadata optionRule;

        public ConditionRuleMetadata(
                String expression, ExpressionNode expressionTree, OptionRuleMetadata optionRule) {
            this.expression = expression;
            this.expressionTree = expressionTree;
            this.optionRule = optionRule;
        }
    }

    @Getter
    public static class RequiredOptionMetadata {

        private final RuleType ruleType;
        private final List<OptionMetadata> options;
        private final String expression;
        private final ExpressionNode expressionTree;

        public RequiredOptionMetadata(
                RuleType ruleType,
                List<OptionMetadata> options,
                String expression,
                ExpressionNode expressionTree) {
            this.ruleType = ruleType;
            this.options = options;
            this.expression = expression;
            this.expressionTree = expressionTree;
        }
    }

    @Getter
    public static class OptionMetadata {

        private final String key;
        private final String type;
        private final Object defaultValue;
        private final String description;
        private final List<String> fallbackKeys;
        private final List<Object> optionValues;

        public OptionMetadata(
                String key,
                String type,
                Object defaultValue,
                String description,
                List<String> fallbackKeys,
                List<Object> optionValues) {
            this.key = key;
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
            this.fallbackKeys = fallbackKeys;
            this.optionValues = optionValues;
        }
    }

    @Getter
    public static class ExpressionNode {

        private final ConditionNode condition;
        private final LogicalOperator operator;
        private final ExpressionNode next;

        public ExpressionNode(
                ConditionNode condition, LogicalOperator operator, ExpressionNode next) {
            this.condition = condition;
            this.operator = operator;
            this.next = next;
        }
    }

    @Getter
    public static class ConditionNode {

        private final OptionMetadata option;
        private final Object expectValue;
        private final String compareOperator;
        private final OptionMetadata compareOption;
        private final String conditionOperator;
        private final String conditionOperatorCategory;
        private final LogicalOperator operator;
        private final ConditionNode next;

        public ConditionNode(
                OptionMetadata option,
                Object expectValue,
                LogicalOperator operator,
                ConditionNode next) {
            this(option, expectValue, null, null, null, null, operator, next);
        }

        public ConditionNode(
                OptionMetadata option,
                Object expectValue,
                String compareOperator,
                OptionMetadata compareOption,
                String conditionOperator,
                String conditionOperatorCategory,
                LogicalOperator operator,
                ConditionNode next) {
            this.option = option;
            this.expectValue = expectValue;
            this.compareOperator = compareOperator;
            this.compareOption = compareOption;
            this.conditionOperator = conditionOperator;
            this.conditionOperatorCategory = conditionOperatorCategory;
            this.operator = operator;
            this.next = next;
        }
    }

    public enum RuleType {
        ABSOLUTELY_REQUIRED,
        EXCLUSIVE,
        BUNDLED,
        CONDITIONAL
    }

    public enum LogicalOperator {
        AND,
        OR
    }
}
