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

package org.apache.seatunnel.flink.assertion;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.seatunnel.flink.assertion.rule.AssertFieldRule;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * AssertExecutor is used to determine whether a row data is available
 * It can not only be used in AssertSink, but also other Sink plugin
 * (stateless Object)
 */
public class AssertExecutor {
    /**
     * determine whether a row data is available
     * @param row row data
     * @param assertFieldRules definition of user's available data
     * @return the first rule that can NOT pass, it will be null if pass through all rules
     */
    public Optional<AssertFieldRule> fail(Row row, List<AssertFieldRule> assertFieldRules) {
            return assertFieldRules.stream()
                    .filter(assertFieldRule -> !pass(row, assertFieldRule))
                    .findFirst();
    }

    private boolean pass(Row row, AssertFieldRule assertFieldRule) {
        Object value = row.getField(assertFieldRule.getFieldName());
        if (Objects.isNull(value))
            return Boolean.FALSE;
        Boolean typeChecked = checkType(value, assertFieldRule.getFieldType());
        if (Boolean.FALSE.equals(typeChecked)) {
            return Boolean.FALSE;
        }
        Boolean valueChecked = checkValue(value, assertFieldRule.getFieldValueRules());
        if (Boolean.FALSE.equals(valueChecked)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean checkValue(Object value, List<AssertFieldRule.AssertValueRule> fieldValueRules) {
        if (CollectionUtils.isNotEmpty(fieldValueRules)) {
            AssertFieldRule.AssertValueRule failValueRule = fieldValueRules.stream()
                    .filter(valueRule -> !pass(value, valueRule))
                    .findFirst()
                    .orElse(null);
            if (Objects.nonNull(failValueRule)) {
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    private boolean pass(Object value, AssertFieldRule.AssertValueRule valueRule) {
        if (AssertFieldRule.AssertValueRuleType.NOT_NULL.equals(valueRule.getFieldValueRuleType())) {
            return Objects.nonNull(value);
        }

        if (value instanceof Number && AssertFieldRule.AssertValueRuleType.MAX.equals(valueRule.getFieldValueRuleType())){
            return ((Number) value).doubleValue() <= valueRule.getFieldValueRuleValue();
        }
        if (value instanceof Number && AssertFieldRule.AssertValueRuleType.MIN.equals(valueRule.getFieldValueRuleType())){
            return ((Number) value).doubleValue() >= valueRule.getFieldValueRuleValue();
        }

        String valueStr = Objects.isNull(value) ? StringUtils.EMPTY : String.valueOf(value);
        if (AssertFieldRule.AssertValueRuleType.MAX_LENGTH.equals(valueRule.getFieldValueRuleType())) {
            return valueStr.length()<=valueRule.getFieldValueRuleValue();
        }

        if (AssertFieldRule.AssertValueRuleType.MIN_LENGTH.equals(valueRule.getFieldValueRuleType())) {
            return valueStr.length()>=valueRule.getFieldValueRuleValue();
        }
        return Boolean.TRUE;
    }

    private Boolean checkType(Object value, TypeInformation<?> fieldType) {
        return value.getClass().equals(fieldType.getTypeClass());
    }
}
