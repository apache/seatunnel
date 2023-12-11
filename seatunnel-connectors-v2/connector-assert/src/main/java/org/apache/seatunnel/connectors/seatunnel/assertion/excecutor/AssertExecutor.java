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

package org.apache.seatunnel.connectors.seatunnel.assertion.excecutor;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorException;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * AssertExecutor is used to determine whether a row data is available It can not only be used in
 * AssertSink, but also other Sink plugin (stateless Object)
 */
public class AssertExecutor {
    /**
     * determine whether a rowData data is available
     *
     * @param rowData row data
     * @param rowType row type
     * @param assertFieldRules definition of user's available data
     * @return the first rule that can NOT pass, it will be null if pass through all rules
     */
    public Optional<AssertFieldRule> fail(
            SeaTunnelRow rowData,
            SeaTunnelRowType rowType,
            List<AssertFieldRule> assertFieldRules) {
        return assertFieldRules.stream()
                .filter(assertFieldRule -> !pass(rowData, rowType, assertFieldRule))
                .findFirst();
    }

    private boolean pass(
            SeaTunnelRow rowData, SeaTunnelRowType rowType, AssertFieldRule assertFieldRule) {
        if (Objects.isNull(rowData)) {
            return Boolean.FALSE;
        }
        int index =
                Iterables.indexOf(
                        Lists.newArrayList(rowType.getFieldNames()),
                        fieldName -> fieldName.equals(assertFieldRule.getFieldName()));

        Object value = rowData.getField(index);
        if (Objects.isNull(value)) {
            return Boolean.FALSE;
        }
        Boolean typeChecked = checkType(value, assertFieldRule.getFieldType());
        if (Boolean.FALSE.equals(typeChecked)) {
            return Boolean.FALSE;
        }
        Boolean valueChecked = checkValue(value, assertFieldRule.getFieldRules());
        if (Boolean.FALSE.equals(valueChecked)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean checkValue(Object value, List<AssertFieldRule.AssertRule> fieldValueRules) {
        Optional<AssertFieldRule.AssertRule> failValueRule =
                fieldValueRules.stream().filter(valueRule -> !pass(value, valueRule)).findFirst();
        if (failValueRule.isPresent()) {
            return Boolean.FALSE;
        } else {
            return Boolean.TRUE;
        }
    }

    private boolean pass(Object value, AssertFieldRule.AssertRule valueRule) {
        if (AssertFieldRule.AssertRuleType.NOT_NULL.equals(valueRule.getRuleType())) {
            return Objects.nonNull(value);
        }

        if (value instanceof Number
                && AssertFieldRule.AssertRuleType.MAX.equals(valueRule.getRuleType())) {
            return ((Number) value).doubleValue() <= valueRule.getRuleValue();
        }
        if (value instanceof Number
                && AssertFieldRule.AssertRuleType.MIN.equals(valueRule.getRuleType())) {
            return ((Number) value).doubleValue() >= valueRule.getRuleValue();
        }
        if (valueRule.getEqualTo() != null) {
            return compareValue(value, valueRule);
        }
        String valueStr = Objects.isNull(value) ? StringUtils.EMPTY : String.valueOf(value);
        if (AssertFieldRule.AssertRuleType.MAX_LENGTH.equals(valueRule.getRuleType())) {
            return valueStr.length() <= valueRule.getRuleValue();
        }

        if (AssertFieldRule.AssertRuleType.MIN_LENGTH.equals(valueRule.getRuleType())) {
            return valueStr.length() >= valueRule.getRuleValue();
        }
        return Boolean.TRUE;
    }

    private boolean compareValue(Object value, AssertFieldRule.AssertRule valueRule) {
        if (value instanceof String) {
            return value.equals(valueRule.getEqualTo());
        } else if (value instanceof Integer) {
            return value.equals(Integer.parseInt(valueRule.getEqualTo()));
        } else if (value instanceof Long) {
            return value.equals(Long.parseLong(valueRule.getEqualTo()));
        } else if (value instanceof Short) {
            return value.equals(Short.parseShort(valueRule.getEqualTo()));
        } else if (value instanceof Float) {
            return value.equals((Float.parseFloat(valueRule.getEqualTo())));
        } else if (value instanceof Byte) {
            return value.equals((Byte.parseByte(valueRule.getEqualTo())));
        } else if (value instanceof Double) {
            return value.equals(Double.parseDouble(valueRule.getEqualTo()));
        } else if (value instanceof BigDecimal) {
            return value.equals(new BigDecimal(valueRule.getEqualTo()));
        } else if (value instanceof Boolean) {
            return value.equals(Boolean.parseBoolean(valueRule.getEqualTo()));
        } else if (value instanceof LocalDateTime) {
            TemporalAccessor parsedTimestamp =
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(valueRule.getEqualTo());
            LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
            LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
            return ((LocalDateTime) value).isEqual(LocalDateTime.of(localDate, localTime));
        } else if (value instanceof LocalDate) {
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            return ((LocalDate) value).isEqual(LocalDate.parse(valueRule.getEqualTo(), fmt));
        } else if (value instanceof LocalTime) {
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss");
            return value.equals(LocalTime.parse(valueRule.getEqualTo(), fmt));
        } else {
            throw new AssertConnectorException(
                    AssertConnectorErrorCode.TYPES_NOT_SUPPORTED_FAILED,
                    String.format(" %s types not supported yet", value.getClass().getSimpleName()));
        }
    }

    private Boolean checkType(Object value, SeaTunnelDataType<?> fieldType) {
        return value.getClass().equals(fieldType.getTypeClass());
    }
}
