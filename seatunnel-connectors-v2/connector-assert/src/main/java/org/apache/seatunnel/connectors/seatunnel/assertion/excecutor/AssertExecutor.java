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

import org.apache.seatunnel.shade.com.google.common.collect.Iterables;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.format.json.JsonToRowConverters;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

        SeaTunnelDataType<?> type = rowType.getFieldType(index);
        Object value = rowData.getField(index);
        String fieldName = rowType.getFieldName(index);
        Boolean typeChecked = checkType(value, assertFieldRule.getFieldType());
        if (Boolean.FALSE.equals(typeChecked)) {
            return Boolean.FALSE;
        }
        Boolean valueChecked = checkValue(value, type, assertFieldRule.getFieldRules(), fieldName);
        if (Boolean.FALSE.equals(valueChecked)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean checkValue(
            Object value,
            SeaTunnelDataType<?> type,
            List<AssertFieldRule.AssertRule> fieldValueRules,
            String fieldName) {
        Optional<AssertFieldRule.AssertRule> failValueRule =
                fieldValueRules.stream()
                        .filter(valueRule -> !pass(value, type, valueRule, fieldName))
                        .findFirst();
        if (failValueRule.isPresent()) {
            return Boolean.FALSE;
        } else {
            return Boolean.TRUE;
        }
    }

    private boolean pass(
            Object value,
            SeaTunnelDataType<?> type,
            AssertFieldRule.AssertRule valueRule,
            String fieldName) {
        AssertFieldRule.AssertRuleType ruleType = valueRule.getRuleType();
        boolean isPass = true;
        if (ruleType != null) {
            isPass = checkAssertRule(value, type, valueRule);
        }

        if (Objects.nonNull(value) && valueRule.getEqualTo() != null) {
            isPass = isPass && compareValue(value, type, valueRule, fieldName);
        }
        return isPass;
    }

    private boolean checkAssertRule(
            Object value, SeaTunnelDataType<?> type, AssertFieldRule.AssertRule valueRule) {
        switch (valueRule.getRuleType()) {
            case NULL:
                return Objects.isNull(value);
            case NOT_NULL:
                return Objects.nonNull(value);
            case MAX:
                {
                    if (Objects.isNull(value) || !(value instanceof Number)) {
                        return Boolean.FALSE;
                    }
                    return ((Number) value).doubleValue() <= valueRule.getRuleValue();
                }
            case MIN:
                {
                    if (Objects.isNull(value) || !(value instanceof Number)) {
                        return Boolean.FALSE;
                    }
                    return ((Number) value).doubleValue() >= valueRule.getRuleValue();
                }
            case MAX_LENGTH:
                {
                    String valueStr =
                            Objects.isNull(value) ? StringUtils.EMPTY : String.valueOf(value);
                    return valueStr.length() <= valueRule.getRuleValue();
                }
            case MIN_LENGTH:
                {
                    String valueStr =
                            Objects.isNull(value) ? StringUtils.EMPTY : String.valueOf(value);
                    return valueStr.length() >= valueRule.getRuleValue();
                }
            default:
                return false;
        }
    }

    private boolean compareValue(
            Object value,
            SeaTunnelDataType<?> type,
            AssertFieldRule.AssertRule valueRule,
            String fieldName) {
        Object config = valueRule.getEqualTo();
        String confJsonStr = JsonUtils.toJsonString(config);

        JsonToRowConverters converters = new JsonToRowConverters(true, false);
        JsonToRowConverters.JsonToObjectConverter converter = converters.createConverter(type);

        Object confValue;
        try {
            confValue =
                    converter.convert(
                            JsonUtils.stringToJsonNode(JsonUtils.toJsonString(config)), fieldName);
        } catch (IOException e) {
            throw CommonError.jsonOperationError("Assert", confJsonStr, e);
        }
        return compareValue(value, type, confValue);
    }

    private boolean compareValue(Object value, SeaTunnelDataType<?> type, Object confValue) {
        switch (type.getSqlType()) {
            case ROW:
                {
                    return compareRowValue(
                            (SeaTunnelRow) value,
                            (SeaTunnelRowType) type,
                            (SeaTunnelRow) confValue);
                }
            case ARRAY:
                {
                    return compareArrayValue(
                            (Object[]) value, (ArrayType<?, ?>) type, (Object[]) confValue);
                }
            case MAP:
                {
                    return compareMapValue(
                            (Map<?, ?>) value, (MapType<?, ?>) type, (Map<?, ?>) confValue);
                }
            case NULL:
                return value == null && confValue == null;
            case BYTES:
                {
                    return Arrays.equals((byte[]) value, (byte[]) confValue);
                }
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case DATE:
            default:
                return value.equals(confValue);
        }
    }

    private boolean compareRowValue(
            SeaTunnelRow value, SeaTunnelRowType type, SeaTunnelRow confValue) {
        Object[] valFields = value.getFields();
        Object[] confValFields = confValue.getFields();
        if (valFields.length != confValFields.length) {
            return false;
        }
        for (int idx = 0; idx < confValFields.length; idx++) {
            Object fieldVal = valFields[idx];
            Object confField = confValFields[idx];
            SeaTunnelDataType<?> fieldType = type.getFieldType(idx);
            if (!compareValue(fieldVal, fieldType, confField)) {
                return false;
            }
        }
        return true;
    }

    private boolean compareArrayValue(Object[] value, ArrayType<?, ?> type, Object[] confValue) {
        if (value.length != confValue.length) {
            return false;
        }

        SeaTunnelDataType<?> elementType = type.getElementType();
        for (int idx = 0; idx < confValue.length; idx++) {
            Object elementVal = value[idx];
            Object confElement = confValue[idx];
            if (!compareValue(elementVal, elementType, confElement)) {
                return false;
            }
        }
        return true;
    }

    private boolean compareMapValue(Map<?, ?> value, MapType<?, ?> type, Map<?, ?> confValue) {
        if (value.size() != confValue.size()) {
            return false;
        }

        if (value.isEmpty()) {
            return true;
        }

        SeaTunnelDataType<?> valType = type.getValueType();
        for (Map.Entry<?, ?> entry : confValue.entrySet()) {
            Object confKey = entry.getKey();
            Object confVal = entry.getValue();
            if (!value.containsKey(confKey)) {
                return false;
            }

            Object val = value.get(confKey);
            if (!compareValue(val, valType, confVal)) {
                return false;
            }
        }
        return true;
    }

    private Boolean checkType(Object value, SeaTunnelDataType<?> fieldType) {
        if (value == null) {
            return true;
        }

        if (fieldType.getSqlType() == SqlType.NULL) {
            return false;
        }

        if (fieldType.getSqlType() == SqlType.ROW) {
            return checkRowType(value, (SeaTunnelRowType) fieldType);
        }

        if (fieldType.getSqlType() == SqlType.ARRAY) {
            return checkArrayType(value, (ArrayType<?, ?>) fieldType);
        }

        if (fieldType.getSqlType() == SqlType.MAP) {
            return checkMapType(value, (MapType) fieldType);
        }

        if (fieldType.getSqlType() == SqlType.DECIMAL) {
            return checkDecimalType(value, fieldType);
        }

        if (fieldType.getSqlType() == SqlType.FLOAT_VECTOR
                || fieldType.getSqlType() == SqlType.FLOAT16_VECTOR
                || fieldType.getSqlType() == SqlType.BFLOAT16_VECTOR
                || fieldType.getSqlType() == SqlType.BINARY_VECTOR) {
            return value instanceof ByteBuffer;
        }

        return value.getClass().equals(fieldType.getTypeClass());
    }

    private boolean checkArrayType(Object value, ArrayType<?, ?> fieldType) {
        if (!value.getClass().isArray()) {
            return false;
        }

        Object[] val = (Object[]) value;
        SeaTunnelDataType<?> elementType = fieldType.getElementType();

        for (Object elementObj : val) {
            if (!checkType(elementObj, elementType)) {
                return false;
            }
        }
        return true;
    }

    private boolean checkMapType(Object value, MapType<?, ?> fieldType) {
        if (!(value instanceof Map)) {
            return false;
        }

        Map<?, ?> val = (Map<?, ?>) value;
        SeaTunnelDataType<?> keyType = fieldType.getKeyType();
        SeaTunnelDataType<?> valType = fieldType.getValueType();
        for (Map.Entry<?, ?> entry : val.entrySet()) {
            Object keyObj = entry.getKey();
            Object valObj = entry.getValue();
            if (!(checkType(keyObj, keyType) && checkType(valObj, valType))) {
                return false;
            }
        }
        return true;
    }

    private boolean checkRowType(Object value, SeaTunnelRowType rowType) {
        if (!(value instanceof SeaTunnelRow)) {
            return false;
        }

        SeaTunnelRow row = (SeaTunnelRow) value;
        Object[] fields = row.getFields();
        for (int idx = 0; idx < fields.length; idx++) {
            Object fieldVal = fields[idx];
            SeaTunnelDataType<?> fieldType = rowType.getFieldType(idx);
            if (!checkType(fieldVal, fieldType)) {
                return false;
            }
        }
        return true;
    }

    private static Boolean checkDecimalType(Object value, SeaTunnelDataType<?> fieldType) {
        if (!value.getClass().equals(fieldType.getTypeClass())) {
            return false;
        }
        DecimalType fieldDecimalType = (DecimalType) fieldType;
        BigDecimal valueObj = (BigDecimal) value;
        if (valueObj.scale() != fieldDecimalType.getScale()) {
            return false;
        }
        return valueObj.precision() <= fieldDecimalType.getPrecision();
    }
}
