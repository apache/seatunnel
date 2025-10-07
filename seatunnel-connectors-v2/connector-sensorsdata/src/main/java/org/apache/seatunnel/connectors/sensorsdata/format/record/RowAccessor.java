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

package org.apache.seatunnel.connectors.sensorsdata.format.record;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.sensorsdata.format.SensorsDataTypes;
import org.apache.seatunnel.connectors.sensorsdata.format.config.SensorsDataConfigBase;
import org.apache.seatunnel.connectors.sensorsdata.format.config.TargetColumnConfig;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataErrorCode;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataException;
import org.apache.seatunnel.connectors.sensorsdata.format.utils.TypeUtil;

import com.sensorsdata.analytics.javasdk.SensorsConst;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

public class RowAccessor implements Serializable {
    private static final Pattern EVENT_NAME_CONFIG_PATTERN =
            Pattern.compile("\\$\\{(.*?)\\}", Pattern.DOTALL);

    private final SensorsDataConfigBase config;

    private final String schema;

    private final Map<String, Integer> columnIndex = new HashMap<>();

    private final Integer distinctIdColumnIndex;
    private Integer timeColumnIndex;
    private boolean eventTimeUseCurrentTime;

    private String eventName;
    private Integer eventColumnIndex;

    private final Integer detailIdColumnIndex;

    private final Integer itemIdColumnIndex;
    private final Integer itemTypeColumnIndex;

    private static final String CURRENT_TIME_KEY = "current_time()";

    private static final String OLD_DISTINCT_ID = "distinct_id";
    private static final String LOGIN_ID = "$identity_login_id";
    private static final String ANONYMOUS_ID = "$identity_anonymous_id";
    private static final String DISTINCT_ID = "$identity_distinct_id";

    public RowAccessor(SensorsDataConfigBase config, SeaTunnelRowType rowType) {
        this.config = config;

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            columnIndex.put(fieldName, i);
        }

        this.distinctIdColumnIndex = checkAndGetColumnIndex(config.getDistinctIdColumn());

        if (StringUtils.isNotBlank(config.getTimeColumn())) {
            if (config.getTimeColumn().equals(CURRENT_TIME_KEY)) {
                this.eventTimeUseCurrentTime = true;
            } else {
                this.eventTimeUseCurrentTime = false;
                this.timeColumnIndex = checkAndGetColumnIndex(config.getTimeColumn());
            }
        }

        initEventNameConfig(config);

        this.schema = config.getSchema();
        this.detailIdColumnIndex = checkAndGetColumnIndex(config.getDetailIdColumn());
        this.itemIdColumnIndex = checkAndGetColumnIndex(config.getItemIdColumn());
        this.itemTypeColumnIndex = checkAndGetColumnIndex(config.getItemTypeColumn());

        checkTargetColumnConfigs();
    }

    private void initEventNameConfig(SensorsDataConfigBase config) {
        String str = config.getEventName();
        if (StringUtils.isBlank(str)) {
            return;
        }

        Matcher matcher = EVENT_NAME_CONFIG_PATTERN.matcher(str);
        if (matcher.find()) {
            eventName = null;
            eventColumnIndex = checkAndGetColumnIndex(matcher.group(1));
        } else {
            eventName = str;
            eventColumnIndex = null;
        }
    }

    private Integer checkAndGetColumnIndex(String columnName) {
        if (StringUtils.isBlank(columnName)) {
            return null;
        }

        Integer index = columnIndex.get(columnName);
        if (index == null) {
            String message = String.format("Field [%s] not found in source column", columnName);
            throw new SensorsDataException(SensorsDataErrorCode.UNKNOWN_SOURCE_FIELD, message);
        }
        return index;
    }

    private void checkTargetColumnConfigs() {
        ArrayList<TargetColumnConfig> targetColumnConfigs =
                new ArrayList<>(config.getPropertyFields());

        if (config.getIdentityFields() != null) {
            targetColumnConfigs.addAll(config.getIdentityFields());
        }

        List<String> unknownSourceFields =
                targetColumnConfigs.stream()
                        .map(TargetColumnConfig::getSource)
                        .distinct()
                        .filter(source -> !columnIndex.containsKey(source))
                        .collect(toList());

        if (!unknownSourceFields.isEmpty()) {
            String message =
                    String.format(
                            "Fields [%s] not found in source column",
                            String.join(", ", unknownSourceFields));
            throw new SensorsDataException(SensorsDataErrorCode.UNKNOWN_SOURCE_FIELD, message);
        }
    }

    public String getEventName(SeaTunnelRow row) {
        if (eventName != null) {
            return eventName;
        }

        if (eventColumnIndex != null) {
            return (String) row.getField(eventColumnIndex);
        }

        throw new SensorsDataException(
                SensorsDataErrorCode.EVENT_NAME_NOT_SET, "Event name not set");
    }

    public String getDistinctId(SeaTunnelRow row) {
        Object distinctValue =
                TypeUtil.toTargetType(
                        row.getField(this.distinctIdColumnIndex),
                        SensorsDataTypes.DataTypes.STRING);
        if ((!config.isDistinctIdByIdentities())
                || (distinctValue != null && StringUtils.isNotBlank((String) distinctValue))) {
            return (String) distinctValue;
        }
        // if the distinctId field is not obtained from the data, it needs to be supplemented with
        // information from the identitity fields
        return getDistinctId(getUserIdentities(row));
    }

    /**
     * Get the first non-null field in the order of: distinct_id, $identity_login_id,
     * $identity_anonymous_id, $identity_distinct_id, and other identity fields as the distinct_id.
     */
    private String getDistinctId(Map<String, Object> userIdentities) {
        if (userIdentities.containsKey(OLD_DISTINCT_ID)) {
            return getIdentityValue(OLD_DISTINCT_ID, userIdentities.get(OLD_DISTINCT_ID));
        }

        if (userIdentities.containsKey(LOGIN_ID)) {
            return getIdentityValue(LOGIN_ID, userIdentities.get(LOGIN_ID));
        }

        if (userIdentities.containsKey(ANONYMOUS_ID)) {
            return getIdentityValue(ANONYMOUS_ID, userIdentities.get(ANONYMOUS_ID));
        }

        if (userIdentities.containsKey(DISTINCT_ID)) {
            return getIdentityValue(DISTINCT_ID, userIdentities.get(DISTINCT_ID));
        }

        return userIdentities.entrySet().stream()
                .findFirst()
                .map(
                        it ->
                                String.format(
                                        "%s+%s",
                                        it.getKey(), getIdentityValue(it.getKey(), it.getValue())))
                .orElse(null);
    }

    private String getIdentityValue(String field, Object value) {
        if (value instanceof List) {
            return ((List) value).get(0).toString();
        } else if (value instanceof String) {
            return (String) value;
        }
        throw new SensorsDataException(
                SensorsDataErrorCode.ILLEGAL_ARGUMENT,
                String.format("Identity value must be String or List. [field=%s]", field));
    }

    public Map<String, Object> getUserIdentities(SeaTunnelRow row) {
        Map<String, Object> identities = new HashMap<>();

        for (TargetColumnConfig col : config.getIdentityFields()) {
            String key = col.getTarget();
            int index = columnIndex.get(col.getSource());

            Object strValue =
                    TypeUtil.toTargetType(row.getField(index), SensorsDataTypes.DataTypes.STRING);

            // if the value is null or blank, skip it
            if (strValue == null || StringUtils.isBlank((String) strValue)) {
                continue;
            }

            Object value;
            if (isLoginId(key)) {
                // if it is $identity_login_id, convert and parse it as STRING
                value = TypeUtil.toTargetType(strValue, SensorsDataTypes.DataTypes.STRING);
            } else {
                // otherwise, other identity value are converted and parsed as LIST
                value = TypeUtil.toTargetType(strValue, SensorsDataTypes.DataTypes.LIST);
            }

            if (value != null) {
                identities.put(key, value);
            }
        }

        return identities;
    }

    /**
     * Whether the identity field is $identity_login_id.
     *
     * @param field identity field name
     * @return true if the field is $identity_login_id
     */
    private boolean isLoginId(String field) {
        return LOGIN_ID.equals(field);
    }

    public Map<String, String> getIdentities(SeaTunnelRow row) {
        Map<String, String> identities = new HashMap<>();

        for (TargetColumnConfig col : config.getIdentityFields()) {
            String key = col.getTarget();
            int index = columnIndex.get(col.getSource());
            String value =
                    (String)
                            TypeUtil.toTargetType(
                                    row.getField(index), SensorsDataTypes.DataTypes.STRING);
            if (value != null) {
                identities.put(key, value);
            }
        }

        return identities;
    }

    public Map<String, Object> getProperties(SeaTunnelRow row) {
        Map<String, Object> properties = new HashMap<>();

        for (TargetColumnConfig col : config.getPropertyFields()) {
            String key = col.getTarget();
            int index = columnIndex.get(col.getSource());
            Object value = TypeUtil.toTargetType(row.getField(index), col.getType());
            if (value != null) {
                properties.put(key, value);
            }
        }

        // Set $time
        if (this.eventTimeUseCurrentTime) {
            properties.put(SensorsConst.TIME_SYSTEM_ATTR, new Date());
        } else {
            if (this.timeColumnIndex != null) {
                properties.put(
                        SensorsConst.TIME_SYSTEM_ATTR,
                        TypeUtil.toTargetType(
                                row.getField(this.timeColumnIndex),
                                SensorsDataTypes.DataTypes.DATE));
            }
        }
        return properties;
    }

    public String getSchemaRequired() {
        if (StringUtils.isBlank(schema)) {
            throw new SensorsDataException(
                    SensorsDataErrorCode.MISSING_NECESSARY_FIELD, "'schema' is required.");
        }

        return schema;
    }

    public String getDetailIdRequired(SeaTunnelRow row) {
        String detailId =
                (String)
                        TypeUtil.toTargetType(
                                row.getField(detailIdColumnIndex),
                                SensorsDataTypes.DataTypes.STRING);

        if (StringUtils.isBlank(detailId)) {
            throw new SensorsDataException(
                    SensorsDataErrorCode.MISSING_NECESSARY_FIELD, "'detailId' is required.");
        }

        return detailId;
    }

    public String getItemIdRequired(SeaTunnelRow row) {
        String itemId =
                (String)
                        TypeUtil.toTargetType(
                                row.getField(itemIdColumnIndex), SensorsDataTypes.DataTypes.STRING);

        if (StringUtils.isBlank(itemId)) {
            throw new SensorsDataException(
                    SensorsDataErrorCode.MISSING_NECESSARY_FIELD, "'itemId' is required.");
        }

        return itemId;
    }

    public String getItemTypeRequired(SeaTunnelRow row) {
        String itemType =
                (String)
                        TypeUtil.toTargetType(
                                row.getField(itemTypeColumnIndex),
                                SensorsDataTypes.DataTypes.STRING);

        if (StringUtils.isBlank(itemType)) {
            throw new SensorsDataException(
                    SensorsDataErrorCode.MISSING_NECESSARY_FIELD, "'itemType' is required.");
        }

        return itemType;
    }
}
