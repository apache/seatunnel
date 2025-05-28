package org.apache.seatunnel.format.sensorsdata.record;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.sensorsdata.SensorsDataTypes;
import org.apache.seatunnel.format.sensorsdata.config.SensorsDataConfigBase;
import org.apache.seatunnel.format.sensorsdata.config.TargetColumnConfig;
import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataErrorCode;
import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataException;
import org.apache.seatunnel.format.sensorsdata.utils.TypeUtil;

import org.apache.commons.lang3.StringUtils;

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

    private final String CURRENT_TIME_KEY = "current_time()";

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
        // 当配置的 distinctId 字段没有获取到数据时, 需要使用 identities 里面的信息补充一个数据
        return getDistinctId(getUserIdentities(row));
    }

    /**
     * 按照 distinct_id, $identity_login_id, $identity_anonymous_id, $identity_distinct_id , 其他
     * identity 的顺序获取第一个有值的字段作为 distinct_id
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

            // 如果 value 没有数据则不传递该信息
            if (strValue == null || StringUtils.isBlank((String) strValue)) {
                continue;
            }

            Object value;
            if (isLoginId(key)) {
                // 如果是 $identity_login_id，那么按照 STRING 来转换和解析。
                value = TypeUtil.toTargetType(strValue, SensorsDataTypes.DataTypes.STRING);
            } else {
                // 否则，其它 ID 标识按照 LIST 来转换和解析。
                value = TypeUtil.toTargetType(strValue, SensorsDataTypes.DataTypes.LIST);
            }

            if (value != null) {
                identities.put(key, value);
            }
        }

        return identities;
    }

    /**
     * 判断指定的 ID 标识字段是否为 $identity_login_id。
     *
     * @param field ID 标识字段
     * @return
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

        // 设置 $time
        if (this.eventTimeUseCurrentTime) {
            // 如果设置的是使用当前当前时间, 则获取当前时间处理
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
