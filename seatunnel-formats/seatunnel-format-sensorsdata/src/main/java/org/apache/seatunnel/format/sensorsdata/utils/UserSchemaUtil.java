package org.apache.seatunnel.format.sensorsdata.utils;

import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataErrorCode;
import org.apache.seatunnel.format.sensorsdata.exception.SensorsDataException;

import org.apache.commons.collections4.MapUtils;

import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import lombok.experimental.UtilityClass;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.sensorsdata.analytics.javasdk.SensorsConst.PROJECT_SYSTEM_ATTR;

@UtilityClass
public class UserSchemaUtil {

    public UserSchema buildUnsetUserSchema(UserSchema userSchema, Set<String> allProperties) {
        try {
            Map<String, Object> propertyMap =
                    buildUnsetPropertyMap(userSchema.getPropertyMap(), allProperties);
            // 如果构建出来的 propertyMap 为空时, 无需构建 userSchema
            // 因为 userSchema 中如果没有属性, 则表示不需要进行 unset 操作
            if (MapUtils.isEmpty(propertyMap)) {
                return null;
            }
            return UserSchema.init()
                    .setDistinctId(userSchema.getDistinctId())
                    .identityMap(userSchema.getIdentityMap())
                    .addProperties(propertyMap)
                    .start();
        } catch (InvalidArgumentException e) {
            throw new SensorsDataException(SensorsDataErrorCode.ILLEGAL_ARGUMENT, e.getMessage());
        }
    }

    public Map<String, Object> buildUnsetPropertyMap(
            Map<String, Object> propertyMap, Set<String> allProperties) {
        Map<String, Object> unsetMap = new HashMap<>();
        if (MapUtils.isNotEmpty(propertyMap)) {
            for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
                if (entry.getValue() == null && !PROJECT_SYSTEM_ATTR.equals(entry.getKey())) {
                    unsetMap.put(entry.getKey(), Boolean.TRUE);
                }
            }
        } else {
            propertyMap = new HashMap<>();
        }
        // 如果对应的属性没有读取到数据, 补全 unset 列表
        Set<String> dataProperties = propertyMap.keySet();
        allProperties.forEach(
                name -> {
                    if (!dataProperties.contains(name) && !PROJECT_SYSTEM_ATTR.equals(name)) {
                        unsetMap.put(name, Boolean.TRUE);
                    }
                });
        return unsetMap;
    }
}
