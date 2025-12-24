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

package org.apache.seatunnel.connectors.sensorsdata.format.utils;

import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataErrorCode;
import org.apache.seatunnel.connectors.sensorsdata.format.exception.SensorsDataException;

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
            // If the propertyMap is empty, no need to build userSchema
            // Because if there are no properties in userSchema, it means that there is no need to
            // perform unset operation
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
        // If the corresponding property is not read, complete the unset list
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
