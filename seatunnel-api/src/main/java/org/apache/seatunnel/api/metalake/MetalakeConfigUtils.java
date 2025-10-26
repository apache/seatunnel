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

package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import org.apache.seatunnel.common.utils.PlaceholderUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class MetalakeConfigUtils {

    public static Config getMetalakeConfig(Config jobConfigTmp) {
        Config envConfig = jobConfigTmp.getConfig("env");
        boolean metalakeEnabled =
                envConfig.hasPath("metalake_enabled")
                        ? envConfig.getBoolean("metalake_enabled")
                        : Boolean.parseBoolean(
                                System.getenv().getOrDefault("METALAKE_ENABLED", "false"));
        if (!metalakeEnabled) return jobConfigTmp;

        Config update = jobConfigTmp;
        String metalakeType =
                envConfig.hasPath("metalake_type")
                        ? envConfig.getString("metalake_type")
                        : System.getenv("METALAKE_TYPE");
        String metalakeUrl =
                envConfig.hasPath("metalake_url")
                        ? envConfig.getString("metalake_url")
                        : System.getenv("METALAKE_URL");
        MetalakeClient metalakeClient = MetalakeClientFactory.create(metalakeType, metalakeUrl);
        update = replaceConfigList(update, "source", metalakeClient);
        update = replaceConfigList(update, "sink", metalakeClient);
        update = replaceConfigList(update, "transform", metalakeClient);
        return update;
    }

    private static Config replaceConfigList(
            Config updateConfig, String key, MetalakeClient metalakeClient) {
        ConfigList list = updateConfig.getList(key);
        List<ConfigValue> newConfigList = new ArrayList<>(list);

        try {
            for (int i = 0; i < list.size(); i++) {
                ConfigObject Obj = (ConfigObject) list.get(i);
                if (Obj.containsKey("sourceId")) {
                    ConfigObject tmp = Obj;
                    String sourceId = Obj.toConfig().getString("sourceId");
                    JsonNode metalakeJson = metalakeClient.getMetaInfo(sourceId);
                    for (Map.Entry<String, ConfigValue> entry : Obj.entrySet()) {
                        String subKey = entry.getKey();
                        ConfigValue value = entry.getValue();

                        if (value.valueType() == ConfigValueType.STRING) {
                            String strValue = (String) value.unwrapped();
                            String newValue =
                                    PlaceholderUtils.replacePlaceholders(strValue, metalakeJson);
                            tmp = tmp.withValue(subKey, ConfigValueFactory.fromAnyRef(newValue));
                        }
                    }
                    newConfigList.set(i, tmp);
                }
            }
        } catch (IOException e) {
            log.error("Fail to get MetaInfo", e);
        }
        return updateConfig.withValue(key, ConfigValueFactory.fromIterable(newConfigList));
    }
}
