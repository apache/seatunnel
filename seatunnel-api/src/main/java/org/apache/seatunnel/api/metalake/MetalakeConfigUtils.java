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

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.constants.MetaLakeType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.PlaceholderUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Deprecated
@Slf4j
public class MetalakeConfigUtils {

    private static final String SOURCE_ID = "sourceId";

    public static Config getMetalakeConfig(Config jobConfigTmp) {
        Config envConfig = jobConfigTmp.getConfig(Constants.ENV);
        boolean metalakeEnabled =
                envConfig.hasPath(EnvCommonOptions.METALAKE_ENABLED.key())
                        ? envConfig.getBoolean(EnvCommonOptions.METALAKE_ENABLED.key())
                        : Boolean.parseBoolean(
                                System.getenv()
                                        .getOrDefault(
                                                EnvCommonOptions.METALAKE_ENABLED
                                                        .key()
                                                        .toUpperCase(),
                                                Boolean.toString(false)));
        if (!metalakeEnabled) return jobConfigTmp;

        Config update = jobConfigTmp;
        String metalakeType =
                envConfig.hasPath(EnvCommonOptions.METALAKE_TYPE.key())
                        ? envConfig.getString(EnvCommonOptions.METALAKE_TYPE.key())
                        : System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase());
        String metalakeUrl =
                envConfig.hasPath(EnvCommonOptions.METALAKE_URL.key())
                        ? envConfig.getString(EnvCommonOptions.METALAKE_URL.key())
                        : System.getenv(EnvCommonOptions.METALAKE_URL.key().toUpperCase());
        MetalakeClient metalakeClient =
                MetaLakeFactory.createClient(MetaLakeType.valueOf(metalakeType.toUpperCase()));
        update =
                replaceConfigList(update, PluginType.SOURCE.getType(), metalakeClient, metalakeUrl);
        update = replaceConfigList(update, PluginType.SINK.getType(), metalakeClient, metalakeUrl);
        update =
                replaceConfigList(
                        update, PluginType.TRANSFORM.getType(), metalakeClient, metalakeUrl);
        return update;
    }

    private static Config replaceConfigList(
            Config updateConfig, String key, MetalakeClient metalakeClient, String metalakeUrl) {
        ConfigList list = updateConfig.getList(key);
        List<ConfigValue> newConfigList = new ArrayList<>(list);

        try {
            for (int i = 0; i < list.size(); i++) {
                ConfigObject Obj = (ConfigObject) list.get(i);
                if (Obj.containsKey(SOURCE_ID)) {
                    ConfigObject tmp = Obj;
                    String sourceId = Obj.toConfig().getString(SOURCE_ID);
                    JsonNode metalakeJson = metalakeClient.getMetaInfo(sourceId, metalakeUrl);
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
