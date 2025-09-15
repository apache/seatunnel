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
        Config update = jobConfigTmp;

        try {
            ConfigList sourceList = jobConfigTmp.getList("source");
            update =
                    update.withValue(
                            "source",
                            ConfigValueFactory.fromIterable(replaceConfigList(sourceList)));
        } catch (IOException e) {
            log.error("Fail to get MetaInfo", e);
        }

        try {
            ConfigList sinkList = jobConfigTmp.getList("sink");
            update =
                    update.withValue(
                            "sink", ConfigValueFactory.fromIterable(replaceConfigList(sinkList)));
        } catch (IOException e) {
            log.error("Fail to get MetaInfo", e);
        }
        return update;
    }

    private static List<ConfigValue> replaceConfigList(ConfigList list) throws IOException {
        String metalakeType = System.getenv("METALAKE_TYPE");
        String metalakeUrl = System.getenv("METALAKE_URL");
        MetalakeClient metalakeClient = MetalakeClientFactory.create(metalakeType, metalakeUrl);

        List<ConfigValue> newConfigList = new ArrayList<>(list);

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
        return newConfigList;
    }
}
