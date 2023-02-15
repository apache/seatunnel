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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.configuration.ConfigShade;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/** Config shade utilities */
@Slf4j
public final class ConfigShadeUtils {

    private static final String SHADE_IDENTIFIER_OPTION = "shade.identifier";

    private static final String[] SENSITIVE_OPTIONS = new String[] {"password", "username"};

    private static final Map<String, ConfigShade> CONFIG_SHADES = new HashMap<>();

    private static final ConfigShade DEFAULT_SHADE = new DefaultConfigShade();

    static {
        ServiceLoader<ConfigShade> serviceLoader = ServiceLoader.load(ConfigShade.class);
        Iterator<ConfigShade> it = serviceLoader.iterator();
        if (it.hasNext()) {
            try {
                ConfigShade configShade = it.next();
                CONFIG_SHADES.put(configShade.getIdentifier(), configShade);
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
    }

    private static class DefaultConfigShade implements ConfigShade {
        private static final String IDENTIFIER = "default";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return content;
        }

        @Override
        public String decrypt(String content) {
            return content;
        }
    }

    public static String encryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return configShade.encrypt(content);
    }

    public static String decryptOption(String identifier, String content) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        return configShade.decrypt(content);
    }

    public static Config decryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.getConfig(Constants.ENV),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        return decryptConfig(identifier, config);
    }

    @SuppressWarnings("unchecked")
    public static Config decryptConfig(String identifier, Config config) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        String jsonString = config.root().render(ConfigRenderOptions.concise());
        ObjectNode jsonNodes = JsonUtils.parseObject(jsonString);
        Map<String, Object> configMap = JsonUtils.toMap(jsonNodes);
        List<Map<String, Object>> sources =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SOURCE);
        List<Map<String, Object>> sinks =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SINK);
        sources.forEach(
                source -> {
                    for (String sensitiveOption : SENSITIVE_OPTIONS) {
                        source.computeIfPresent(
                                sensitiveOption,
                                (key, value) -> configShade.decrypt(value.toString()));
                    }
                });
        sinks.forEach(
                sink -> {
                    for (String sensitiveOption : SENSITIVE_OPTIONS) {
                        sink.computeIfPresent(
                                sensitiveOption,
                                (key, value) -> configShade.decrypt(value.toString()));
                    }
                });
        configMap.put(Constants.SOURCE, sources);
        configMap.put(Constants.SINK, sinks);
        return ConfigFactory.parseMap(configMap);
    }
}
