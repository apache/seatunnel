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
import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.configuration.ConfigShade;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BiFunction;

/** Config shade utilities */
@Slf4j
public final class ConfigShadeUtils {

    private static final String SHADE_IDENTIFIER_OPTION = "shade.identifier";
    private static final String SHADE_PROPS_OPTION = "shade.properties";

    public static final String[] DEFAULT_SENSITIVE_KEYWORDS =
            new String[] {"password", "username", "auth", "token", "access_key", "secret_key"};

    private static final Map<String, ConfigShade> CONFIG_SHADES = new HashMap<>();

    private static final ConfigShade DEFAULT_SHADE = new DefaultConfigShade();

    static {
        ServiceLoader<ConfigShade> serviceLoader = ServiceLoader.load(ConfigShade.class);
        Iterator<ConfigShade> it = serviceLoader.iterator();
        it.forEachRemaining(
                configShade -> {
                    CONFIG_SHADES.put(configShade.getIdentifier(), configShade);
                });
        log.info("Load config shade spi: {}", CONFIG_SHADES.keySet());
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
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        Map<String, Object> props =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_PROPS_OPTION,
                        new HashMap<>());
        return decryptConfig(identifier, config, props);
    }

    public static Config encryptConfig(Config config) {
        String identifier =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_IDENTIFIER_OPTION,
                        DEFAULT_SHADE.getIdentifier());
        Map<String, Object> props =
                TypesafeConfigUtils.getConfig(
                        config.hasPath(Constants.ENV)
                                ? config.getConfig(Constants.ENV)
                                : ConfigFactory.empty(),
                        SHADE_PROPS_OPTION,
                        new HashMap<>());
        return encryptConfig(identifier, config, props);
    }

    private static Config decryptConfig(
            String identifier, Config config, Map<String, Object> props) {
        return processConfig(identifier, config, true, props);
    }

    private static Config encryptConfig(
            String identifier, Config config, Map<String, Object> props) {
        return processConfig(identifier, config, false, props);
    }

    @SuppressWarnings("unchecked")
    private static Config processConfig(
            String identifier, Config config, boolean isDecrypted, Map<String, Object> props) {
        ConfigShade configShade = CONFIG_SHADES.getOrDefault(identifier, DEFAULT_SHADE);
        // call open method before the encrypt/decrypt
        configShade.open(props);

        List<String> sensitiveOptions = new ArrayList<>(Arrays.asList(DEFAULT_SENSITIVE_KEYWORDS));
        sensitiveOptions.addAll(Arrays.asList(configShade.sensitiveOptions()));
        BiFunction<String, Object, String> processFunction =
                (key, value) -> {
                    if (isDecrypted) {
                        return configShade.decrypt(value.toString());
                    } else {
                        return configShade.encrypt(value.toString());
                    }
                };
        String jsonString = config.root().render(ConfigRenderOptions.concise());
        ObjectNode jsonNodes = JsonUtils.parseObject(jsonString);
        Map<String, Object> configMap = JsonUtils.toMap(jsonNodes);
        List<Map<String, Object>> sources =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SOURCE);
        List<Map<String, Object>> sinks =
                (ArrayList<Map<String, Object>>) configMap.get(Constants.SINK);
        Preconditions.checkArgument(
                !sources.isEmpty(), "Miss <Source> config! Please check the config file.");
        Preconditions.checkArgument(
                !sinks.isEmpty(), "Miss <Sink> config! Please check the config file.");
        sources.forEach(
                source -> {
                    for (String sensitiveOption : sensitiveOptions) {
                        source.computeIfPresent(sensitiveOption, processFunction);
                    }
                });
        sinks.forEach(
                sink -> {
                    for (String sensitiveOption : sensitiveOptions) {
                        sink.computeIfPresent(sensitiveOption, processFunction);
                    }
                });
        configMap.put(Constants.SOURCE, sources);
        configMap.put(Constants.SINK, sinks);
        return ConfigFactory.parseMap(configMap);
    }

    public static class Base64ConfigShade implements ConfigShade {

        private static final Base64.Encoder ENCODER = Base64.getEncoder();

        private static final Base64.Decoder DECODER = Base64.getDecoder();

        private static final String IDENTIFIER = "base64";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return ENCODER.encodeToString(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String decrypt(String content) {
            return new String(DECODER.decode(content));
        }
    }
}
