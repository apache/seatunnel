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

package org.apache.seatunnel.api.configuration;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.api.configuration.util.ConfigUtil.convertToJsonString;
import static org.apache.seatunnel.api.configuration.util.ConfigUtil.convertValue;

@Slf4j
public class ReadonlyConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    /** Stores the concrete key/value pairs of this configuration object. */
    protected final Map<String, Object> confData;

    private ReadonlyConfig(Map<String, Object> confData) {
        this.confData = confData;
    }

    public static ReadonlyConfig fromMap(Map<String, Object> map) {
        return new ReadonlyConfig(map);
    }

    public static ReadonlyConfig fromConfig(Config config) {
        try {
            return fromMap(
                    JACKSON_MAPPER.readValue(
                            config.root().render(ConfigRenderOptions.concise()),
                            new TypeReference<Map<String, Object>>() {}));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Json parsing exception.", e);
        }
    }

    public <T> T get(Option<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }

    /**
     * Transform to Config todo: This method should be removed after we remove Config
     *
     * @return Config
     */
    public Config toConfig() {
        return ConfigFactory.parseMap(confData);
    }

    public Map<String, String> toMap() {
        if (confData.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> result = new LinkedHashMap<>();
        toMap(result);
        return result;
    }

    public void toMap(Map<String, String> result) {
        if (confData.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : confData.entrySet()) {
            result.put(entry.getKey(), convertToJsonString(entry.getValue()));
        }
    }

    public <T> Optional<T> getOptional(Option<T> option) {
        if (option == null) {
            throw new NullPointerException("Option not be null.");
        }
        Object value = getValue(option.key());
        if (value == null) {
            for (String fallbackKey : option.getFallbackKeys()) {
                value = getValue(fallbackKey);
                if (value != null) {
                    log.info(
                            "Config uses fallback configuration key '{}' instead of key '{}'",
                            fallbackKey,
                            option.key());
                    break;
                }
            }
        }
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(convertValue(value, option));
    }

    private Object getValue(String key) {
        if (this.confData.containsKey(key)) {
            return this.confData.get(key);
        } else {
            String[] keys = key.split("\\.");
            Map<String, Object> data = this.confData;
            Object value = null;
            for (int i = 0; i < keys.length; i++) {
                value = data.get(keys[i]);
                if (i < keys.length - 1) {
                    if (!(value instanceof Map)) {
                        return null;
                    } else {
                        data = (Map<String, Object>) value;
                    }
                }
            }
            return value;
        }
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (String s : this.confData.keySet()) {
            hash ^= s.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReadonlyConfig)) {
            return false;
        }
        Map<String, Object> otherConf = ((ReadonlyConfig) obj).confData;
        return this.confData.equals(otherConf);
    }

    @Override
    public String toString() {
        return convertToJsonString(this.confData);
    }
}
