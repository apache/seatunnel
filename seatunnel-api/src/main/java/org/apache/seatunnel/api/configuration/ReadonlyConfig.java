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

import static org.apache.seatunnel.api.configuration.util.ConfigUtil.convertToJsonString;
import static org.apache.seatunnel.api.configuration.util.ConfigUtil.convertValue;

import org.apache.seatunnel.api.configuration.util.ConfigUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ReadonlyConfig {

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    /**
     * Stores the concrete key/value pairs of this configuration object.
     */
    protected final Map<String, Object> confData;

    private ReadonlyConfig(Map<String, Object> confData) {
        this.confData = confData;
    }

    public static ReadonlyConfig fromMap(Map<String, Object> map) {
        return new ReadonlyConfig(ConfigUtil.treeMap(map));
    }

    public static ReadonlyConfig fromConfig(Config config) {
        try {
            return new ReadonlyConfig(ConfigUtil.treeMap(
                JACKSON_MAPPER.readValue(
                    config.root().render(ConfigRenderOptions.concise()),
                    Object.class)));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Json parsing exception.");
        }
    }

    public <T> T get(Option<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }

    public Map<String, String> toMap() {
        synchronized (this.confData) {
            Map<String, Object> flatteningMap = ConfigUtil.flatteningMap(confData);
            Map<String, String> result = new HashMap<>(flatteningMap.size());
            for (Map.Entry<String, Object> entry : flatteningMap.entrySet()) {
                result.put(entry.getKey(), convertToJsonString(entry.getValue()));
            }
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> getOptional(Option<T> option) {
        if (option == null) {
            throw new NullPointerException("Option not be null.");
        }
        String[] keys = option.key().split("\\.");
        synchronized (this.confData) {
            Map<String, Object> data = this.confData;
            Object value = null;
            for (int i = 0; i < keys.length; i++) {
                value = data.get(keys[i]);
                if (i < keys.length - 1) {
                    if (!((value instanceof Map))) {
                        return Optional.empty();
                    } else {
                        data = (Map<String, Object>) value;
                    }
                }
            }
            if (value == null) {
                return Optional.empty();
            }
            return Optional.of(convertValue(value, option.typeReference()));
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
        } else if (obj instanceof ReadonlyConfig) {
            Map<String, Object> otherConf = ((ReadonlyConfig) obj).confData;

            for (Map.Entry<String, Object> e : this.confData.entrySet()) {
                Object thisVal = e.getValue();
                Object otherVal = otherConf.get(e.getKey());
                if (!thisVal.getClass().equals(byte[].class)) {
                    if (!thisVal.equals(otherVal)) {
                        return false;
                    }
                } else if (otherVal.getClass().equals(byte[].class)) {
                    if (!Arrays.equals((byte[]) thisVal, (byte[]) otherVal)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return convertToJsonString(this.confData);
    }
}
