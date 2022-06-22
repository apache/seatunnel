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

package org.apache.seatunnel.common;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

public final class PropertiesUtil {

    private PropertiesUtil() {
    }

    public static void setProperties(Config config, Properties properties, String prefix, boolean keepPrefix) {
        config.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            if (key.startsWith(prefix)) {
                if (keepPrefix) {
                    properties.put(key, value);
                } else {
                    properties.put(key.substring(prefix.length()), value);
                }
            }
        });
    }

    public static <E extends Enum<E>> E getEnum(final Config conf, final String key, final Class<E> enumClass, final E defaultEnum) {
        if (!conf.hasPath(key)) {
            return defaultEnum;
        }
        final String value = conf.getString(key);
        if (StringUtils.isBlank(value)) {
            return defaultEnum;
        }
        return Enum.valueOf(enumClass, value.toUpperCase());
    }

    public static <T> void setOption(Config config, String optionName, T defaultValue, Function<String, T> getter, Consumer<T> setter) {
        T value;
        if (config.hasPath(optionName)) {
            value = getter.apply(optionName);
        } else {
            value = defaultValue;
        }
        if (value != null) {
            setter.accept(value);
        }
    }

    public static <T> void setOption(Config config, String optionName, Function<String, T> getter, Consumer<T> setter) {
        T value = null;
        if (config.hasPath(optionName)) {
            value = getter.apply(optionName);
        }
        if (value != null) {
            setter.accept(value);
        }
    }
}
