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

package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Prepares JSON-derived maps for config parsing without changing valid path expressions. */
public final class ConfigMapPathUtils {

    private ConfigMapPathUtils() {}

    /**
     * {@link ConfigFactory#parseMap(Map)} parses every map key as a path, including nested maps and
     * maps inside lists. Quote only keys rejected by the same path parser, so regex and other JSON
     * literal keys survive each map-to-config conversion while valid path expressions retain their
     * existing meaning. A rendered Config loses this quoting, so each later parse must call this
     * method again.
     */
    public static Map<String, Object> quoteInvalidPathKeys(Map<String, Object> objectMap) {
        Map<String, Object> result = new LinkedHashMap<>();
        objectMap.forEach(
                (key, value) ->
                        result.put(quoteInvalidPathKey(key), quoteInvalidPathKeysInValue(value)));
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Object quoteInvalidPathKeysInValue(Object value) {
        if (value instanceof Map<?, ?>) {
            return quoteInvalidPathKeys((Map<String, Object>) value);
        }
        if (value instanceof List<?>) {
            List<Object> result = new ArrayList<>();
            for (Object item : (List<?>) value) {
                result.add(quoteInvalidPathKeysInValue(item));
            }
            return result;
        }
        return value;
    }

    private static String quoteInvalidPathKey(String key) {
        try {
            ConfigUtil.splitPath(key);
            return key;
        } catch (ConfigException.BadPath ignored) {
            return ConfigUtil.quoteString(key);
        }
    }
}
