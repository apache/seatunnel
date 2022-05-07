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

import java.util.HashMap;
import java.util.Map;

public class MapUtil {
    private MapUtil() {

    }

    public static Map<String, Object> setMap(Config config, String prefix, boolean keepPrefix) {

        final Map<String, Object> map = new HashMap<>(config.entrySet().size());

        config.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue().unwrapped();
            if (key.startsWith(prefix)) {
                map.put(keepPrefix ? key : key.substring(prefix.length()), value);
            }
        });

        return map;
    }
}
