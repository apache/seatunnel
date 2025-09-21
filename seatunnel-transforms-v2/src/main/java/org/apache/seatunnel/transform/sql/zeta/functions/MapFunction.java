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

package org.apache.seatunnel.transform.sql.zeta.functions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapFunction {
    private MapFunction() {}

    public static Map<String, Object> map(List<Object> args) {
        if (args == null || args.isEmpty()) {
            return new LinkedHashMap<>();
        }
        if ((args.size() & 1) == 1) {
            throw new IllegalArgumentException(
                    "MAP requires even number of arguments: key,value,...");
        }
        Map<String, Object> result = new LinkedHashMap<>(args.size() / 2);
        for (int i = 0; i < args.size(); i += 2) {
            Object keyObj = args.get(i);
            Object val = args.get(i + 1);
            if (keyObj == null) {
                throw new IllegalArgumentException("MAP key cannot be null at index " + i);
            }
            String key = (keyObj instanceof String) ? (String) keyObj : String.valueOf(keyObj);
            result.put(key, val);
        }
        return result;
    }
}
