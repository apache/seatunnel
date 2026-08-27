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

package org.apache.seatunnel.common.exception;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExceptionParamsUtil {

    private static final Pattern PARAMS_PATTERN = Pattern.compile("<([a-zA-Z0-9]+)+>");

    /**
     * Get all params key in description, the param key should be wrapped by <>. eg: "<param1>
     * <param2>" will return ["param1", "param2"]
     *
     * @param description error description
     * @return params key list
     */
    public static List<String> getParams(String description) {
        // find all match params key in description
        Matcher matcher = PARAMS_PATTERN.matcher(description);
        List<String> params = new ArrayList<>();
        while (matcher.find()) {
            String key = matcher.group(1);
            params.add(key);
        }
        return params;
    }

    public static String getDescription(String descriptionTemplate, Map<String, String> params) {
        assertParamsMatchWithDescription(descriptionTemplate, params);
        String description = descriptionTemplate;
        for (String param : getParams(descriptionTemplate)) {
            String value = params.get(param);
            description = description.replace(String.format("<%s>", param), value);
        }
        return description;
    }

    public static void assertParamsMatchWithDescription(
            String descriptionTemplate, Map<String, String> params) {
        getParams(descriptionTemplate)
                .forEach(
                        param -> {
                            if (!params.containsKey(param)) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Param [%s] is not set in error message [%s]",
                                                param, descriptionTemplate));
                            }
                        });
    }
}
