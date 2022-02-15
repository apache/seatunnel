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

package org.apache.seatunnel.common.utils;

import org.apache.commons.lang3.text.StrSubstitutor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class StringTemplate {

    private StringTemplate() {
    }

    /**
     * @param str        raw string
     * @param timeFormat example : "yyyy-MM-dd HH:mm:ss"
     * @return replaced string
     */
    public static String substitute(String str, String timeFormat) {

        final SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        final String formattedDate = sdf.format(new Date());

        final Map<String, String> valuesMap = new HashMap<>(3);
        valuesMap.put("uuid", UUID.randomUUID().toString());
        valuesMap.put("now", formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        final StrSubstitutor sub = new StrSubstitutor(valuesMap);
        return sub.replace(str);
    }
}

