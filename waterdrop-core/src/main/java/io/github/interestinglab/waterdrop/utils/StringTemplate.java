/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.utils;

import org.apache.commons.lang.text.StrSubstitutor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StringTemplate {

    /**
     * @param timeFormat example : "yyyy-MM-dd HH:mm:ss"
     * */
    public static String substitute(String str, String timeFormat) {

        final SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        final String formatteddDate = sdf.format(new Date());

        final Map valuesMap = new HashMap();
        valuesMap.put("uuid", UUID.randomUUID().toString());
        valuesMap.put("now", formatteddDate);
        valuesMap.put(timeFormat, formatteddDate);
        final StrSubstitutor sub = new StrSubstitutor(valuesMap);
        return sub.replace(str);
    }
}
