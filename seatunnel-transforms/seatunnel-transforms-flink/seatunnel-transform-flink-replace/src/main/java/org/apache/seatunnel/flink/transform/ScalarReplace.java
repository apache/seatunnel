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
package org.apache.seatunnel.flink.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class ScalarReplace extends ScalarFunction {

    private String fields;
    private String pattern;
    private String replacement;
    private Boolean isRegex;
    private Boolean replaceFirst;


    public ScalarReplace(String pattern, String replacement, Boolean isRegex, Boolean replaceFirst) {
        this.pattern = pattern;
        this.replacement = replacement;
        this.isRegex = isRegex;
        this.replaceFirst = replaceFirst;
    }

    public String eval(String str) {
        String rawText = str;
        if (isRegex) {
            if (replaceFirst) {
                rawText = str.replaceFirst(pattern, replacement);
            } else {
                rawText = str.replaceAll(pattern, replacement);
            }
        } else {
            rawText = StringUtils.replace(str, pattern, replacement, replaceFirst ? 1 : -1);
        }
        return rawText;
    }

}
