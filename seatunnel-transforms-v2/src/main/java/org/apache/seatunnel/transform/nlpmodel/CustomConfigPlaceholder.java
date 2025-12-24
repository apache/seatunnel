/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.seatunnel.transform.nlpmodel;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomConfigPlaceholder {

    // Placeholder ${model}
    public static final String REPLACE_PLACEHOLDER_MODEL = "model";
    // Placeholder ${input}
    public static final String REPLACE_PLACEHOLDER_INPUT = "input";
    // Placeholder ${prompt}
    public static final String REPLACE_PLACEHOLDER_PROMPT = "prompt";

    public static String replacePlaceholders(
            String input, String placeholderName, String value, String defaultValue) {
        String placeholderRegex = "\\$\\{" + Pattern.quote(placeholderName) + "(:[^}]*)?\\}";
        Pattern pattern = Pattern.compile(placeholderRegex);
        Matcher matcher = pattern.matcher(input);

        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement =
                    value != null && !value.isEmpty()
                            ? value
                            : (matcher.group(1) != null
                                    ? matcher.group(1).substring(1).trim()
                                    : defaultValue);
            if (replacement == null) {
                continue;
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    public static Boolean findPlaceholder(String input, String placeholderName) {
        String placeholderRegex = "\\$\\{" + Pattern.quote(placeholderName) + "(:[^}]*)?\\}";
        Pattern pattern = Pattern.compile(placeholderRegex);
        Matcher matcher = pattern.matcher(input);
        return matcher.find();
    }
}
