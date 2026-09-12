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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Removes connection details and credential-like values from connect dry-run failures. */
public final class DryRunConnectFailureMessageSanitizer {

    private static final int MAX_MESSAGE_LENGTH = 2048;
    private static final String MASK = "***";
    private static final String SENSITIVE_KEYS =
            "password|passwd|pwd|token|secret|signature|credential|access[_-]?key|api[_-]?key|private[_-]?key";
    private static final Pattern JDBC_URL =
            Pattern.compile("\\bjdbc:[^\\s]+", Pattern.CASE_INSENSITIVE);
    private static final Pattern SENSITIVE_FREE_TEXT_KEY =
            Pattern.compile(
                    "([\"']?\\b(?:"
                            + SENSITIVE_KEYS
                            + ")\\b[\"']?)(\\s*[:=]\\s*)(\\\"[^\\\"]*\\\"|'[^']*'|[^\\s,;&]+)",
                    Pattern.CASE_INSENSITIVE);

    private DryRunConnectFailureMessageSanitizer() {}

    /** Returns a CLI-safe failure message without JDBC URLs or credential-like values. */
    public static String sanitize(String message) {
        if (message == null || message.isEmpty()) {
            return message;
        }

        String sanitized = JDBC_URL.matcher(message).replaceAll("the configured JDBC URL");
        sanitized = maskSensitiveFreeTextKeys(sanitized);
        return truncate(sanitized);
    }

    private static String maskSensitiveFreeTextKeys(String message) {
        Matcher matcher = SENSITIVE_FREE_TEXT_KEY.matcher(message);
        StringBuffer sanitized = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(
                    sanitized,
                    Matcher.quoteReplacement(matcher.group(1) + matcher.group(2) + MASK));
        }
        matcher.appendTail(sanitized);
        return sanitized.toString();
    }

    private static String truncate(String message) {
        if (message.length() <= MAX_MESSAGE_LENGTH) {
            return message;
        }
        return message.substring(0, MAX_MESSAGE_LENGTH) + "...";
    }
}
