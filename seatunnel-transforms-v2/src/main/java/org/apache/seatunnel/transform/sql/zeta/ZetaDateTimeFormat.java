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

package org.apache.seatunnel.transform.sql.zeta;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;

public enum ZetaDateTimeFormat {
    // DateTime formats
    DATETIME_STANDARD("yyyy-MM-dd HH:mm:ss", FormatType.DATETIME),
    DATETIME_WITH_MILLIS("yyyy-MM-dd HH:mm:ss.SSS", FormatType.DATETIME),
    DATETIME_ISO8601("yyyy-MM-dd'T'HH:mm:ss", FormatType.DATETIME),
    DATETIME_ISO8601_WITH_MILLIS("yyyy-MM-dd'T'HH:mm:ss.SSS", FormatType.DATETIME),
    DATETIME_SLASH("yyyy/MM/dd HH:mm:ss", FormatType.DATETIME),
    DATETIME_SLASH_WITH_MILLIS("yyyy/MM/dd HH:mm:ss.SSS", FormatType.DATETIME),
    DATETIME_COMPACT("yyyyMMddHHmmss", FormatType.DATETIME),

    // Date formats
    DATE_ISO8601("yyyy-MM-dd", FormatType.DATE),
    DATE_SLASH("yyyy/MM/dd", FormatType.DATE),
    DATE_COMPACT("yyyyMMdd", FormatType.DATE),

    // Time formats
    TIME_STANDARD("HH:mm:ss", FormatType.TIME),
    TIME_WITH_MILLIS("HH:mm:ss.SSS", FormatType.TIME),
    TIME_COMPACT("HHmmss", FormatType.TIME);

    private final String pattern;
    private final FormatType type;
    private final DateTimeFormatter formatter;

    ZetaDateTimeFormat(String pattern, FormatType type) {
        this.pattern = pattern;
        this.type = type;
        this.formatter = DateTimeFormatter.ofPattern(pattern);
    }

    public String getPattern() {
        return pattern;
    }

    public FormatType getType() {
        return type;
    }

    public DateTimeFormatter getFormatter() {
        return formatter;
    }

    public static Optional<ZetaDateTimeFormat> fromPattern(String pattern) {
        return Arrays.stream(values()).filter(format -> format.pattern.equals(pattern)).findFirst();
    }

    public enum FormatType {
        DATETIME,
        DATE,
        TIME
    }
}
