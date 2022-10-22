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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class DateUtils {
    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP = new HashMap<>();

    static {
        FORMATTER_MAP.put(Formatter.YYYY_MM_DD, DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD.value));
        FORMATTER_MAP.put(Formatter.YYYY_MM_DD_SPOT, DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_SPOT.value));
        FORMATTER_MAP.put(Formatter.YYYY_MM_DD_SLASH, DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_SLASH.value));
    }

    public static LocalDate parse(String date, Formatter formatter) {
        return LocalDate.parse(date, FORMATTER_MAP.get(formatter));
    }

    public static String toString(LocalDate date, Formatter formatter) {
        return date.format(FORMATTER_MAP.get(formatter));
    }

    public enum Formatter {
        YYYY_MM_DD("yyyy-MM-dd"),
        YYYY_MM_DD_SPOT("yyyy.MM.dd"),
        YYYY_MM_DD_SLASH("yyyy/MM/dd");
        private final String value;

        Formatter(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Formatter parse(String format) {
            Formatter[] formatters = Formatter.values();
            for (Formatter formatter : formatters) {
                if (formatter.getValue().equals(format)) {
                    return formatter;
                }
            }
            String errorMsg = String.format("Illegal format [%s]", format);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
