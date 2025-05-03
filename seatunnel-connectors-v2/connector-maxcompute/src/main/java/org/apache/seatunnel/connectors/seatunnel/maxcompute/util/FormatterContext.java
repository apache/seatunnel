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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class FormatterContext {
    private final String localDateTimeFormat;
    private final String offsetDateTimeFormat;

    public FormatterContext(String localDateTimeFormat, String offsetDateTimeFormat) {
        this.localDateTimeFormat = localDateTimeFormat;
        this.offsetDateTimeFormat = offsetDateTimeFormat;
    }

    public boolean isDateTimeType(Object field) {
        if (field instanceof LocalDateTime) {
            return true;
        }
        if (field instanceof OffsetDateTime) {
            return true;
        }
        return false;
    }

    public String formatDateTime(Object field) {
        if (field instanceof LocalDateTime) {
            return this.format(((LocalDateTime) field));
        }
        if (field instanceof OffsetDateTime) {
            return this.format(((OffsetDateTime) field));
        }
        return String.valueOf(field);
    }

    private String format(LocalDateTime localDateTime) {
        return localDateTime.format(DateTimeFormatter.ofPattern(localDateTimeFormat));
    }

    private String format(OffsetDateTime offsetDateTime) {
        return offsetDateTime.format(DateTimeFormatter.ofPattern(offsetDateTimeFormat));
    }
}
