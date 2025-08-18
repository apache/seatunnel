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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Minimal timezone utility class for Hive connector, following Iceberg's approach Only includes
 * methods that are actually used in the implementation
 */
public class HiveTimezoneUtils {

    /**
     * Convert various timestamp types to OffsetDateTime using system default timezone This follows
     * the same approach as Iceberg
     */
    public static OffsetDateTime convertToOffsetDateTime(Object value) {
        if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        } else if (value instanceof LocalDateTime) {
            // Convert to OffsetDateTime using the system(jvm) default timezone
            // This is the same approach as Iceberg
            return ((LocalDateTime) value)
                    .atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toOffsetDateTime();
        } else if (value instanceof Date) {
            // Convert Date to OffsetDateTime
            return ((Date) value).toInstant().atOffset(ZoneOffset.UTC);
        } else if (value instanceof Number) {
            // Convert timestamp millis to OffsetDateTime
            long millis = ((Number) value).longValue();
            return OffsetDateTime.ofInstant(java.time.Instant.ofEpochMilli(millis), ZoneOffset.UTC);
        }

        throw new RuntimeException(
                "Cannot convert to OffsetDateTime: "
                        + value
                        + ", type: "
                        + (value != null ? value.getClass() : "null"));
    }

    /** Generate timezone-aware table properties for Hive tables */
    public static String getTimezoneTableProperties() {
        ZoneId systemZone = ZoneId.systemDefault();
        return String.format("'seatunnel.timezone'='%s'", systemZone.getId());
    }
}
