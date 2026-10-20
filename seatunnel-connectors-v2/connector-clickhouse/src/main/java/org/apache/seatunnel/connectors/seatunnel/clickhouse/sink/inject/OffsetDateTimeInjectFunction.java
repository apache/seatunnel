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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject;

import com.clickhouse.client.ClickHouseColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/** Binds UTC text together with an explicitly UTC SQL conversion to preserve the instant. */
public class OffsetDateTimeInjectFunction extends DateTimeInjectFunction {
    private static final DateTimeFormatter SECONDS =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FRACTIONS =
            new DateTimeFormatterBuilder()
                    .append(SECONDS)
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();
    private final boolean dateTime64;
    private final String parameterExpression;

    public OffsetDateTimeInjectFunction(String targetType) {
        dateTime64 = targetType.startsWith("DateTime64");
        parameterExpression =
                dateTime64
                        ? "toDateTime64(?, "
                                + ClickHouseColumn.of("timestamp", targetType).getScale()
                                + ", 'UTC')"
                        : "toDateTime(?, 'UTC')";
    }

    public String getParameterExpression() {
        return parameterExpression;
    }

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        OffsetDateTime utc = ((OffsetDateTime) value).withOffsetSameInstant(ZoneOffset.UTC);
        statement.setString(index, (dateTime64 ? FRACTIONS : SECONDS).format(utc));
    }
}
