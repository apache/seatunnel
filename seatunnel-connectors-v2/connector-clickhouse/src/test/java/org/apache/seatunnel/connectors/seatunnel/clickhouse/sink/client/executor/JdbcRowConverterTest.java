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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.executor;

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class JdbcRowConverterTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "DateTime",
                "DateTime('Asia/Shanghai')",
                "DateTime64(0)",
                "DateTime64(3)",
                "DateTime64(9, 'UTC')",
                "Nullable(DateTime64(6, 'America/New_York'))"
            })
    void bindsOffsetDateTimeWithoutDiscardingOffsetOrPrecision(String targetType) throws Exception {
        JdbcRowConverter converter = converter(targetType);
        for (String literal :
                new String[] {
                    "2026-09-12T10:00:00.123456789Z",
                    "2026-09-12T10:00:00.123456789+05:30",
                    "2026-09-12T10:00:00.123456789-07:00"
                }) {
            PreparedStatement statement = mock(PreparedStatement.class);
            OffsetDateTime value = OffsetDateTime.parse(literal);
            converter.toExternal(new SeaTunnelRow(new Object[] {value}), statement);
            String expected =
                    value.withOffsetSameInstant(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss"));
            if (targetType.contains("DateTime64")) {
                expected += ".123456789";
            }
            verify(statement).setString(1, expected);
            verifyNoMoreInteractions(statement);
        }
    }

    @Test
    void preservesExistingTimestampBindingsAndNulls() throws Exception {
        JdbcRowConverter converter =
                new JdbcRowConverter(
                        new SeaTunnelRowType(
                                new String[] {"event_time"},
                                new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE}),
                        Collections.singletonMap("event_time", "Nullable(DateTime64(9, 'UTC'))"),
                        new String[] {"event_time"});
        LocalDateTime local = LocalDateTime.parse("2026-09-12T10:00:00.123456789");
        Timestamp timestamp = Timestamp.valueOf(local);
        for (Object value :
                new Object[] {local, timestamp, "2026-09-12 10:00:00.123456789", null}) {
            PreparedStatement statement = mock(PreparedStatement.class);
            converter.toExternal(new SeaTunnelRow(new Object[] {value}), statement);
            if (value == null || value instanceof LocalDateTime) {
                verify(statement).setObject(1, value);
            } else {
                verify(statement).setTimestamp(1, timestamp);
            }
            verifyNoMoreInteractions(statement);
        }
    }

    @Test
    void wrapsOnlyRecognizedTimestampParametersAndPreservesRepeatedBindings() throws Exception {
        String[] fields = {"id", "id2"};
        Map<String, String> target = new HashMap<>();
        target.put("id", "Nullable(DateTime64(6, 'America/New_York'))");
        target.put("id2", "DateTime64(3, 'UTC')");
        JdbcRowConverter converter =
                new JdbcRowConverter(
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {
                                    LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                    LocalTimeType.LOCAL_DATE_TIME_TYPE
                                }),
                        target,
                        fields);
        Connection connection = mock(Connection.class);
        PreparedStatement delegate = mock(PreparedStatement.class);
        String expected =
                "SELECT 1 FROM events WHERE id = toDateTime64(?, 6, 'UTC') AND id2 = ? OR id = toDateTime64(?, 6, 'UTC')";
        when(connection.prepareStatement(expected)).thenReturn(delegate);
        PreparedStatement statement =
                converter.prepareStatement(
                        connection,
                        "SELECT 1 FROM events WHERE id = :id AND id2 = :id2 OR id = :id",
                        fields);
        converter.toExternal(
                new SeaTunnelRow(
                        new Object[] {
                            OffsetDateTime.parse("2026-11-01T01:30:00.123456789-05:00"),
                            LocalDateTime.parse("2026-09-12T10:00:00")
                        }),
                statement);
        verify(connection).prepareStatement(expected);
        verify(delegate).setString(1, "2026-11-01 06:30:00.123456789");
        verify(delegate).setString(3, "2026-11-01 06:30:00.123456789");
        verify(delegate).setObject(2, LocalDateTime.parse("2026-09-12T10:00:00"));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    void preservesSqlForExistingTimestampTypes() throws Exception {
        String[] fields = {"event_time"};
        JdbcRowConverter converter =
                new JdbcRowConverter(
                        new SeaTunnelRowType(
                                fields,
                                new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE}),
                        Collections.singletonMap("event_time", "DateTime64(9, 'UTC')"),
                        fields);
        Connection connection = mock(Connection.class);
        converter.prepareStatement(
                connection, SqlUtils.getInsertIntoStatement("events", fields), fields);
        verify(connection).prepareStatement("INSERT INTO events (\"event_time\") VALUES (?)");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3, 6, 9})
    void preservesTargetDateTime64Scale(int scale) throws Exception {
        String[] fields = {"event_time"};
        Connection connection = mock(Connection.class);
        converter("DateTime64(" + scale + ")")
                .prepareStatement(
                        connection, SqlUtils.getInsertIntoStatement("events", fields), fields);
        verify(connection)
                .prepareStatement(
                        "INSERT INTO events (\"event_time\") VALUES (toDateTime64(?, "
                                + scale
                                + ", 'UTC'))");
    }

    @Test
    void bindsNullWithoutFormattingIt() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        converter("Nullable(DateTime64(3, 'UTC'))")
                .toExternal(new SeaTunnelRow(new Object[] {null}), statement);
        verify(statement).setObject(1, null);
        verifyNoMoreInteractions(statement);
    }

    @Test
    void rejectsPositionalTimestampParametersWithoutUtcConversion() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        converter("DateTime64(9, 'UTC')")
                                .prepareStatement(
                                        mock(Connection.class),
                                        "INSERT INTO events VALUES (?)",
                                        new String[] {"event_time"}));
    }

    private JdbcRowConverter converter(String targetType) {
        return new JdbcRowConverter(
                new SeaTunnelRowType(
                        new String[] {"event_time"},
                        new SeaTunnelDataType[] {LocalTimeType.OFFSET_DATE_TIME_TYPE}),
                Collections.singletonMap("event_time", targetType),
                new String[] {"event_time"});
    }
}
