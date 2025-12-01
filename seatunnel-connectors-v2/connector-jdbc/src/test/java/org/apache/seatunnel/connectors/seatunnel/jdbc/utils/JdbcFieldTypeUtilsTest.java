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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcFieldTypeUtilsTest {

    @Test
    public void testGetOffsetDateTimeFromTimestampUsesInstant() throws SQLException {
        Instant instant = Instant.parse("2025-01-01T00:00:00Z");
        Timestamp timestamp = Timestamp.from(instant);

        ResultSet rs = mock(ResultSet.class);
        when(rs.getObject(1)).thenReturn(timestamp);
        OffsetDateTime result = JdbcFieldTypeUtils.getOffsetDateTime(rs, 1);

        assertEquals(instant, result.toInstant());
        assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    @Test
    public void testGetOffsetDateTimeFromDate() throws SQLException {
        Instant instant = Instant.parse("2025-02-02T12:34:56Z");
        Date date = Date.from(instant);

        ResultSet rs = mock(ResultSet.class);
        when(rs.getObject(1)).thenReturn(date);
        OffsetDateTime result = JdbcFieldTypeUtils.getOffsetDateTime(rs, 1);

        assertEquals(instant, result.toInstant());
        assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    @Test
    public void testGetOffsetDateTimeFromEpochMilli() throws SQLException {
        Instant instant = Instant.parse("2025-03-03T08:00:00Z");
        long epochMilli = instant.toEpochMilli();

        ResultSet rs = mock(ResultSet.class);
        when(rs.getObject(1)).thenReturn(epochMilli);
        OffsetDateTime result = JdbcFieldTypeUtils.getOffsetDateTime(rs, 1);

        assertEquals(instant, result.toInstant());
        assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    @Test
    public void testGetOffsetDateTimeFromIsoString() throws SQLException {
        Instant instant = Instant.parse("2025-04-04T16:20:30Z");
        String value = "2025-04-04T16:20:30Z";

        ResultSet rs = mock(ResultSet.class);
        when(rs.getObject(1)).thenReturn(value);
        OffsetDateTime result = JdbcFieldTypeUtils.getOffsetDateTime(rs, 1);

        assertEquals(instant, result.toInstant());
        assertEquals(ZoneOffset.UTC, result.getOffset());
    }
}
