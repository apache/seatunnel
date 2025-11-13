/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MysqlJdbcRowConverterTest {

    private TableSchema buildSchemaWithTimestampTz() {
        return TableSchema.builder()
                .column(
                        PhysicalColumn.of(
                                "ts_tz",
                                LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                6,
                                true,
                                null,
                                "ts_tz"))
                .build();
    }

    @Test
    public void testReadTimestampTzNull() throws Exception {
        TableSchema schema = buildSchemaWithTimestampTz();
        ResultSet rs = mock(ResultSet.class);
        when(rs.getTimestamp(1)).thenReturn(null);

        MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
        SeaTunnelRow row = converter.toInternal(rs, schema);

        Assertions.assertNull(row.getField(0));
    }

    @Test
    public void testReadTimestampTzWithSystemDefaultZone() throws Exception {
        TableSchema schema = buildSchemaWithTimestampTz();
        ResultSet rs = mock(ResultSet.class);

        ZoneId systemZone = ZoneId.systemDefault();
        Instant instant = Instant.parse("2021-06-01T00:00:00Z");
        when(rs.getTimestamp(1)).thenReturn(Timestamp.from(instant));

        MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
        SeaTunnelRow row = converter.toInternal(rs, schema);

        OffsetDateTime expected = instant.atZone(systemZone).toOffsetDateTime();
        Assertions.assertEquals(expected, row.getField(0));
    }
}
