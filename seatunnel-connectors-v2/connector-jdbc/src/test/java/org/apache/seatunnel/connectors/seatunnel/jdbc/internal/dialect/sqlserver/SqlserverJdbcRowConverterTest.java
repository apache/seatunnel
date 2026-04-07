/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;

class SqlserverJdbcRowConverterTest {

    @Test
    void testToInternalReadsDatetimeOffsetAsOffsetDateTime() throws Exception {
        SqlserverJdbcRowConverter converter = new SqlserverJdbcRowConverter();
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        OffsetDateTime expected = OffsetDateTime.parse("2024-12-16T21:02:09.799+05:30");
        Mockito.when(resultSet.getObject(1, OffsetDateTime.class)).thenReturn(expected);

        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "datetimeoffset_col",
                                        LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        null))
                        .build();

        SeaTunnelRow row = converter.toInternal(resultSet, tableSchema);

        Assertions.assertEquals(expected, row.getField(0));
        Mockito.verify(resultSet).getObject(1, OffsetDateTime.class);
    }

    @Test
    void testToExternalWritesDatetimeOffsetAsTimestampWithTimezone() throws Exception {
        SqlserverJdbcRowConverter converter = new SqlserverJdbcRowConverter();
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        OffsetDateTime value = OffsetDateTime.parse("2024-12-16T21:02:09.799+05:30");
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {value});
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"datetimeoffset_col"},
                        new SeaTunnelDataType[] {LocalTimeType.OFFSET_DATE_TIME_TYPE});

        converter.toExternal(rowType, row, statement);

        Mockito.verify(statement).setObject(1, value, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
    }
}
