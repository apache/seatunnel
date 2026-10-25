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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MysqlJdbcRowConverterTest {

    @Test
    public void testTimeReadAndWritePreserveMicroseconds() throws Exception {
        LocalTime expected = LocalTime.parse("12:34:56.123456");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("time_col")
                                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                        .build())
                        .build();
        MysqlJdbcRowConverter converter = new MysqlJdbcRowConverter();
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1, LocalTime.class)).thenReturn(expected);

        SeaTunnelRow result = converter.toInternal(resultSet, tableSchema);
        assertEquals(expected, result.getField(0));

        PreparedStatement statement = mock(PreparedStatement.class);
        converter.toExternal(tableSchema, new SeaTunnelRow(new Object[] {expected}), statement);
        ArgumentCaptor<Timestamp> timestamp = ArgumentCaptor.forClass(Timestamp.class);
        verify(statement).setTimestamp(org.mockito.ArgumentMatchers.eq(1), timestamp.capture());
        assertEquals(expected.getNano(), timestamp.getValue().getNanos());
    }
}
