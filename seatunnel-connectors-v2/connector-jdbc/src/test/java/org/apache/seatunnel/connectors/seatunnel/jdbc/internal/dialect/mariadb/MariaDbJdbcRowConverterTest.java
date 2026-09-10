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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mariadb;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicReference;

public class MariaDbJdbcRowConverterTest {

    @Test
    public void testConverterName() {
        MariaDbJdbcRowConverter rowConverter = new MariaDbJdbcRowConverter();
        Assertions.assertEquals(DatabaseIdentifier.MARIADB, rowConverter.converterName());
    }

    @Test
    public void testToExternalTime() throws SQLException {
        MariaDbJdbcRowConverter rowConverter = new MariaDbJdbcRowConverter();
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("t")
                                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                        .build())
                        .build();

        AtomicReference<Object> col1Value = new AtomicReference<>();
        AtomicReference<Timestamp> col2Value = new AtomicReference<>();

        PreparedStatement statement =
                (PreparedStatement)
                        Proxy.newProxyInstance(
                                PreparedStatement.class.getClassLoader(),
                                new Class<?>[] {PreparedStatement.class},
                                (proxy, method, args) -> {
                                    if ("setInt".equals(method.getName())) {
                                        col1Value.set(args[1]);
                                    } else if ("setTimestamp".equals(method.getName())) {
                                        col2Value.set((Timestamp) args[1]);
                                    }
                                    return null;
                                });

        LocalTime time = LocalTime.of(12, 34, 56, 123000000);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, time});

        rowConverter.toExternal(schema, row, statement);

        Assertions.assertEquals(1, col1Value.get());
        Assertions.assertNotNull(col2Value.get());
        Assertions.assertEquals(time, col2Value.get().toLocalDateTime().toLocalTime());
    }

    @Test
    public void testToInternalTime() throws SQLException {
        MariaDbJdbcRowConverter rowConverter = new MariaDbJdbcRowConverter();
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("t")
                                        .dataType(LocalTimeType.LOCAL_TIME_TYPE)
                                        .build())
                        .build();

        ResultSet rs =
                (ResultSet)
                        Proxy.newProxyInstance(
                                ResultSet.class.getClassLoader(),
                                new Class<?>[] {ResultSet.class},
                                (proxy, method, args) -> {
                                    if ("getInt".equals(method.getName())) {
                                        return 1;
                                    }
                                    if ("wasNull".equals(method.getName())) {
                                        return false;
                                    }
                                    if ("getObject".equals(method.getName())) {
                                        int col = (int) args[0];
                                        if (col == 1) {
                                            return 1;
                                        }
                                        if (args.length > 1 && args[1] == LocalTime.class) {
                                            return LocalTime.of(12, 34, 56);
                                        }
                                        return LocalTime.of(12, 34, 56);
                                    }
                                    return null;
                                });

        SeaTunnelRow row = rowConverter.toInternal(rs, schema);
        Assertions.assertNotNull(row);
        Assertions.assertEquals(1, row.getField(0));
        Assertions.assertEquals(LocalTime.of(12, 34, 56), row.getField(1));
    }
}
