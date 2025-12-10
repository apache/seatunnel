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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MySqlDialectFactoryTest {

    @Test
    void testTimestampUsesOffsetDateTimeWhenServerTimeZoneConfigured() {
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .url("jdbc:mysql://localhost:3306/test")
                        .driverName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .build();

        MySqlDialectFactory factory = new MySqlDialectFactory();
        JdbcDialect dialect = factory.create("mysql", null, config);

        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("ts")
                        .columnType("timestamp")
                        .dataType("timestamp")
                        .build();

        Column column = dialect.getTypeConverter().convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    void testTimestampUsesLocalDateTimeWhenServerTimeZoneNotConfigured() {
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .url("jdbc:mysql://localhost:3306/test")
                        .driverName("com.mysql.cj.jdbc.Driver")
                        .build();

        MySqlDialectFactory factory = new MySqlDialectFactory();
        JdbcDialect dialect = factory.create("mysql", null, config);

        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("ts")
                        .columnType("timestamp")
                        .dataType("timestamp")
                        .build();

        Column column = dialect.getTypeConverter().convert(typeDefine);
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
    }
}
