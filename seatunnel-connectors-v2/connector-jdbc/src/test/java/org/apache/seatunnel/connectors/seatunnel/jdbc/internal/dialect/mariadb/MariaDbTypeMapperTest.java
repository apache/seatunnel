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

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MariaDbTypeMapperTest {

    @Test
    public void testMappingColumnFromResultSetMetaData() throws SQLException {
        ResultSetMetaData metaData =
                (ResultSetMetaData)
                        Proxy.newProxyInstance(
                                ResultSetMetaData.class.getClassLoader(),
                                new Class<?>[] {ResultSetMetaData.class},
                                (proxy, method, args) -> {
                                    int col = (int) args[0];
                                    switch (method.getName()) {
                                        case "getColumnLabel":
                                            return col == 1 ? "user_id" : "is_active";
                                        case "getColumnTypeName":
                                            return col == 1 ? "INT" : "TINYINT";
                                        case "isNullable":
                                            return col == 1
                                                    ? ResultSetMetaData.columnNoNulls
                                                    : ResultSetMetaData.columnNullable;
                                        case "getPrecision":
                                            return col == 1 ? 10 : 1;
                                        case "getScale":
                                            return 0;
                                        default:
                                            return null;
                                    }
                                });

        MariaDbTypeMapper mapper = new MariaDbTypeMapper();
        Column col1 = mapper.mappingColumn(metaData, 1);
        Assertions.assertEquals("user_id", col1.getName());
        Assertions.assertEquals(BasicType.INT_TYPE, col1.getDataType());
        Assertions.assertFalse(col1.isNullable());

        Column col2 = mapper.mappingColumn(metaData, 2);
        Assertions.assertEquals("is_active", col2.getName());
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, col2.getDataType());
        Assertions.assertTrue(col2.isNullable());
    }
}
