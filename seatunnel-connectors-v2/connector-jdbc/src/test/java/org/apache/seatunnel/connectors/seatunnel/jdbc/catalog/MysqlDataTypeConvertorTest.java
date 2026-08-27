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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;

import java.util.Collections;

public class MysqlDataTypeConvertorTest {

    private final MysqlDataTypeConvertor mysqlDataTypeConvertor = new MysqlDataTypeConvertor();

    @Test
    public void toSeaTunnelTypeWithString() {
        Assertions.assertEquals(
                new DecimalType(5, 2), mysqlDataTypeConvertor.toSeaTunnelType("", "DECIMAL(5,2)"));

        Assertions.assertEquals(
                new DecimalType(5, 0), mysqlDataTypeConvertor.toSeaTunnelType("", "DECIMAL(5)"));

        Assertions.assertEquals(
                new DecimalType(10, 0), mysqlDataTypeConvertor.toSeaTunnelType("", "DECIMAL"));
    }

    @Test
    public void toSeaTunnelType() {
        Assertions.assertEquals(
                BasicType.VOID_TYPE,
                mysqlDataTypeConvertor.toSeaTunnelType("", MysqlType.NULL, Collections.emptyMap()));
        Assertions.assertEquals(
                BasicType.STRING_TYPE,
                mysqlDataTypeConvertor.toSeaTunnelType(
                        "", MysqlType.VARCHAR, Collections.emptyMap()));
    }
}
