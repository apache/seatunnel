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

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift.RedshiftDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.snowflake.SnowflakeDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb.TiDBDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static com.mysql.cj.MysqlType.UNKNOWN;

public class DataTypeConvertorTest {

    @Test
    void testConvertorErrorMsgWithUnsupportedType() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(new String[0], new SeaTunnelDataType[0]);
        MultipleRowType multipleRowType =
                new MultipleRowType(new String[] {"table"}, new SeaTunnelRowType[] {rowType});

        DamengDataTypeConvertor dameng = new DamengDataTypeConvertor();
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> dameng.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Doesn't support DMDB type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception.getMessage());
        JdbcConnectorException exception2 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> dameng.toSeaTunnelType("test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Doesn't support DMDB type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception2.getMessage());
        UnsupportedOperationException exception3 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> dameng.toConnectorType("test", rowType, new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support SeaTunnel type 'ROW<>' of the 'test' field yet.",
                exception3.getMessage());

        MysqlDataTypeConvertor mysql = new MysqlDataTypeConvertor();
        DataTypeConvertException exception4 =
                Assertions.assertThrows(
                        DataTypeConvertException.class,
                        () -> mysql.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Convert type: UNKNOWN of the test field to SeaTunnel data type error.",
                exception4.getMessage());
        DataTypeConvertException exception5 =
                Assertions.assertThrows(
                        DataTypeConvertException.class,
                        () -> mysql.toSeaTunnelType("test", UNKNOWN, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Convert type: UNKNOWN of the test field to SeaTunnel data type error.",
                exception5.getMessage());
        JdbcConnectorException exception6 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> mysql.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Doesn't support MySQL type 'MULTIPLE_ROW' of the 'test' field yet",
                exception6.getMessage());

        OracleDataTypeConvertor oracle = new OracleDataTypeConvertor();
        JdbcConnectorException exception7 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> oracle.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Doesn't support ORACLE type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception7.getMessage());
        JdbcConnectorException exception8 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> oracle.toSeaTunnelType("test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Doesn't support ORACLE type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception8.getMessage());
        UnsupportedOperationException exception9 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> oracle.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support SeaTunnel type 'MULTIPLE_ROW' of the 'test' field yet.",
                exception9.getMessage());

        PostgresDataTypeConvertor postgres = new PostgresDataTypeConvertor();
        UnsupportedOperationException exception10 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> postgres.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "Doesn't support POSTGRES type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception10.getMessage());
        UnsupportedOperationException exception11 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                postgres.toSeaTunnelType(
                                        "test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support POSTGRES type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception11.getMessage());
        UnsupportedOperationException exception12 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> postgres.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support SeaTunnel type 'MULTIPLE_ROW' of the 'test' field yet.",
                exception12.getMessage());

        RedshiftDataTypeConvertor redshift = new RedshiftDataTypeConvertor();
        UnsupportedOperationException exception13 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> redshift.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "Doesn't support REDSHIFT type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception13.getMessage());
        UnsupportedOperationException exception14 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                redshift.toSeaTunnelType(
                                        "test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support REDSHIFT type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception14.getMessage());
        UnsupportedOperationException exception15 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> redshift.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support SeaTunnel type 'MULTIPLE_ROW' of the 'test' field yet.",
                exception15.getMessage());

        SnowflakeDataTypeConvertor snowflake = new SnowflakeDataTypeConvertor();
        UnsupportedOperationException exception16 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> snowflake.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "Doesn't support SNOWFLAKE type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception16.getMessage());
        UnsupportedOperationException exception17 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                snowflake.toSeaTunnelType(
                                        "test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support SNOWFLAKE type 'UNSUPPORTED_TYPE' of the 'test' field yet.",
                exception17.getMessage());
        UnsupportedOperationException exception18 =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> snowflake.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "Doesn't support SeaTunnel type 'MULTIPLE_ROW' of the 'test' field yet.",
                exception18.getMessage());

        SqlServerDataTypeConvertor sqlserver = new SqlServerDataTypeConvertor();
        JdbcConnectorException exception19 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> sqlserver.toSeaTunnelType("test", "unknown"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Doesn't support SQLSERVER type 'UNKNOWN' of the 'test' field",
                exception19.getMessage());
        JdbcConnectorException exception20 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                sqlserver.toSeaTunnelType(
                                        "test", SqlServerType.UNKNOWN, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-05], ErrorDescription:[Unsupported operation] - Doesn't support SQLSERVER type 'UNKNOWN' of the 'test' field",
                exception20.getMessage());
        JdbcConnectorException exception21 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> sqlserver.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Doesn't support SqlServer type 'MULTIPLE_ROW' of the 'test' field yet",
                exception21.getMessage());

        TiDBDataTypeConvertor tidb = new TiDBDataTypeConvertor();
        DataTypeConvertException exception22 =
                Assertions.assertThrows(
                        DataTypeConvertException.class,
                        () -> tidb.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Convert type: UNKNOWN of the test field to SeaTunnel data type error.",
                exception22.getMessage());
        DataTypeConvertException exception23 =
                Assertions.assertThrows(
                        DataTypeConvertException.class,
                        () -> tidb.toSeaTunnelType("test", UNKNOWN, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - Convert type: UNKNOWN of the test field to SeaTunnel data type error.",
                exception23.getMessage());
        JdbcConnectorException exception24 =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> tidb.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-07], ErrorDescription:[Unsupported data type] - TiDB doesn't support SeaTunnel type 'MULTIPLE_ROW' of the 'test' field yet",
                exception24.getMessage());
    }
}
