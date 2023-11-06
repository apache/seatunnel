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

import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift.RedshiftDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.snowflake.SnowflakeDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerDataTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb.TiDBDataTypeConvertor;

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
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> dameng.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Dameng' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception.getMessage());
        SeaTunnelRuntimeException exception2 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> dameng.toSeaTunnelType("test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Dameng' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception2.getMessage());
        SeaTunnelRuntimeException exception3 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> dameng.toConnectorType("test", rowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['Dameng' unsupported convert SeaTunnel data type 'ROW<>' of 'test' to connector data type.]",
                exception3.getMessage());

        MysqlDataTypeConvertor mysql = new MysqlDataTypeConvertor();
        SeaTunnelRuntimeException exception4 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> mysql.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['MySQL' unsupported convert type 'UNKNOWN' of 'test' to SeaTunnel data type.]",
                exception4.getMessage());
        SeaTunnelRuntimeException exception5 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> mysql.toSeaTunnelType("test", UNKNOWN, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['MySQL' unsupported convert type 'UNKNOWN' of 'test' to SeaTunnel data type.]",
                exception5.getMessage());
        SeaTunnelRuntimeException exception6 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> mysql.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['MySQL' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception6.getMessage());

        OracleDataTypeConvertor oracle = new OracleDataTypeConvertor();
        SeaTunnelRuntimeException exception7 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> oracle.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Oracle' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception7.getMessage());
        SeaTunnelRuntimeException exception8 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> oracle.toSeaTunnelType("test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Oracle' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception8.getMessage());
        SeaTunnelRuntimeException exception9 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> oracle.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['Oracle' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception9.getMessage());

        PostgresDataTypeConvertor postgres = new PostgresDataTypeConvertor();
        SeaTunnelRuntimeException exception10 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> postgres.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Postgres' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception10.getMessage());
        SeaTunnelRuntimeException exception11 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                postgres.toSeaTunnelType(
                                        "test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Postgres' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception11.getMessage());
        SeaTunnelRuntimeException exception12 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> postgres.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['Postgres' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception12.getMessage());

        RedshiftDataTypeConvertor redshift = new RedshiftDataTypeConvertor();
        SeaTunnelRuntimeException exception13 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> redshift.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Redshift' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception13.getMessage());
        SeaTunnelRuntimeException exception14 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                redshift.toSeaTunnelType(
                                        "test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Redshift' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception14.getMessage());
        SeaTunnelRuntimeException exception15 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> redshift.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['Redshift' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception15.getMessage());

        SnowflakeDataTypeConvertor snowflake = new SnowflakeDataTypeConvertor();
        SeaTunnelRuntimeException exception16 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> snowflake.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Snowflake' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception16.getMessage());
        SeaTunnelRuntimeException exception17 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                snowflake.toSeaTunnelType(
                                        "test", "UNSUPPORTED_TYPE", new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Snowflake' unsupported convert type 'UNSUPPORTED_TYPE' of 'test' to SeaTunnel data type.]",
                exception17.getMessage());
        SeaTunnelRuntimeException exception18 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> snowflake.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['Snowflake' unsupported convert type 'MULTIPLE_ROW' of 'test' to SeaTunnel data type.]",
                exception18.getMessage());

        SqlServerDataTypeConvertor sqlserver = new SqlServerDataTypeConvertor();
        SeaTunnelRuntimeException exception19 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> sqlserver.toSeaTunnelType("test", "unknown"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['SqlServer' unsupported convert type 'UNKNOWN' of 'test' to SeaTunnel data type.]",
                exception19.getMessage());
        SeaTunnelRuntimeException exception20 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                sqlserver.toSeaTunnelType(
                                        "test", SqlServerType.UNKNOWN, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['SqlServer' unsupported convert type 'UNKNOWN' of 'test' to SeaTunnel data type.]",
                exception20.getMessage());
        SeaTunnelRuntimeException exception21 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> sqlserver.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['SqlServer' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception21.getMessage());

        TiDBDataTypeConvertor tidb = new TiDBDataTypeConvertor();
        SeaTunnelRuntimeException exception22 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> tidb.toSeaTunnelType("test", "UNSUPPORTED_TYPE"));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['TiDB' unsupported convert type 'UNKNOWN' of 'test' to SeaTunnel data type.]",
                exception22.getMessage());
        SeaTunnelRuntimeException exception23 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> tidb.toSeaTunnelType("test", UNKNOWN, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-17], ErrorDescription:['TiDB' unsupported convert type 'UNKNOWN' of 'test' to SeaTunnel data type.]",
                exception23.getMessage());
        SeaTunnelRuntimeException exception24 =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> tidb.toConnectorType("test", multipleRowType, new HashMap<>()));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-19], ErrorDescription:['TiDB' unsupported convert SeaTunnel data type 'MULTIPLE_ROW' of 'test' to connector data type.]",
                exception24.getMessage());
    }
}
