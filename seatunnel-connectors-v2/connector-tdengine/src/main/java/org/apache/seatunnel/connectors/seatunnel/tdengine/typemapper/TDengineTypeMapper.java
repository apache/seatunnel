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

package org.apache.seatunnel.connectors.seatunnel.tdengine.typemapper;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.tdengine.exception.TDengineConnectorException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TDengineTypeMapper {

    // ============================data types=====================

    private static final String TDENGINE_UNKNOWN = "UNKNOWN";
    private static final String TDENGINE_BIT = "BIT";
    private static final String TDENGINE_BOOL = "BOOL";

    // -------------------------number----------------------------
    private static final String TDENGINE_TINYINT = "TINYINT";
    private static final String TDENGINE_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TDENGINE_SMALLINT = "SMALLINT";
    private static final String TDENGINE_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String TDENGINE_MEDIUMINT = "MEDIUMINT";
    private static final String TDENGINE_MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String TDENGINE_INT = "INT";
    private static final String TDENGINE_INT_UNSIGNED = "INT UNSIGNED";
    private static final String TDENGINE_INTEGER = "INTEGER";
    private static final String TDENGINE_INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String TDENGINE_BIGINT = "BIGINT";
    private static final String TDENGINE_BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String TDENGINE_DECIMAL = "DECIMAL";
    private static final String TDENGINE_DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String TDENGINE_FLOAT = "FLOAT";
    private static final String TDENGINE_FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String TDENGINE_DOUBLE = "DOUBLE";
    private static final String TDENGINE_DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";

    // -------------------------string----------------------------
    private static final String TDENGINE_CHAR = "CHAR";
    private static final String TDENGINE_NCHAR = "NCHAR";
    private static final String TDENGINE_VARCHAR = "VARCHAR";
    private static final String TDENGINE_TINYTEXT = "TINYTEXT";
    private static final String TDENGINE_MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TDENGINE_TEXT = "TEXT";
    private static final String TDENGINE_LONGTEXT = "LONGTEXT";
    private static final String TDENGINE_JSON = "JSON";

    // ------------------------------time-------------------------
    private static final String TDENGINE_DATE = "DATE";
    private static final String TDENGINE_DATETIME = "DATETIME";
    private static final String TDENGINE_TIME = "TIME";
    private static final String TDENGINE_TIMESTAMP = "TIMESTAMP";
    private static final String TDENGINE_YEAR = "YEAR";

    // ------------------------------blob-------------------------
    private static final String TDENGINE_TINYBLOB = "TINYBLOB";
    private static final String TDENGINE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String TDENGINE_BLOB = "BLOB";
    private static final String TDENGINE_LONGBLOB = "LONGBLOB";
    private static final String TDENGINE_BINARY = "BINARY";
    private static final String TDENGINE_VARBINARY = "VARBINARY";
    private static final String TDENGINE_GEOMETRY = "GEOMETRY";

    public static SeaTunnelDataType<?> mapping(String tdengineType) {
        switch (tdengineType) {
            case TDENGINE_BOOL:
            case TDENGINE_BIT:
                return BasicType.BOOLEAN_TYPE;
            case TDENGINE_TINYINT:
            case TDENGINE_TINYINT_UNSIGNED:
            case TDENGINE_SMALLINT:
            case TDENGINE_SMALLINT_UNSIGNED:
            case TDENGINE_MEDIUMINT:
            case TDENGINE_MEDIUMINT_UNSIGNED:
            case TDENGINE_INT:
            case TDENGINE_INTEGER:
            case TDENGINE_YEAR:
                return BasicType.INT_TYPE;
            case TDENGINE_INT_UNSIGNED:
            case TDENGINE_INTEGER_UNSIGNED:
            case TDENGINE_BIGINT:
                return BasicType.LONG_TYPE;
            case TDENGINE_BIGINT_UNSIGNED:
                return new DecimalType(20, 0);
            case TDENGINE_DECIMAL:
                log.warn("{} will probably cause value overflow.", TDENGINE_DECIMAL);
                return new DecimalType(38, 18);
            case TDENGINE_DECIMAL_UNSIGNED:
                return new DecimalType(38, 18);
            case TDENGINE_FLOAT:
                return BasicType.FLOAT_TYPE;
            case TDENGINE_FLOAT_UNSIGNED:
                log.warn("{} will probably cause value overflow.", TDENGINE_FLOAT_UNSIGNED);
                return BasicType.FLOAT_TYPE;
            case TDENGINE_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case TDENGINE_DOUBLE_UNSIGNED:
                log.warn("{} will probably cause value overflow.", TDENGINE_DOUBLE_UNSIGNED);
                return BasicType.DOUBLE_TYPE;
            case TDENGINE_CHAR:
            case TDENGINE_NCHAR:
            case TDENGINE_TINYTEXT:
            case TDENGINE_MEDIUMTEXT:
            case TDENGINE_TEXT:
            case TDENGINE_VARCHAR:
            case TDENGINE_JSON:
            case TDENGINE_LONGTEXT:
                return BasicType.STRING_TYPE;
            case TDENGINE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TDENGINE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TDENGINE_DATETIME:
            case TDENGINE_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;

            case TDENGINE_TINYBLOB:
            case TDENGINE_MEDIUMBLOB:
            case TDENGINE_BLOB:
            case TDENGINE_LONGBLOB:
            case TDENGINE_VARBINARY:
            case TDENGINE_BINARY:
                return PrimitiveByteArrayType.INSTANCE;

                // Doesn't support yet
            case TDENGINE_GEOMETRY:
            case TDENGINE_UNKNOWN:
            default:
                throw new TDengineConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format("Doesn't support TDENGINE type '%s' yet.", tdengineType));
        }
    }
}
