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

package org.apache.seatunnel.connectors.seatunnel.databend.util;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;

import lombok.extern.slf4j.Slf4j;

/** Type converter for Databend data types */
@Slf4j
public class DatabendTypeConverter {

    /** Convert SeaTunnel Column to Databend compatible BasicTypeDefine */
    public static BasicTypeDefine convertToDatabendType(Column column) {
        SqlType sqlType = column.getDataType().getSqlType();
        String databendType = mapToDatabendType(sqlType, column);

        return BasicTypeDefine.builder()
                .name(column.getName())
                .columnType(databendType)
                .nullable(column.isNullable())
                .comment(column.getComment())
                .defaultValue(column.getDefaultValue())
                .build();
    }

    /** Map SeaTunnel SqlType to Databend data type */
    private static String mapToDatabendType(SqlType sqlType, Column column) {
        switch (sqlType) {
            case STRING:
                return "STRING";
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                return String.format(
                        "DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BYTES:
                return "VARBINARY";
            case ARRAY:
                return "ARRAY(STRING)";
            case MAP:
                return "MAP(STRING, STRING)";
            case NULL:
                return "NULL";
            case ROW:
                return "STRING";
            default:
                log.warn("Unsupported SQL type: {}, fallback to STRING", sqlType);
                return "STRING";
        }
    }
}
