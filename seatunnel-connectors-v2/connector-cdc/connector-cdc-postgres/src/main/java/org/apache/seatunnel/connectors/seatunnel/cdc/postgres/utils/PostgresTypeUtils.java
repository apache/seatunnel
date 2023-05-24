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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;

import java.sql.Types;
import java.util.List;

public class PostgresTypeUtils {
    private PostgresTypeUtils() {}

    public static SeaTunnelDataType<?> convertFromColumn(Column column) {
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.STRUCT:
            case Types.CLOB:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return BasicType.STRING_TYPE;
            case Types.BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            case Types.INTEGER:
                return BasicType.INT_TYPE;
            case Types.SMALLINT:
            case Types.TINYINT:
                return BasicType.SHORT_TYPE;
            case Types.BIGINT:
                return BasicType.LONG_TYPE;
            case Types.FLOAT:
            case Types.REAL:
                return BasicType.FLOAT_TYPE;
            case Types.DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return new DecimalType(column.length(), column.scale().orElse(0));
            case Types.TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case Types.DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case Types.TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case Types.BOOLEAN:
            case Types.BIT:
                return BasicType.BOOLEAN_TYPE;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Don't support Postgres type '%s' yet, jdbcType:'%s'.",
                                column.typeName(), column.jdbcType()));
        }
    }

    public static SeaTunnelRowType convertFromTable(Table table) {

        List<Column> columns = table.columns();
        String[] fieldNames = columns.stream().map(Column::name).toArray(String[]::new);

        SeaTunnelDataType<?>[] fieldTypes =
                columns.stream()
                        .map(PostgresTypeUtils::convertFromColumn)
                        .toArray(SeaTunnelDataType[]::new);

        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }
}
