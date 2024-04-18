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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.Optional;

public class PostgresJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String PG_GEOMETRY = "GEOMETRY";
    private static final String PG_GEOGRAPHY = "GEOGRAPHY";

    @Override
    public String converterName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            String metaDataColumnType =
                    rs.getMetaData().getColumnTypeName(resultSetIndex).toUpperCase(Locale.ROOT);
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    if (metaDataColumnType.equals(PG_GEOMETRY)
                            || metaDataColumnType.equals(PG_GEOGRAPHY)) {
                        fields[fieldIndex] =
                                rs.getObject(resultSetIndex) == null
                                        ? null
                                        : rs.getObject(resultSetIndex).toString();
                    } else {
                        fields[fieldIndex] = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
                    }
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBoolean(rs, resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getByte(rs, resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getShort(rs, resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getInt(rs, resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getLong(rs, resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getFloat(rs, resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getDouble(rs, resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBigDecimal(rs, resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = JdbcFieldTypeUtils.getDate(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = JdbcFieldTypeUtils.getTime(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBytes(rs, resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case ARRAY:
                    Array jdbcArray = rs.getArray(resultSetIndex);
                    if (jdbcArray == null) {
                        fields[fieldIndex] = null;
                        break;
                    }

                    Object arrayObject = jdbcArray.getArray();
                    if (((ArrayType) seaTunnelDataType)
                            .getTypeClass()
                            .equals(arrayObject.getClass())) {
                        fields[fieldIndex] = arrayObject;
                    } else {
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unexpected value: " + seaTunnelDataType.getTypeClass());
                    }
                    break;
                case MAP:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }
}
