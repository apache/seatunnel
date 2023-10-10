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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

import java.math.BigDecimal;
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
        return null;
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType typeInfo) throws SQLException {
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
                        fields[fieldIndex] = rs.getObject(resultSetIndex, String.class);
                    }
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, Boolean.class);
                    break;
                case TINYINT:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, Byte.class);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, Short.class);
                    break;
                case INT:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, Integer.class);
                    break;
                case BIGINT:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, Long.class);
                    break;
                case FLOAT:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, Float.class);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, Double.class);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = rs.getObject(resultSetIndex, BigDecimal.class);
                    break;
                case DATE:
                    fields[fieldIndex] =
                            Optional.ofNullable(rs.getObject(resultSetIndex, Date.class))
                                    .map(e -> e.toLocalDate())
                                    .orElse(null);
                    break;
                case TIME:
                    fields[fieldIndex] =
                            Optional.ofNullable(rs.getObject(resultSetIndex, Time.class))
                                    .map(e -> e.toLocalTime())
                                    .orElse(null);
                    break;
                case TIMESTAMP:
                    fields[fieldIndex] =
                            Optional.ofNullable(rs.getObject(resultSetIndex, Timestamp.class))
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = rs.getBytes(resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }
}
