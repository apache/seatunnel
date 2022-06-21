/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for all converters that convert between JDBC object and Flink internal object.
 */
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {

    public abstract String converterName();

    public AbstractJdbcRowConverter() {
    }

    @Override
    @SuppressWarnings("checkstyle:Indentation")
    public SeaTunnelRow toInternal(ResultSet rs, ResultSetMetaData metaData, SeaTunnelRowType typeInfo) throws SQLException {

        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();

        for (int i = 1; i <= seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i - 1];
            if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBoolean(i);
            } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getByte(i);
            } else if (BasicType.SHORT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getShort(i);
            } else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getInt(i);
            } else if (BasicType.BIG_INT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getObject(i);
            } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getLong(i);
            } else if (BasicType.BIG_DECIMAL_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBigDecimal(i);
            } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getFloat(i);
            } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDouble(i);
            } else if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getString(i);
            } else if (BasicType.DATE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getObject(i);
            } else if (LocalTimeType.LOCAL_TIME_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getTime(i).toLocalTime();
            } else if (LocalTimeType.LOCAL_DATE_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getDate(i).toLocalDate();
            } else if (LocalTimeType.LOCAL_DATE_TIME_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getTimestamp(i).toLocalDateTime();
            } else if (PrimitiveArrayType.PRIMITIVE_BYTE_ARRAY_TYPE.equals(seaTunnelDataType)) {
                seatunnelField = rs.getBytes(i);
            } else {
                throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
            }

            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }
}
