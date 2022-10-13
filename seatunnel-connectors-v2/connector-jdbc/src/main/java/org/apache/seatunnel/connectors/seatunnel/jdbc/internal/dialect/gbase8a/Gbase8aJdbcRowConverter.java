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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.gbase8a;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Gbase8aJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "Gbase8a";
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public SeaTunnelRow toInternal(ResultSet rs, ResultSetMetaData metaData, SeaTunnelRowType typeInfo) throws SQLException {
        List<Object> fields = new ArrayList<>(16);
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();

        for (int i = 1; i <= seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SqlType sqlType = seaTunnelDataTypes[i - 1].getSqlType();
            if (null == rs.getObject(i)) {
                seatunnelField = null;
            } else if (SqlType.BOOLEAN.equals(sqlType)) {
                seatunnelField = rs.getBoolean(i);
            } else if (SqlType.TINYINT.equals(sqlType)) {
                seatunnelField = rs.getByte(i);
            } else if (SqlType.SMALLINT.equals(sqlType)) {
                seatunnelField = rs.getShort(i);
            } else if (SqlType.INT.equals(sqlType)) {
                seatunnelField = rs.getInt(i);
            } else if (SqlType.BIGINT.equals(sqlType)) {
                seatunnelField = rs.getLong(i);
            } else if (SqlType.DECIMAL.equals(sqlType)) {
                Object value = rs.getObject(i);
                seatunnelField = value instanceof BigInteger ?
                    new BigDecimal((BigInteger) value, 0)
                    : value;
            } else if (SqlType.FLOAT.equals(sqlType)) {
                seatunnelField = rs.getFloat(i);
            } else if (SqlType.DOUBLE.equals(sqlType)) {
                seatunnelField = rs.getDouble(i);
            } else if (SqlType.STRING.equals(sqlType)) {
                seatunnelField = rs.getString(i);
            } else if (SqlType.TIME.equals(sqlType)) {
                seatunnelField = rs.getTime(i);
            } else if (SqlType.DATE.equals(sqlType)) {
                seatunnelField = rs.getDate(i);
            } else if (SqlType.TIMESTAMP.equals(sqlType)) {
                seatunnelField = rs.getTimestamp(i);
            } else if (SqlType.BYTES.equals(sqlType)) {
                seatunnelField = rs.getBytes(i);
            } else {
                throw new IllegalStateException("Unexpected value: " + sqlType);
            }

            fields.add(seatunnelField);
        }

        return new SeaTunnelRow(fields.toArray());
    }

}
