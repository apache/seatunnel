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
package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.shade.com.google.common.io.ByteStreams;

import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Formatter;

public final class JdbcUtils {

    private JdbcUtils() {}

    public static String getString(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getString(columnIndex);
    }

    public static Boolean getBoolean(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getBoolean(columnIndex);
    }

    public static Byte getByte(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getByte(columnIndex);
    }

    public static Short getShort(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getShort(columnIndex);
    }

    public static Integer getInt(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getInt(columnIndex);
    }

    public static Long getLong(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getLong(columnIndex);
    }

    public static Float getFloat(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getFloat(columnIndex);
    }

    public static Double getDouble(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getDouble(columnIndex);
    }

    public static BigDecimal getBigDecimal(ResultSet resultSet, int columnIndex)
            throws SQLException {
        return resultSet.getBigDecimal(columnIndex);
    }

    public static Date getDate(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getDate(columnIndex);
    }

    public static Time getTime(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getTime(columnIndex);
    }

    public static Timestamp getTimestamp(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getTimestamp(columnIndex);
    }

    public static byte[] getBytes(ResultSet resultSet, int columnIndex) throws SQLException {
        if (null == resultSet.getObject(columnIndex)) {
            return null;
        }
        return resultSet.getBytes(columnIndex);
    }

    /** Support to LOG for debug. */
    public static void formatResultSet(ResultSet resultSet, Appendable appendable)
            throws SQLException, IOException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        StringBuilder formatBuilder = new StringBuilder();
        Formatter formatter = new Formatter(appendable);
        Object[] headers = new String[columnCount];
        formatBuilder.append("|");
        for (int col = 1; col <= columnCount; col++) {
            formatBuilder.append("%-8.8s").append("|");
            headers[col - 1] = resultSetMetaData.getColumnName(col);
        }
        String format = formatBuilder.toString();
        formatter.format(format, headers);
        if (resultSetMetaData.getColumnCount() > 0) {
            while (resultSet.next()) {
                Object[] row = new Object[columnCount];
                for (int col = 1; col <= columnCount; col++) {
                    Object obj = resultSet.getObject(col);
                    if (obj == null) {
                        row[col - 1] = "\\N";
                    } else {
                        if (obj instanceof Blob) {
                            row[col - 1] =
                                    Base64.encodeBase64String(
                                            ByteStreams.toByteArray(
                                                    ((Blob) obj).getBinaryStream()));
                        } else {
                            row[col - 1] = obj;
                        }
                    }
                }
                appendable.append("\n");
                formatter.format(format, row);
            }
        } else {
            appendable.append("\n").append("<Not Row>");
        }
    }
}
