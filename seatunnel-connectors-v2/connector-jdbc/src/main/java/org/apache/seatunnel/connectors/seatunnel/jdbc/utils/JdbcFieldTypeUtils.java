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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

public final class JdbcFieldTypeUtils {

    private JdbcFieldTypeUtils() {}

    public static Boolean getBoolean(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getBoolean);
    }

    public static Byte getByte(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getByte);
    }

    public static Short getShort(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getShort);
    }

    public static Integer getInt(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getInt);
    }

    public static Long getLong(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getLong);
    }

    public static Float getFloat(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getFloat);
    }

    public static Double getDouble(ResultSet resultSet, int columnIndex) throws SQLException {
        return getNullableValue(resultSet, columnIndex, ResultSet::getDouble);
    }

    public static String getString(ResultSet resultSet, int columnIndex) throws SQLException {
        return resultSet.getString(columnIndex);
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
        return resultSet.getBytes(columnIndex);
    }

    private static <T> T getNullableValue(
            ResultSet resultSet,
            int columnIndex,
            ThrowingFunction<ResultSet, T, SQLException> getter)
            throws SQLException {
        if (resultSet.getObject(columnIndex) == null) {
            return null;
        }
        return getter.apply(resultSet, columnIndex);
    }

    @FunctionalInterface
    private interface ThrowingFunction<T, R, E extends Exception> {
        R apply(T t, int columnIndex) throws E;
    }
}
