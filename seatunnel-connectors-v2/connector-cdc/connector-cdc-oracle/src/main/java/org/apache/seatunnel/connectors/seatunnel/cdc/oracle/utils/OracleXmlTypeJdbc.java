/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import io.debezium.relational.Column;
import oracle.jdbc.OracleTypes;

import java.lang.reflect.Method;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.Locale;

/**
 * JDBC helpers for Oracle {@code XMLTYPE}. Snapshot and re-select require {@code xdb} and {@code
 * xmlparserv2} next to {@code ojdbc8}, the same jars the Oracle JDBC driver uses for {@code
 * XMLType}.
 */
public final class OracleXmlTypeJdbc {

    private OracleXmlTypeJdbc() {}

    public static boolean isXmlType(Column column) {
        if (column == null) {
            return false;
        }
        return isXmlType(column.jdbcType(), column.typeName());
    }

    public static boolean isXmlType(int jdbcType, String typeName) {
        if (jdbcType == OracleTypes.SQLXML || jdbcType == Types.SQLXML) {
            return true;
        }
        return isXmlTypeName(typeName);
    }

    public static boolean isXmlTypeName(String typeName) {
        if (typeName == null) {
            return false;
        }
        String upper = typeName.toUpperCase(Locale.ROOT);
        int parenthesis = upper.indexOf('(');
        if (parenthesis >= 0) {
            upper = upper.substring(0, parenthesis).trim();
        }
        return "XMLTYPE".equals(upper) || upper.endsWith(".XMLTYPE");
    }

    public static boolean isXmlTypeValue(Object value) {
        return value != null && value.getClass().getName().endsWith("XMLType");
    }

    /**
     * Returns a {@link SQLXML} locator, a document {@link String}, or {@code null}. Callers that
     * need text should use {@link #asString(Object)}.
     */
    public static Object read(ResultSet resultSet, int columnIndex) throws SQLException {
        try {
            SQLXML xml = resultSet.getSQLXML(columnIndex);
            if (resultSet.wasNull()) {
                return null;
            }
            if (xml != null) {
                return xml;
            }
        } catch (SQLException | RuntimeException | LinkageError ignored) {
            // Fall through to getString / XMLType.
        }
        try {
            String text = resultSet.getString(columnIndex);
            if (text == null || resultSet.wasNull()) {
                return null;
            }
            return text;
        } catch (SQLException | RuntimeException | LinkageError stringFailed) {
            Object value = resultSet.getObject(columnIndex);
            if (resultSet.wasNull() || value == null) {
                return null;
            }
            if (isXmlTypeValue(value)) {
                return value;
            }
            throw new SQLException(
                    "Failed to read Oracle XMLTYPE at column " + columnIndex, stringFailed);
        }
    }

    public static String asString(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            long length = clob.length();
            if (length > Integer.MAX_VALUE) {
                throw new SQLException(
                        "CLOB length " + length + " exceeds the supported 2GB limit");
            }
            return clob.getSubString(1, (int) length);
        }
        if (value instanceof SQLXML) {
            SQLXML xml = (SQLXML) value;
            try {
                return xml.getString();
            } finally {
                try {
                    xml.free();
                } catch (SQLException ignored) {
                    // The caller already holds the document text.
                }
            }
        }
        if (isXmlTypeValue(value)) {
            return xmlTypeString(value);
        }
        return value.toString();
    }

    /**
     * {@code oracle.xdb.XMLType} exposes {@code getStringVal()} and does not always implement
     * {@link SQLXML}.
     */
    public static String xmlTypeString(Object value) throws SQLException {
        if (!isXmlTypeValue(value)) {
            return null;
        }
        try {
            Method getStringVal = value.getClass().getMethod("getStringVal");
            Object text = getStringVal.invoke(value);
            return text == null ? null : text.toString();
        } catch (ReflectiveOperationException e) {
            throw new SQLException(
                    "Failed to read Oracle XMLType value from " + value.getClass().getName(), e);
        }
    }
}
