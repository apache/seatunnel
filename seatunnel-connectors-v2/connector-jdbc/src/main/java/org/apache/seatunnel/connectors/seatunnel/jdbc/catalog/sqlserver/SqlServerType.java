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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.sql.SQLType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public enum SqlServerType implements SQLType {
    UNKNOWN("unknown", 999, Object.class),
    TINYINT("tinyint", java.sql.Types.TINYINT, Short.class),
    BIT("bit", java.sql.Types.BIT, Boolean.class),
    SMALLINT("smallint", java.sql.Types.SMALLINT, Short.class),
    INTEGER("int", java.sql.Types.INTEGER, Integer.class),
    BIGINT("bigint", java.sql.Types.BIGINT, Long.class),
    FLOAT("float", java.sql.Types.DOUBLE, Double.class),
    REAL("real", java.sql.Types.REAL, Float.class),
    SMALLDATETIME("smalldatetime", microsoft.sql.Types.SMALLDATETIME, java.sql.Timestamp.class),
    DATETIME("datetime", microsoft.sql.Types.DATETIME, java.sql.Timestamp.class),
    DATE("date", java.sql.Types.DATE, java.sql.Date.class),
    TIME("time", java.sql.Types.TIME, java.sql.Time.class),
    DATETIME2("datetime2", java.sql.Types.TIMESTAMP, java.sql.Timestamp.class),
    DATETIMEOFFSET(
            "datetimeoffset",
            microsoft.sql.Types.DATETIMEOFFSET,
            microsoft.sql.DateTimeOffset.class),
    SMALLMONEY("smallmoney", microsoft.sql.Types.SMALLMONEY, BigDecimal.class),
    MONEY("money", microsoft.sql.Types.MONEY, BigDecimal.class),
    CHAR("char", java.sql.Types.CHAR, String.class),
    VARCHAR("varchar", java.sql.Types.VARCHAR, String.class),
    VARCHARMAX("varchar", java.sql.Types.LONGVARCHAR, String.class),
    TEXT("text", java.sql.Types.LONGVARCHAR, String.class),
    NCHAR("nchar", -15, String.class),
    NVARCHAR("nvarchar", -9, String.class),
    NVARCHARMAX("nvarchar", -16, String.class),
    NTEXT("ntext", -16, String.class),
    BINARY("binary", java.sql.Types.BINARY, byte[].class),
    VARBINARY("varbinary", java.sql.Types.VARBINARY, byte[].class),
    VARBINARYMAX("varbinary", java.sql.Types.LONGVARBINARY, byte[].class),
    IMAGE("image", java.sql.Types.LONGVARBINARY, byte[].class),
    DECIMAL("decimal", java.sql.Types.DECIMAL, BigDecimal.class, true, true),
    NUMERIC("numeric", java.sql.Types.NUMERIC, BigDecimal.class),
    GUID("uniqueidentifier", microsoft.sql.Types.GUID, String.class),
    SQL_VARIANT("sql_variant", microsoft.sql.Types.SQL_VARIANT, Object.class),
    UDT("udt", java.sql.Types.VARBINARY, byte[].class),
    XML("xml", -16, String.class),
    TIMESTAMP("timestamp", java.sql.Types.BINARY, byte[].class),
    GEOMETRY("geometry", microsoft.sql.Types.GEOMETRY, Object.class),
    GEOGRAPHY("geography", microsoft.sql.Types.GEOMETRY, Object.class);

    private static final String PRECISION = "precision";
    private static final String SCALE = "scale";
    private static final String LENGTH = "length";

    private final String name;
    private final int jdbcType;
    private final Class<?> javaClass;
    private final boolean isDecimal;
    private final boolean hasLength;

    SqlServerType(String sqlServerTypeName, int jdbcType, Class<?> javaClass) {
        this(sqlServerTypeName, jdbcType, javaClass, false, false);
    }

    SqlServerType(
            String sqlServerTypeName,
            int jdbcType,
            Class<?> javaClass,
            boolean isDec,
            boolean hasLength) {
        this.name = sqlServerTypeName;
        this.jdbcType = jdbcType;
        this.javaClass = javaClass;
        this.isDecimal = isDec;
        this.hasLength = hasLength;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getVendor() {
        return "com.microsoft.sqlserver.jdbc";
    }

    @Override
    public Integer getVendorTypeNumber() {
        return jdbcType;
    }

    public String getSqlTypeName(Map<String, Object> params) {
        if (isDecimal) {
            Object precision = params.get(PRECISION);
            Object scale = params.get(SCALE);
            return String.format("%s(%s, %s)", getName(), precision, scale);
        }
        if (hasLength) {
            Object length = params.get(LENGTH);
            return String.format("%s(%s)", getName(), length);
        }
        return getName();
    }

    public String getSqlTypeName() {
        return getSqlTypeName(Collections.emptyMap());
    }

    public String getSqlTypeName(long length) {
        return getSqlTypeName(Collections.singletonMap(LENGTH, length));
    }

    public String getSqlTypeName(long precision, long scale) {
        return getSqlTypeName(ImmutableMap.of(PRECISION, precision, SCALE, scale));
    }

    public static Pair<SqlServerType, Map<String, Object>> parse(String fullTypeName) {
        Map<String, Object> params = new HashMap<>();
        String typeName = fullTypeName;
        if (fullTypeName.indexOf("(") != -1) {
            typeName = fullTypeName.substring(0, fullTypeName.indexOf("(")).trim();
            String paramsStr =
                    fullTypeName.substring(
                            fullTypeName.indexOf("(") + 1, fullTypeName.indexOf(")"));
            if (DECIMAL.getName().equalsIgnoreCase(typeName)) {
                String[] precisionAndScale = paramsStr.split(",");
                params.put(PRECISION, precisionAndScale[0].trim());
                params.put(SCALE, precisionAndScale[1].trim());
            } else {
                params.put(LENGTH, paramsStr.trim());
            }
        }

        SqlServerType sqlServerType = null;
        for (SqlServerType type : SqlServerType.values()) {
            if (type.getName().equalsIgnoreCase(typeName)) {
                sqlServerType = type;
                break;
            }
        }
        Objects.requireNonNull(sqlServerType);
        return Pair.of(sqlServerType, params);
    }
}
