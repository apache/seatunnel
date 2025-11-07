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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;

public enum OceanBaseMysqlType implements SQLType {
    DECIMAL(
            "DECIMAL",
            Types.DECIMAL,
            BigDecimal.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            65L,
            "[(M[,D])] [UNSIGNED] [ZEROFILL]"),

    DECIMAL_UNSIGNED(
            "DECIMAL UNSIGNED",
            Types.DECIMAL,
            BigDecimal.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            65L,
            "[(M[,D])] [UNSIGNED] [ZEROFILL]"),

    TINYINT(
            "TINYINT",
            Types.TINYINT,
            Integer.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            3L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    TINYINT_UNSIGNED(
            "TINYINT UNSIGNED",
            Types.TINYINT,
            Integer.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            3L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 3L, ""),

    SMALLINT(
            "SMALLINT",
            Types.SMALLINT,
            Integer.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            5L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    SMALLINT_UNSIGNED(
            "SMALLINT UNSIGNED",
            Types.SMALLINT,
            Integer.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            5L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    INT(
            "INT",
            Types.INTEGER,
            Integer.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            10L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    INT_UNSIGNED(
            "INT UNSIGNED",
            Types.INTEGER,
            Long.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            10L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    FLOAT(
            "FLOAT",
            Types.REAL,
            Float.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            12L,
            "[(M,D)] [UNSIGNED] [ZEROFILL]"),

    FLOAT_UNSIGNED(
            "FLOAT UNSIGNED",
            Types.REAL,
            Float.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            12L,
            "[(M,D)] [UNSIGNED] [ZEROFILL]"),

    DOUBLE(
            "DOUBLE",
            Types.DOUBLE,
            Double.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            22L,
            "[(M,D)] [UNSIGNED] [ZEROFILL]"),

    DOUBLE_UNSIGNED(
            "DOUBLE UNSIGNED",
            Types.DOUBLE,
            Double.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            22L,
            "[(M,D)] [UNSIGNED] [ZEROFILL]"),
    /** FIELD_TYPE_NULL = 6 */
    NULL("NULL", Types.NULL, Object.class, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 0L, ""),

    TIMESTAMP(
            "TIMESTAMP",
            Types.TIMESTAMP,
            Timestamp.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            26L,
            "[(fsp)]"),

    BIGINT(
            "BIGINT",
            Types.BIGINT,
            Long.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            19L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    BIGINT_UNSIGNED(
            "BIGINT UNSIGNED",
            Types.BIGINT,
            BigInteger.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            20L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    MEDIUMINT(
            "MEDIUMINT",
            Types.INTEGER,
            Integer.class,
            OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            7L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    MEDIUMINT_UNSIGNED(
            "MEDIUMINT UNSIGNED",
            Types.INTEGER,
            Integer.class,
            OceanBaseMysqlType.FIELD_FLAG_UNSIGNED | OceanBaseMysqlType.FIELD_FLAG_ZEROFILL,
            OceanBaseMysqlType.IS_DECIMAL,
            8L,
            "[(M)] [UNSIGNED] [ZEROFILL]"),

    DATE("DATE", Types.DATE, Date.class, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 10L, ""),

    TIME("TIME", Types.TIME, Time.class, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 16L, "[(fsp)]"),

    DATETIME(
            "DATETIME",
            Types.TIMESTAMP,
            LocalDateTime.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            26L,
            "[(fsp)]"),

    YEAR("YEAR", Types.DATE, Date.class, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 4L, "[(4)]"),

    VARCHAR(
            "VARCHAR",
            Types.VARCHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            65535L,
            "(M) [CHARACTER SET charset_name] [COLLATE collation_name]"),

    VARBINARY(
            "VARBINARY",
            Types.VARBINARY,
            null,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            65535L,
            "(M)"),

    BIT("BIT", Types.BIT, Boolean.class, 0, OceanBaseMysqlType.IS_DECIMAL, 1L, "[(M)]"),

    JSON(
            "JSON",
            Types.LONGVARCHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            1073741824L,
            ""),

    ENUM(
            "ENUM",
            Types.CHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            65535L,
            "('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]"),

    SET(
            "SET",
            Types.CHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            64L,
            "('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]"),

    TINYBLOB("TINYBLOB", Types.VARBINARY, null, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 255L, ""),

    TINYTEXT(
            "TINYTEXT",
            Types.VARCHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            255L,
            " [CHARACTER SET charset_name] [COLLATE collation_name]"),

    MEDIUMBLOB(
            "MEDIUMBLOB",
            Types.LONGVARBINARY,
            null,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            16777215L,
            ""),

    MEDIUMTEXT(
            "MEDIUMTEXT",
            Types.LONGVARCHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            16777215L,
            " [CHARACTER SET charset_name] [COLLATE collation_name]"),

    LONGBLOB(
            "LONGBLOB",
            Types.LONGVARBINARY,
            null,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            4294967295L,
            ""),

    LONGTEXT(
            "LONGTEXT",
            Types.LONGVARCHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            4294967295L,
            " [CHARACTER SET charset_name] [COLLATE collation_name]"),

    BLOB("BLOB", Types.LONGVARBINARY, null, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 65535L, "[(M)]"),

    TEXT(
            "TEXT",
            Types.LONGVARCHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            65535L,
            "[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]"),

    CHAR(
            "CHAR",
            Types.CHAR,
            String.class,
            0,
            OceanBaseMysqlType.IS_NOT_DECIMAL,
            255L,
            "[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]"),

    BINARY("BINARY", Types.BINARY, null, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 255L, "(M)"),

    GEOMETRY("GEOMETRY", Types.BINARY, null, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 65535L, ""),
    // is represented by BLOB
    UNKNOWN("UNKNOWN", Types.OTHER, null, 0, OceanBaseMysqlType.IS_NOT_DECIMAL, 65535L, "");

    private final String name;
    protected int jdbcType;
    protected final Class<?> javaClass;
    private final int flagsMask;
    private final boolean isDecimal;
    private final Long precision;
    private final String createParams;

    private OceanBaseMysqlType(
            String oceanBaseMysqlTypeName,
            int jdbcType,
            Class<?> javaClass,
            int allowedFlags,
            boolean isDec,
            Long precision,
            String createParams) {
        this.name = oceanBaseMysqlTypeName;
        this.jdbcType = jdbcType;
        this.javaClass = javaClass;
        this.flagsMask = allowedFlags;
        this.isDecimal = isDec;
        this.precision = precision;
        this.createParams = createParams;
    }

    public static final int FIELD_FLAG_UNSIGNED = 32;
    public static final int FIELD_FLAG_ZEROFILL = 64;

    private static final boolean IS_DECIMAL = true;
    private static final boolean IS_NOT_DECIMAL = false;

    public static OceanBaseMysqlType getByName(String fullMysqlTypeName) {

        String typeName = "";

        if (fullMysqlTypeName.indexOf("(") != -1) {
            typeName = fullMysqlTypeName.substring(0, fullMysqlTypeName.indexOf("(")).trim();
        } else {
            typeName = fullMysqlTypeName;
        }

        // the order of checks is important because some short names could match parts of longer
        // names
        if (StringUtils.indexOfIgnoreCase(typeName, "DECIMAL") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "DEC") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "NUMERIC") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "FIXED") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                    ? DECIMAL_UNSIGNED
                    : DECIMAL;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "TINYBLOB") != -1) {
            // IMPORTANT: "TINYBLOB" must be checked before "TINY"
            return TINYBLOB;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "TINYTEXT") != -1) {
            // IMPORTANT: "TINYTEXT" must be checked before "TINY"
            return TINYTEXT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "TINYINT") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "TINY") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "INT1") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                            || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ZEROFILL") != -1
                    ? TINYINT_UNSIGNED
                    : TINYINT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "MEDIUMINT") != -1
                // IMPORTANT: "INT24" must be checked before "INT2"
                || StringUtils.indexOfIgnoreCase(typeName, "INT24") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "INT3") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "MIDDLEINT") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                            || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ZEROFILL") != -1
                    ? MEDIUMINT_UNSIGNED
                    : MEDIUMINT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "SMALLINT") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "INT2") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                            || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ZEROFILL") != -1
                    ? SMALLINT_UNSIGNED
                    : SMALLINT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "BIGINT") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "SERIAL") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "INT8") != -1) {
            // SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                            || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ZEROFILL") != -1
                    ? BIGINT_UNSIGNED
                    : BIGINT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "POINT") != -1) {
            // also covers "MULTIPOINT"
            // IMPORTANT: "POINT" must be checked before "INT"
        } else if (StringUtils.indexOfIgnoreCase(typeName, "INT") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "INTEGER") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "INT4") != -1) {
            // IMPORTANT: "INT" must be checked after all "*INT*" types
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                            || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ZEROFILL") != -1
                    ? INT_UNSIGNED
                    : INT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "DOUBLE") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "REAL") != -1
                /* || StringUtils.indexOfIgnoreCase(name, "DOUBLE PRECISION") != -1 is caught by "DOUBLE" check */
                // IMPORTANT: "FLOAT8" must be checked before "FLOAT"
                || StringUtils.indexOfIgnoreCase(typeName, "FLOAT8") != -1) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                            || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ZEROFILL") != -1
                    ? DOUBLE_UNSIGNED
                    : DOUBLE;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "FLOAT") != -1 /*
         * || StringUtils.indexOfIgnoreCase(name, "FLOAT4") != -1 is caught by
         * "FLOAT" check
         */) {
            return StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "UNSIGNED") != -1
                            || StringUtils.indexOfIgnoreCase(fullMysqlTypeName, "ZEROFILL") != -1
                    ? FLOAT_UNSIGNED
                    : FLOAT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "NULL") != -1) {
            return NULL;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "TIMESTAMP") != -1) {
            // IMPORTANT: "TIMESTAMP" must be checked before "TIME"
            return TIMESTAMP;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "DATETIME") != -1) {
            // IMPORTANT: "DATETIME" must be checked before "DATE" and "TIME"
            return DATETIME;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "DATE") != -1) {
            return DATE;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "TIME") != -1) {
            return TIME;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "YEAR") != -1) {
            return YEAR;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "LONGBLOB") != -1) {
            // IMPORTANT: "LONGBLOB" must be checked before "LONG" and "BLOB"
            return LONGBLOB;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "LONGTEXT") != -1) {
            // IMPORTANT: "LONGTEXT" must be checked before "LONG" and "TEXT"
            return LONGTEXT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "MEDIUMBLOB") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "LONG VARBINARY") != -1) {
            // IMPORTANT: "MEDIUMBLOB" must be checked before "BLOB"
            // IMPORTANT: "LONG VARBINARY" must be checked before "LONG" and "VARBINARY"
            return MEDIUMBLOB;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "MEDIUMTEXT") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "LONG VARCHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "LONG") != -1) {
            // IMPORTANT: "MEDIUMTEXT" must be checked before "TEXT"
            // IMPORTANT: "LONG VARCHAR" must be checked before "VARCHAR"
            return MEDIUMTEXT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "VARCHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "NVARCHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "NATIONAL VARCHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "CHARACTER VARYING") != -1) {
            // IMPORTANT: "CHARACTER VARYING" must be checked before "CHARACTER" and "CHAR"
            return VARCHAR;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "VARBINARY") != -1) {
            return VARBINARY;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "BINARY") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "CHAR BYTE") != -1) {
            // IMPORTANT: "BINARY" must be checked after all "*BINARY" types
            // IMPORTANT: "CHAR BYTE" must be checked before "CHAR"
            return BINARY;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "LINESTRING") != -1) {
            // also covers "MULTILINESTRING"
            // IMPORTANT: "LINESTRING" must be checked before "STRING"
            return GEOMETRY;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "STRING") != -1
                // IMPORTANT: "CHAR" must be checked after all "*CHAR*" types
                || StringUtils.indexOfIgnoreCase(typeName, "CHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "NCHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "NATIONAL CHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "CHARACTER") != -1) {
            return CHAR;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "BOOLEAN") != -1
                || StringUtils.indexOfIgnoreCase(typeName, "BOOL") != -1) {
            return BOOLEAN;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "BIT") != -1) {
            return BIT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "JSON") != -1) {
            return JSON;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "ENUM") != -1) {
            return ENUM;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "SET") != -1) {
            return SET;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "BLOB") != -1) {
            return BLOB;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "TEXT") != -1) {
            return TEXT;

        } else if (StringUtils.indexOfIgnoreCase(typeName, "GEOM")
                        != -1 // covers "GEOMETRY", "GEOMETRYCOLLECTION" and "GEOMCOLLECTION"
                || StringUtils.indexOfIgnoreCase(typeName, "POINT")
                        != -1 // also covers "MULTIPOINT"
                || StringUtils.indexOfIgnoreCase(typeName, "POLYGON")
                        != -1 // also covers "MULTIPOLYGON"
        ) {
            return GEOMETRY;
        }

        return UNKNOWN;
    }

    @Override
    public String getVendor() {
        return "com.oceanbase";
    }

    @Override
    public Integer getVendorTypeNumber() {
        return this.jdbcType;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
