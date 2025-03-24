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
package org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class StarRocksType {
    public static final String SR_NULL = "NULL";
    public static final String SR_BOOLEAN = "BOOLEAN";
    public static final String SR_TINYINT = "TINYINT";
    public static final String SR_SMALLINT = "SMALLINT";
    public static final String SR_INT = "INT";
    public static final String SR_BIGINT = "BIGINT";
    public static final String SR_LARGEINT = "LARGEINT";
    public static final String SR_FLOAT = "FLOAT";
    public static final String SR_DOUBLE = "DOUBLE";
    public static final String SR_DECIMAL = "DECIMAL";
    public static final String SR_DECIMALV3 = "DECIMALV3";
    public static final String SR_DATE = "DATE";
    public static final String SR_DATETIME = "DATETIME";
    public static final String SR_CHAR = "CHAR";
    public static final String SR_VARCHAR = "VARCHAR";
    public static final String SR_STRING = "STRING";

    public static final String SR_BOOLEAN_ARRAY = "ARRAY<boolean>";
    public static final String SR_TINYINT_ARRAY = "ARRAY<tinyint>";
    public static final String SR_SMALLINT_ARRAY = "ARRAY<smallint>";
    public static final String SR_INT_ARRAY = "ARRAY<int(11)>";
    public static final String SR_BIGINT_ARRAY = "ARRAY<bigint>";
    public static final String SR_FLOAT_ARRAY = "ARRAY<float>";
    public static final String SR_DOUBLE_ARRAY = "ARRAY<double>";
    public static final String SR_DECIMALV3_ARRAY = "ARRAY<DECIMALV3>";
    public static final String SR_DECIMALV3_ARRAY_COLUMN_TYPE_TMP = "ARRAY<DECIMALV3(%s, %s)>";
    public static final String SR_DATEV2_ARRAY = "ARRAY<DATEV2>";
    public static final String SR_DATETIMEV2_ARRAY = "ARRAY<DATETIMEV2>";
    public static final String SR_STRING_ARRAY = "ARRAY<STRING>";

    // Because can not get the column length from array, So the following types of arrays cannot be
    // generated properly.
    public static final String SR_LARGEINT_ARRAY = "ARRAY<largeint>";
    public static final String SR_CHAR_ARRAY = "ARRAY<CHAR>";
    public static final String SR_CHAR_ARRAY_COLUMN_TYPE_TMP = "ARRAY<CHAR(%s)>";
    public static final String SR_VARCHAR_ARRAY = "ARRAY<VARCHAR>";
    public static final String SR_VARCHAR_ARRAY_COLUMN_TYPE_TMP = "ARRAY<VARCHAR(%s)>";

    public static final String SR_JSON = "JSON";
    public static final String SR_JSONB = "JSONB";

    public static final String SR_ARRAY = "ARRAY";

    public static final String SR_ARRAY_BOOLEAN_INTER = "tinyint(1)";
    public static final String SR_ARRAY_TINYINT_INTER = "tinyint(4)";
    public static final String SR_ARRAY_SMALLINT_INTER = "smallint(6)";
    public static final String SR_ARRAY_INT_INTER = "int(11)";
    public static final String SR_ARRAY_BIGINT_INTER = "bigint(20)";
    public static final String SR_ARRAY_DECIMAL_PRE = "DECIMAL";
    public static final String SR_ARRAY_DATE_INTER = "date";
    public static final String SR_ARRAY_DATEV2_INTER = "DATEV2";
    public static final String SR_ARRAY_DATETIME_INTER = "DATETIME";
    public static final String SR_ARRAY_DATETIMEV2_INTER = "DATETIMEV2";

    public static final String SR_MAP = "MAP";
    public static final String SR_MAP_COLUMN_TYPE = "MAP<%s, %s>";

    public static final String SR_BOOLEAN_INDENTFIER = "TINYINT(1)";

    private String type;
}
