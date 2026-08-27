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

package org.apache.seatunnel.connectors.sensorsdata.format;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import lombok.Getter;

public class SensorsDataTypes {
    public enum DataTypes {
        UNKNOWN,
        BOOLEAN,
        DECIMAL,
        INT,
        BIGINT,
        FLOAT,
        DOUBLE,
        NUMBER,
        STRING,
        DATE,
        TIMESTAMP,
        LIST,
        LIST_COMMA,
        LIST_SEMICOLON;

        public static DataTypes of(String s) {
            String str = StringUtils.upperCase(StringUtils.trim(s));
            if (StringUtils.isBlank(str)) {
                return DataTypes.UNKNOWN;
            }
            if (StringUtils.startsWith(str, "TIMESTAMP")) {
                // TIMESTAMP include timezone, see
                // org.apache.seatunnel.format.sensorsdata.utils.TypeUtilTest
                return DataTypes.TIMESTAMP;
            }
            switch (str) {
                case "BOOLEAN":
                    return DataTypes.BOOLEAN;
                case "DECIMAL":
                    return DataTypes.DECIMAL;
                case "INT":
                    return DataTypes.INT;
                case "BIGINT":
                case "LONG":
                    return DataTypes.BIGINT;
                case "FLOAT":
                    return DataTypes.FLOAT;
                case "DOUBLE":
                    return DataTypes.DOUBLE;
                case "NUMBER":
                    return DataTypes.NUMBER;
                case "LIST":
                    return DataTypes.LIST;
                case "LIST_COMMA":
                    return DataTypes.LIST_COMMA;
                case "LIST_SEMICOLON":
                    return DataTypes.LIST_SEMICOLON;
                case "DATE":
                    return DataTypes.DATE;
                case "STRING":
                    return DataTypes.STRING;
                default:
                    return DataTypes.UNKNOWN;
            }
        }
    }

    @Getter private final DataTypes type;
    @Getter private final String extra;

    SensorsDataTypes(DataTypes type, String extra) {
        this.type = type;
        this.extra = extra;
    }

    public static SensorsDataTypes of(String str) {
        DataTypes type = DataTypes.of(str);
        String suffix =
                StringUtils.length(str) > type.name().length()
                        ? StringUtils.trim(StringUtils.substring(str, type.name().length()))
                        : null;
        return new SensorsDataTypes(type, suffix);
    }
}
