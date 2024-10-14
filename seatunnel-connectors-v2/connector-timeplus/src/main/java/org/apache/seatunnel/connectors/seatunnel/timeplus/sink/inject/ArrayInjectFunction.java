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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.inject;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.timeplus.exception.TimeplusConnectorException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.regex.Pattern;

public class ArrayInjectFunction implements ProtonFieldInjectFunction {

    private static final Pattern PATTERN = Pattern.compile("(array.*)");
    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        String sqlType;
        Object[] elements = (Object[]) value;
        String type = fieldType.substring(fieldType.indexOf("(") + 1, fieldType.indexOf(")"));
        switch (type) {
            case "string":
                sqlType = "string";
                elements = Arrays.copyOf(elements, elements.length, String[].class);
                break;
            case "int128":
                sqlType = "int128";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "uint128":
                sqlType = "uint128";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "int256":
                sqlType = "int256";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "uint256":
                sqlType = "uint256";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "int8":
                sqlType = "int8";
                elements = Arrays.copyOf(elements, elements.length, Byte[].class);
                break;
            case "uint8":
                sqlType = "uint8";
                elements = Arrays.copyOf(elements, elements.length, Byte[].class);
                break;
            case "int16":
                sqlType = "int16";
                elements = Arrays.copyOf(elements, elements.length, Short[].class);
                break;
            case "uint16":
                sqlType = "uint16";
                elements = Arrays.copyOf(elements, elements.length, Integer[].class);
                break;
            case "int32":
                sqlType = "int32";
                elements = Arrays.copyOf(elements, elements.length, Integer[].class);
                break;
            case "uint32":
                sqlType = "uint32";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "int64":
                sqlType = "int64";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "uint64":
                sqlType = "uint64";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "float32":
                sqlType = "float32";
                elements = Arrays.copyOf(elements, elements.length, Float[].class);
                break;
            case "float64":
                sqlType = "float64";
                elements = Arrays.copyOf(elements, elements.length, Double[].class);
                break;
            case "bool":
                sqlType = "bool";
                elements = Arrays.copyOf(elements, elements.length, Boolean[].class);
                break;
            default:
                throw new TimeplusConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "array inject error, unsupported data type: " + type);
        }
        statement.setArray(index, statement.getConnection().createArrayOf(sqlType, elements));
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        if (PATTERN.matcher(fieldType).matches()) {
            this.fieldType = fieldType;
            return true;
        }
        return false;
    }
}
