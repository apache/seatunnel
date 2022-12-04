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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.regex.Pattern;

public class ArrayInjectFunction implements ClickhouseFieldInjectFunction {

    private static final Pattern PATTERN = Pattern.compile("(Array.*)");
    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value) throws SQLException {
        String sqlType;
        Object[] elements = (Object[]) value;
        String type = fieldType.substring(fieldType.indexOf("(") + 1, fieldType.indexOf(")"));
        switch (type) {
            case "String":
            case "Int128":
            case "UInt128":
            case "Int256":
            case "UInt256":
                sqlType = "TEXT";
                elements = Arrays.copyOf(elements, elements.length, String[].class);
                break;
            case "Int8":
                sqlType = "TINYINT";
                elements = Arrays.copyOf(elements, elements.length, Byte[].class);
                break;
            case "UInt8":
            case "Int16":
                sqlType = "SMALLINT";
                elements = Arrays.copyOf(elements, elements.length, Short[].class);
                break;
            case "UInt16":
            case "Int32":
                sqlType = "INTEGER";
                elements = Arrays.copyOf(elements, elements.length, Integer[].class);
                break;
            case "UInt32":
            case "Int64":
            case "UInt64":
                sqlType = "BIGINT";
                elements = Arrays.copyOf(elements, elements.length, Long[].class);
                break;
            case "Float32":
                sqlType = "REAL";
                elements = Arrays.copyOf(elements, elements.length, Float[].class);
                break;
            case "Float64":
                sqlType = "DOUBLE";
                elements = Arrays.copyOf(elements, elements.length, Double[].class);
                break;
            case "Bool":
                sqlType = "BOOLEAN";
                elements = Arrays.copyOf(elements, elements.length, Boolean[].class);
                break;
            default:
                throw new ClickhouseConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "array inject error, unsupported data type: " + type);
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
