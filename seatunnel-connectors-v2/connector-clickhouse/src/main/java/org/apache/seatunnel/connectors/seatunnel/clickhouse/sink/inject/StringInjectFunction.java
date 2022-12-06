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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StringInjectFunction implements ClickhouseFieldInjectFunction {

    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value) throws SQLException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            if ("Point".equals(fieldType)) {
                statement.setObject(index, mapper.readValue(replace(value.toString()), double[].class));
            } else if ("Ring".equals(fieldType)) {
                statement.setObject(index, mapper.readValue(replace(value.toString()), double[][].class));
            } else if ("Polygon".equals(fieldType)) {
                statement.setObject(index, mapper.readValue(replace(value.toString()), double[][][].class));
            } else if ("MultiPolygon".equals(fieldType)) {
                statement.setObject(index, mapper.readValue(replace(value.toString()), double[][][][].class));
            } else {
                statement.setString(index, value.toString());
            }
        } catch (JsonProcessingException e) {
            throw new ClickhouseConnectorException(CommonErrorCode.JSON_OPERATION_FAILED, e.getMessage());
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        if ("String".equals(fieldType)
            || "Int128".equals(fieldType)
            || "UInt128".equals(fieldType)
            || "Int256".equals(fieldType)
            || "UInt256".equals(fieldType)
            || "Point".equals(fieldType)
            || "Ring".equals(fieldType)
            || "Polygon".equals(fieldType)
            || "MultiPolygon".equals(fieldType)) {
            this.fieldType = fieldType;
            return true;
        }
        return false;
    }

    private static String replace(String str) {
        return str.replaceAll("\\(", "[").replaceAll("\\)", "]");
    }
}
