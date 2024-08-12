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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.common.exception.CommonError;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StringInjectFunction implements ProtonFieldInjectFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value)
            throws SQLException {
        try {
            if ("point".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[].class));
            } else if ("ring".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[][].class));
            } else if ("polygon".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[][][].class));
            } else if ("multi_polygon".equals(fieldType)) {
                statement.setObject(
                        index, MAPPER.readValue(replace(value.toString()), double[][][][].class));
            } else {
                statement.setString(index, value.toString());
            }
        } catch (JsonProcessingException e) {
            throw CommonError.jsonOperationError("Timeplus", value.toString(), e);
        }
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        if ("string".equals(fieldType)
                || "int128".equals(fieldType)
                || "uint128".equals(fieldType)
                || "int256".equals(fieldType)
                || "uint256".equals(fieldType)
                || "point".equals(fieldType)
                || "ring".equals(fieldType)
                || "polygon".equals(fieldType)
                || "multi_polygon".equals(fieldType)) {
            this.fieldType = fieldType;
            return true;
        }
        return false;
    }

    private static String replace(String str) {
        return str.replaceAll("\\(", "[").replaceAll("\\)", "]");
    }
}
