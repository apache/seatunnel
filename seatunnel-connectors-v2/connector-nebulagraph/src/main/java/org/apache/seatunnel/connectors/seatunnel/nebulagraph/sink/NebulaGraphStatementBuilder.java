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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.sink;

import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphWriteMode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class NebulaGraphStatementBuilder {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final String quotedTag;
    private final List<String> propertyNames;
    private final NebulaGraphWriteMode writeMode;

    NebulaGraphStatementBuilder(
            String tag, List<String> propertyNames, NebulaGraphWriteMode writeMode) {
        this.quotedTag = quoteIdentifier(tag);
        this.propertyNames =
                propertyNames.stream()
                        .map(NebulaGraphStatementBuilder::validateIdentifier)
                        .collect(Collectors.toList());
        this.writeMode = writeMode;
    }

    NebulaGraphWriteRequest build(List<NebulaGraphVertex> vertices) {
        if (vertices.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot build a NebulaGraph request for an empty batch.");
        }
        if (writeMode == NebulaGraphWriteMode.INSERT) {
            return buildInsert(vertices);
        }
        return buildUpdate(vertices);
    }

    private NebulaGraphWriteRequest buildInsert(List<NebulaGraphVertex> vertices) {
        Map<String, Object> parameters = new LinkedHashMap<>();
        List<String> values = new ArrayList<>(vertices.size());
        for (int row = 0; row < vertices.size(); row++) {
            NebulaGraphVertex vertex = vertices.get(row);

            List<String> propertyParameters = new ArrayList<>(propertyNames.size());
            for (int field = 0; field < propertyNames.size(); field++) {
                String parameter = parameterName(row, field);
                parameters.put(parameter, vertex.getProperties().get(propertyNames.get(field)));
                propertyParameters.add("$" + parameter);
            }
            values.add(
                    formatVid(vertex.getVid()) + ":(" + String.join(",", propertyParameters) + ")");
        }

        String properties =
                propertyNames.stream()
                        .map(NebulaGraphStatementBuilder::quoteIdentifier)
                        .collect(Collectors.joining(","));
        String statement =
                "INSERT VERTEX IF NOT EXISTS "
                        + quotedTag
                        + " ("
                        + properties
                        + ") VALUES "
                        + String.join(",", values);
        return new NebulaGraphWriteRequest(statement, parameters);
    }

    private NebulaGraphWriteRequest buildUpdate(List<NebulaGraphVertex> vertices) {
        Map<String, Object> parameters = new LinkedHashMap<>();
        List<String> statements = new ArrayList<>(vertices.size());
        for (int row = 0; row < vertices.size(); row++) {
            NebulaGraphVertex vertex = vertices.get(row);

            List<String> assignments = new ArrayList<>(propertyNames.size());
            for (int field = 0; field < propertyNames.size(); field++) {
                String property = propertyNames.get(field);
                String parameter = parameterName(row, field);
                parameters.put(parameter, vertex.getProperties().get(property));
                assignments.add(quoteIdentifier(property) + "=$" + parameter);
            }
            statements.add(
                    "UPDATE VERTEX ON "
                            + quotedTag
                            + " "
                            + formatVid(vertex.getVid())
                            + " SET "
                            + String.join(",", assignments));
        }
        return new NebulaGraphWriteRequest(String.join(";", statements), parameters);
    }

    private static String parameterName(int row, int field) {
        return "value_" + row + "_" + field;
    }

    private static String formatVid(Object vid) {
        if (vid instanceof Number) {
            return Long.toString(((Number) vid).longValue());
        }
        String value = (String) vid;
        StringBuilder escaped = new StringBuilder(value.length() + 2);
        escaped.append('"');
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            switch (character) {
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                default:
                    if (Character.isISOControl(character)) {
                        throw new IllegalArgumentException(
                                "NebulaGraph string vertex IDs must not contain unsupported control characters.");
                    }
                    escaped.append(character);
            }
        }
        return escaped.append('"').toString();
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + validateIdentifier(identifier) + "`";
    }

    private static String validateIdentifier(String identifier) {
        if (identifier == null || !IDENTIFIER.matcher(identifier).matches()) {
            throw new NebulaGraphConnectorException(
                    NebulaGraphConnectorErrorCode.INVALID_CONFIG,
                    "NebulaGraph tag and property names must contain only letters, digits, or underscores and must not start with a digit: "
                            + identifier);
        }
        return identifier;
    }
}
