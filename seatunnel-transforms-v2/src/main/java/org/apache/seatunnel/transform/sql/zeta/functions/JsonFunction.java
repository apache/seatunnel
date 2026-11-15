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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.transform.sql.zeta.ZetaSQLFunction;

import java.util.List;

/**
 * Built-in JSON functions for the Zeta SQL transform.
 *
 * <p>{@link #getJsonObject(List)} extracts a value from a JSON string using a JSON path. The
 * supported path grammar is:
 *
 * <ul>
 *   <li>{@code $} &mdash; the root document
 *   <li>{@code .field} &mdash; object field access
 *   <li>{@code [n]} &mdash; array index (non-negative integer)
 *   <li>{@code ['field']} / {@code ["field"]} &mdash; bracket field access (allows keys that
 *       contain {@code .} or {@code [})
 * </ul>
 *
 * <p>Wildcards ({@code [*]}, {@code .*}), recursive descent ({@code ..}) and filter expressions
 * ({@code [?(...)]}) are not supported.
 *
 * <p>Return semantics:
 *
 * <ul>
 *   <li>any null input (json or path) &rarr; {@code null}
 *   <li>invalid JSON &rarr; {@code null}
 *   <li>path does not match / indexes out of bounds &rarr; {@code null}
 *   <li>a JSON {@code null} value &rarr; {@code null}
 *   <li>a string value &rarr; the unquoted string content
 *   <li>a number or boolean &rarr; the value rendered as text
 *   <li>an object or array &rarr; the raw (compact) JSON text of that node
 * </ul>
 *
 * <p>JSON is parsed via the shaded Jackson runtime shared across SeaTunnel; the path walker is a
 * minimal hand-rolled tokenizer so the grammar above is matched exactly without pulling in a full
 * JSONPath engine.
 */
public class JsonFunction {

    private JsonFunction() {}

    /**
     * Extracts a value via {@code GET_JSON_OBJECT(json, path)}.
     *
     * @param args [json, path]
     * @return the extracted value as a string, or null on null input / invalid JSON / missing path
     */
    public static String getJsonObject(List<Object> args) {
        String operation = ZetaSQLFunction.GET_JSON_OBJECT;
        if (args.size() != 2) {
            throw CommonError.illegalArgument(
                    String.valueOf(args.size()), operation + " expects 2 arguments: (json, path)");
        }
        Object jsonArg = args.get(0);
        Object pathArg = args.get(1);
        if (jsonArg == null || pathArg == null) {
            return null;
        }
        return getJsonObject(jsonArg.toString(), pathArg.toString());
    }

    /** Core implementation, shared with the Calcite UDF so both engines behave identically. */
    public static String getJsonObject(String json, String path) {
        if (json == null || path == null) {
            return null;
        }
        JsonNode node;
        try {
            node = JsonUtils.stringToJsonNode(json);
        } catch (JsonProcessingException e) {
            // Return null on invalid JSON rather than raising.
            return null;
        }
        if (node == null) {
            return null;
        }
        return render(resolvePath(node, path));
    }

    /**
     * Walks the path against {@code root}, returning the matched node or null when the path is
     * malformed or does not resolve.
     */
    private static JsonNode resolvePath(JsonNode root, String path) {
        if (path.isEmpty() || path.charAt(0) != '$') {
            // The path must start with the root marker.
            return null;
        }
        JsonNode current = root;
        int i = 1;
        int len = path.length();
        while (i < len && current != null) {
            char c = path.charAt(i);
            if (c == '.') {
                int start = ++i;
                while (i < len && path.charAt(i) != '.' && path.charAt(i) != '[') {
                    i++;
                }
                String field = path.substring(start, i);
                if (field.isEmpty() || !current.isObject()) {
                    return null;
                }
                current = current.get(field);
            } else if (c == '[') {
                // Scan to the matching ']', skipping any ']' that appears inside a quoted field
                // name so keys like ['a]b'] resolve correctly.
                int close = -1;
                char quoteChar = 0;
                for (int j = i + 1; j < len; j++) {
                    char cj = path.charAt(j);
                    if (quoteChar != 0) {
                        if (cj == quoteChar) {
                            quoteChar = 0;
                        }
                    } else if (cj == '\'' || cj == '"') {
                        quoteChar = cj;
                    } else if (cj == ']') {
                        close = j;
                        break;
                    }
                }
                if (close < 0) {
                    return null;
                }
                String inside = path.substring(i + 1, close).trim();
                i = close + 1;
                if (inside.isEmpty()) {
                    return null;
                }
                char quote = inside.charAt(0);
                if (quote == '\'' || quote == '"') {
                    if (inside.length() < 2 || inside.charAt(inside.length() - 1) != quote) {
                        return null;
                    }
                    if (!current.isObject()) {
                        return null;
                    }
                    current = current.get(inside.substring(1, inside.length() - 1));
                } else {
                    int index;
                    try {
                        index = Integer.parseInt(inside);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                    if (index < 0 || !current.isArray() || index >= current.size()) {
                        return null;
                    }
                    current = current.get(index);
                }
            } else {
                // Unexpected char right after '$' or a segment: not a valid path.
                return null;
            }
        }
        return current;
    }

    /** Renders the matched node per the return semantics (see class Javadoc). */
    private static String render(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        switch (node.getNodeType()) {
            case OBJECT:
            case ARRAY:
                return node.toString();
            case STRING:
            case NUMBER:
            case BOOLEAN:
                return node.asText();
            default:
                return node.asText();
        }
    }
}
