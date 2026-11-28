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

package org.apache.seatunnel.connectors.seatunnel.astra;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.MissingNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Extracts literal, array and wildcard values while limiting traversal work and result size.
 * Distinct output aliases are checked before they can overwrite another selected value.
 */
final class AstraPaths {
    private static final int MAX_VISITS = 32768;
    private static final int MAX_MATCHES = 1024;
    private static final int MAX_NESTING = 32;

    private AstraPaths() {}

    static boolean wildcard(String path) {
        return path.indexOf('*') >= 0 || path.indexOf('?') >= 0 || path.indexOf('[') >= 0;
    }

    static boolean valid(String path) {
        if (path == null
                || path.isBlank()
                || path.length() > 256
                || path.codePoints().anyMatch(Character::isISOControl)) {
            return false;
        }
        String[] segments = path.split("\\.", -1);
        if (segments.length > 12) {
            return false;
        }
        for (String segment : segments) {
            if (segment.isEmpty()
                    || segment.length() > 64
                    || (segment.contains("**") && !"**".equals(segment))) {
                return false;
            }
            try {
                if (!"**".equals(segment) && wildcard(segment)) {
                    glob(segment);
                }
            } catch (PatternSyntaxException ignored) {
                return false;
            }
        }
        return true;
    }

    /**
     * Resolves a literal path as a single JSON value. Missing and explicit JSON null remain
     * distinct. Array components are zero-based unsigned decimal indices.
     */
    static JsonNode one(JsonNode row, String path) {
        JsonNode value = row;
        for (String name : path.split("\\.")) {
            value = child(value, name);
        }
        return value;
    }

    private static JsonNode child(JsonNode node, String name) {
        if (node.isObject()) {
            return node.path(name);
        }
        if (node.isArray() && name.matches("0|[1-9][0-9]{0,8}")) {
            return node.path(Integer.parseInt(name));
        }
        return MissingNode.getInstance();
    }

    /**
     * Produces an ordered object from selected paths or all fields with the configured aliases. A
     * single match stays scalar and multiple wildcard matches remain arrays in traversal order.
     */
    static ObjectNode select(ObjectNode row, List<String> paths, Map<String, String> rename)
            throws IOException {
        ObjectNode selected = AstraJson.object();
        if (paths.isEmpty()) {
            Iterator<Map.Entry<String, JsonNode>> fields = row.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                add(
                        selected,
                        rename.getOrDefault(entry.getKey(), entry.getKey()),
                        entry.getValue());
            }
            return selected;
        }
        int[] budget = {MAX_VISITS};
        for (String path : paths) {
            JsonNode value;
            if (wildcard(path)) {
                List<JsonNode> matches = new ArrayList<>();
                String[] segments = path.split("\\.");
                List<List<Pattern>> compiled = new ArrayList<>(segments.length);
                for (String segment : segments) {
                    compiled.add(!"**".equals(segment) && wildcard(segment) ? glob(segment) : null);
                }
                search(row, segments, compiled, 0, 0, budget, matches);
                value =
                        matches.isEmpty()
                                ? MissingNode.getInstance()
                                : matches.size() == 1
                                        ? matches.get(0)
                                        : AstraJson.array().addAll(matches);
            } else {
                value = one(row, path);
            }
            if (!value.isMissingNode()) {
                add(selected, rename.getOrDefault(path, path), value);
            }
        }
        return selected;
    }

    private static void add(ObjectNode result, String key, JsonNode value) throws IOException {
        if (!AstraConfig.field(key) || result.has(key)) {
            throw new IOException("Astra sink: invalid or colliding selected output field");
        }
        result.set(key, value);
    }

    private static void search(
            JsonNode value,
            String[] path,
            List<List<Pattern>> compiled,
            int at,
            int depth,
            int[] budget,
            List<JsonNode> result)
            throws IOException {
        if (--budget[0] < 0 || depth > MAX_NESTING) {
            throw new IOException("Astra sink: wildcard traversal exceeds its bound");
        }
        if (at == path.length) {
            if (result.size() == MAX_MATCHES) {
                throw new IOException("Astra sink: wildcard expands to too many fields");
            }
            result.add(value);
            return;
        }
        String part = path[at];
        boolean recursive = "**".equals(part);
        if (recursive) {
            search(value, path, compiled, at + 1, depth, budget, result);
        } else if (!wildcard(part)) {
            JsonNode match = child(value, part);
            if (!match.isMissingNode()) {
                search(match, path, compiled, at + 1, depth + 1, budget, result);
            }
            return;
        }
        List<Pattern> names = compiled.get(at);
        if (value.isArray()) {
            for (int i = 0; i < value.size(); i++) {
                visit(budget);
                if (recursive || matches(names, Integer.toString(i))) {
                    search(
                            value.get(i),
                            path,
                            compiled,
                            recursive ? at : at + 1,
                            depth + 1,
                            budget,
                            result);
                }
            }
        } else if (value.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = value.fields();
            while (fields.hasNext()) {
                visit(budget);
                Map.Entry<String, JsonNode> field = fields.next();
                if (recursive || matches(names, field.getKey())) {
                    search(
                            field.getValue(),
                            path,
                            compiled,
                            recursive ? at : at + 1,
                            depth + 1,
                            budget,
                            result);
                }
            }
        }
    }

    private static void visit(int[] budget) throws IOException {
        if (--budget[0] < 0) {
            throw new IOException("Astra sink: wildcard traversal exceeds its bound");
        }
    }

    /**
     * Matches one component with bounded dynamic programming to avoid overlapping wildcard
     * backtracking. A field name cannot span multiple path segments.
     */
    private static boolean matches(List<Pattern> rules, String key) {
        if (key.length() > 256) {
            return false;
        }
        String[] characters =
                key.codePoints()
                        .mapToObj(c -> new String(Character.toChars(c)))
                        .toArray(String[]::new);
        boolean[] previous = new boolean[characters.length + 1];
        previous[0] = true;
        for (Pattern rule : rules) {
            boolean[] current = new boolean[previous.length];
            current[0] = rule == null && previous[0];
            for (int i = 1; i < current.length; i++) {
                current[i] =
                        rule == null
                                ? previous[i] || current[i - 1]
                                : previous[i - 1] && rule.matcher(characters[i - 1]).matches();
            }
            previous = current;
        }
        return previous[characters.length];
    }

    private static List<Pattern> glob(String segment) {
        List<Pattern> rules = new ArrayList<>();
        for (int i = 0; i < segment.length(); i++) {
            int c = segment.codePointAt(i);
            if (c == '*') {
                rules.add(null);
            } else if (c == '?') {
                rules.add(Pattern.compile(".", Pattern.DOTALL));
            } else if (c == '[') {
                int end = segment.indexOf(']', i + 1);
                if (end > i + 1) {
                    String group = segment.substring(i + 1, end);
                    StringBuilder expression = new StringBuilder("[");
                    if (group.startsWith("!")) {
                        expression.append('^');
                        group = group.substring(1);
                    }
                    for (int j = 0; j < group.length(); j++) {
                        char member = group.charAt(j);
                        if ("[\\^&".indexOf(member) >= 0) {
                            expression.append('\\');
                        }
                        expression.append(member);
                    }
                    expression.append(']');
                    rules.add(Pattern.compile(expression.toString()));
                    i = end;
                } else {
                    rules.add(Pattern.compile("\\["));
                }
            } else {
                rules.add(Pattern.compile(Pattern.quote(new String(Character.toChars(c)))));
                i += Character.charCount(c) - 1;
            }
        }
        return rules;
    }
}
