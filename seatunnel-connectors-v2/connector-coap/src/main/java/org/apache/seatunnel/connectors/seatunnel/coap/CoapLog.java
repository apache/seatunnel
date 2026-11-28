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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Decodes common line-oriented application and security log layouts. */
final class CoapLog {
    private static final String ACCESS =
            "(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]*)\" (\\d{3}) (\\d+|-)";
    private static final String[] ACCESS_FIELDS = {
        "clientip", "ident", "auth", "timestamp", "request_line", "response", "bytes"
    };
    private static final String[] ERROR_FIELDS = {
        "timestamp", "module", "level", "pid", "client", "message"
    };
    private static final String[] JAVA_FIELDS = {"timestamp", "level", "logger", "message"};
    private static final Pattern GROK =
            Pattern.compile("%\\{([A-Za-z0-9_]+)(?::([A-Za-z0-9_.@-]+))?\\}");
    private static final Pattern CEF_KEYS = Pattern.compile("(?:^| )([A-Za-z0-9_]+)=");
    private static final Map<String, String> GROK_DEFAULTS =
            Map.ofEntries(
                    Map.entry("WORD", "\\b\\w+\\b"),
                    Map.entry("INT", "[+-]?\\d+"),
                    Map.entry("NUMBER", "[+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)"),
                    Map.entry("IP", "[0-9a-fA-F:.]+"),
                    Map.entry("IPORHOST", "[a-zA-Z0-9.:_-]+"),
                    Map.entry("HOSTNAME", "[a-zA-Z0-9._-]+"),
                    Map.entry("NOTSPACE", "\\S+"),
                    Map.entry("SPACE", "\\s*"),
                    Map.entry("DATA", ".*?"),
                    Map.entry("GREEDYDATA", ".*"),
                    Map.entry("HTTPDATE", "[^\\]]+"),
                    Map.entry(
                            "TIMESTAMP_ISO8601",
                            "\\d{4}-\\d{2}-\\d{2}[T ][0-9:,\\.]+(?:Z|[+-]\\d{2}:?\\d{2})?"));

    private CoapLog() {}

    static CoapPayload.Decoder decoder(CoapSettings.Listener settings, CatalogTable table) {
        CoapOptions.LogMode mode = settings.options.get(CoapOptions.LOG_MODE);
        String pattern = settings.options.get(CoapOptions.LOG_PATTERN);
        CoapParsedRows mapper = new CoapParsedRows(settings, table);
        Parser parser = parser(mode, pattern, settings);
        return (bytes, peer) -> {
            List<SeaTunnelRow> rows = new ArrayList<>();
            Iterator<String> lines = CoapPayload.decodeText(bytes).lines().iterator();
            while (lines.hasNext()) {
                String line = lines.next();
                if (line.isEmpty()
                        || line.length() > 16384
                        || rows.size() == CoapSettings.MAX_ROWS) {
                    throw new IOException("log record is empty or exceeds its limit");
                }
                rows.add(mapper.row(parser.parse(line)));
            }
            if (rows.isEmpty()) {
                throw new IOException("log message contains no records");
            }
            return rows;
        };
    }

    private static Parser parser(
            CoapOptions.LogMode mode, String pattern, CoapSettings.Listener settings) {
        switch (mode) {
            case COMMON_LOG_FORMAT:
                return regex(ACCESS, indices(ACCESS_FIELDS));
            case COMBINED_LOG_FORMAT:
                Map<String, Integer> combined = indices(ACCESS_FIELDS);
                combined.put("referrer", 8);
                combined.put("agent", 9);
                return regex(ACCESS + " \"([^\"]*)\" \"([^\"]*)\"", combined);
            case APACHE_ERROR_LOG_FORMAT:
                return regex(
                        "\\[([^\\]]+)\\] \\[(?:(\\w+):)?(\\w+)\\]"
                                + "(?: \\[pid (\\d+)[^\\]]*\\])?(?: \\[client ([^\\]]+)\\])?\\s*(.*)",
                        indices(ERROR_FIELDS));
            case APACHE_CUSTOM_LOG_FORMAT:
                return customLayout(pattern == null ? "%h %l %u %t \"%r\" %>s %b" : pattern, true);
            case LOG4J:
                return customLayout(
                        pattern == null ? "%d{ISO8601} %-5p %c{1} - %m" : pattern, false);
            case REGEX:
                Map<String, Integer> groups = settings.options.get(CoapOptions.LOG_GROUPS);
                if (pattern == null || groups.isEmpty()) {
                    throw CoapSettings.invalid(
                            "regex log_mode requires log_pattern and log_groups");
                }
                return regex(pattern, groups);
            case GROK:
                return grok(pattern, settings.options.get(CoapOptions.GROK_DEFINITIONS));
            case CEF:
                return CoapLog::cef;
            case LEEF:
                return CoapLog::leef;
            default:
                throw CoapSettings.invalid("invalid log_mode");
        }
    }

    private static Map<String, Integer> indices(String[] names) {
        Map<String, Integer> indices = new LinkedHashMap<>();
        for (int i = 0; i < names.length; i++) {
            indices.put(names[i], i + 1);
        }
        return indices;
    }

    private static Parser regex(String pattern, Map<String, Integer> fields) {
        if (pattern.length() > 8192 || fields.size() > 128) {
            throw CoapSettings.invalid("log pattern or field mapping exceeds its limit");
        }
        Pattern compiled = compile(pattern);
        int groups = compiled.matcher("").groupCount();
        if (fields.values().stream().anyMatch(index -> index < 0 || index > groups)) {
            throw CoapSettings.invalid("log_groups contains an unavailable group index");
        }
        return line -> {
            Matcher matched = compiled.matcher(new GuardedInput(line));
            try {
                if (!matched.matches()) {
                    throw new IOException("log record does not match its layout");
                }
                ObjectNode result = CoapPayload.JSON.createObjectNode();
                for (Map.Entry<String, Integer> named : fields.entrySet()) {
                    result.put(named.getKey(), matched.group(named.getValue()));
                }
                String request = result.path("request_line").asText("");
                String[] parts = request.split(" ", 3);
                if (parts.length == 3) {
                    result.put("verb", parts[0])
                            .put("request", parts[1])
                            .put("httpversion", parts[2]);
                }
                return result;
            } catch (ArithmeticException | StackOverflowError e) {
                throw new IOException("log regex exceeded its parsing budget");
            }
        };
    }

    private static Parser customLayout(String layout, boolean apache) {
        if (layout.length() > 512) {
            throw CoapSettings.invalid("log layout exceeds 512 characters");
        }
        Pattern specifiers =
                apache
                        ? Pattern.compile("%(?:\\{[^}]+\\})?[><]?[a-zA-Z%]")
                        : Pattern.compile("%-?\\d*[dpcmt%](?:\\{[^}]+\\})?");
        Matcher tokens = specifiers.matcher(layout);
        StringBuilder regex = new StringBuilder();
        Map<String, Integer> fields = new LinkedHashMap<>();
        int previous = 0;
        int group = 0;
        while (tokens.find()) {
            regex.append(literal(layout.substring(previous, tokens.start())));
            previous = tokens.end();
            String token = tokens.group();
            if ("%%".equals(token)) {
                regex.append('%');
                continue;
            }
            String field = apache ? apacheField(token) : javaField(token);
            String capture =
                    "timestamp".equals(field) && apache
                            ? "\\[([^\\]]+)\\]"
                            : "response".equals(field)
                                    ? "(\\d{3})"
                                    : "request_line".equals(field)
                                                    || "referrer".equals(field)
                                                    || "agent".equals(field)
                                            ? "([^\"]*)"
                                            : "message".equals(field) || "timestamp".equals(field)
                                                    ? "(.+?)"
                                                    : "(\\S+)";
            regex.append(capture);
            if (fields.put(field, ++group) != null) {
                throw CoapSettings.invalid("log layout emits a field more than once");
            }
        }
        regex.append(literal(layout.substring(previous)));
        if (fields.isEmpty()) {
            throw CoapSettings.invalid("log layout has no supported conversion specifiers");
        }
        return regex(regex.toString(), fields);
    }

    private static String literal(String text) {
        if (text.indexOf('%') >= 0) {
            throw CoapSettings.invalid("unsupported conversion specifier in log_pattern");
        }
        return Pattern.quote(text).replace(" ", "\\E\\s+\\Q");
    }

    private static String apacheField(String token) {
        switch (token) {
            case "%a":
            case "%h":
                return "clientip";
            case "%l":
                return "ident";
            case "%u":
                return "auth";
            case "%t":
                return "timestamp";
            case "%r":
                return "request_line";
            case "%s":
            case "%>s":
                return "response";
            case "%b":
            case "%B":
                return "bytes";
            case "%{Referer}i":
                return "referrer";
            case "%{User-Agent}i":
                return "agent";
            case "%m":
                return "verb";
            case "%U":
                return "request";
            case "%H":
                return "httpversion";
            case "%q":
                return "query";
            case "%D":
                return "request_micros";
            case "%T":
                return "request_seconds";
            case "%p":
                return "port";
            case "%v":
            case "%V":
                return "server";
            default:
                if (token.matches("%\\{[A-Za-z0-9_-]{1,64}\\}[io]")) {
                    return "header_" + token.substring(2, token.length() - 2);
                }
                throw CoapSettings.invalid("unsupported Apache access-log conversion specifier");
        }
    }

    private static String javaField(String token) {
        char specifier = token.substring(1).replaceFirst("^-?\\d*", "").charAt(0);
        switch (specifier) {
            case 'd':
                return JAVA_FIELDS[0];
            case 'p':
                return JAVA_FIELDS[1];
            case 'c':
                return JAVA_FIELDS[2];
            case 'm':
                return JAVA_FIELDS[3];
            case 't':
                return "thread";
            default:
                throw CoapSettings.invalid("unsupported Log4j conversion specifier");
        }
    }

    private static Parser grok(String pattern, Map<String, String> definitions) {
        if (pattern == null || "%{COMMONAPACHELOG}".equals(pattern)) {
            return regex(ACCESS, indices(ACCESS_FIELDS));
        }
        if ("%{COMBINEDAPACHELOG}".equals(pattern)) {
            Map<String, Integer> fields = indices(ACCESS_FIELDS);
            fields.put("referrer", 8);
            fields.put("agent", 9);
            return regex(ACCESS + " \"([^\"]*)\" \"([^\"]*)\"", fields);
        }
        if (definitions.size() > 128) {
            throw CoapSettings.invalid("too many Grok definitions");
        }
        Map<String, Integer> fields = new LinkedHashMap<>();
        String regex = expandGrok(pattern, definitions, fields, 0);
        if (compile(regex).matcher("").groupCount() != fields.size()) {
            throw CoapSettings.invalid("Grok definitions must use noncapturing groups");
        }
        return regex(regex, fields);
    }

    private static Pattern compile(String pattern) {
        try {
            return Pattern.compile(pattern);
        } catch (IllegalArgumentException error) {
            throw CoapSettings.invalid("invalid log expression");
        }
    }

    private static String expandGrok(
            String pattern,
            Map<String, String> definitions,
            Map<String, Integer> fields,
            int depth) {
        if (depth > 8 || pattern.length() > 8192 || fields.size() > 128) {
            throw CoapSettings.invalid("Grok expansion exceeds its limit");
        }
        StringBuffer result = new StringBuffer();
        Matcher placeholders = GROK.matcher(pattern);
        while (placeholders.find()) {
            String name = placeholders.group(1);
            String definition = definitions.getOrDefault(name, GROK_DEFAULTS.get(name));
            if (definition == null) {
                throw CoapSettings.invalid("Grok references an unknown definition");
            }
            String field = placeholders.group(2);
            if (field != null && fields.put(field, fields.size() + 1) != null) {
                throw CoapSettings.invalid("Grok emits a field more than once");
            }
            String expanded = expandGrok(definition, definitions, fields, depth + 1);
            if (field == null) {
                expanded = "(?:" + expanded + ")";
            } else {
                expanded = "(" + expanded + ")";
            }
            placeholders.appendReplacement(result, Matcher.quoteReplacement(expanded));
            if (result.length() > 8192) {
                throw CoapSettings.invalid("Grok expansion exceeds its limit");
            }
        }
        placeholders.appendTail(result);
        return result.toString();
    }

    private static ObjectNode cef(String line) throws IOException {
        List<String> parts = splitHeader(line, "CEF:", 7);
        ObjectNode event = CoapPayload.JSON.createObjectNode();
        String[] keys = {
            "version", "vendor", "product", "device_version", "signature", "name", "severity"
        };
        for (int i = 0; i < keys.length; i++) {
            event.put(keys[i], parts.get(i));
        }
        ObjectNode attributes = event.putObject("extensions");
        String tail = parts.get(7);
        Matcher fields = CEF_KEYS.matcher(tail);
        String previous = null;
        int valueStart = 0;
        while (fields.find()) {
            if (previous != null) {
                attributes.put(
                        previous, tail.substring(valueStart, fields.start()).replace("\\=", "="));
            }
            previous = fields.group(1);
            valueStart = fields.end();
        }
        if (previous != null) {
            attributes.put(previous, tail.substring(valueStart).replace("\\=", "="));
        } else if (!tail.isEmpty()) {
            throw new IOException("invalid CEF attributes");
        }
        return event;
    }

    private static ObjectNode leef(String line) throws IOException {
        boolean versionTwo = line.contains("LEEF:2.0|");
        List<String> parts = splitHeader(line, "LEEF:", versionTwo ? 6 : 5);
        ObjectNode event = CoapPayload.JSON.createObjectNode();
        String[] keys = {"version", "vendor", "product", "device_version", "event_id"};
        for (int i = 0; i < keys.length; i++) {
            event.put(keys[i], parts.get(i));
        }
        String delimiter = versionTwo ? parts.get(5) : "\t";
        if (delimiter.matches("0x[0-9a-fA-F]{2}")) {
            delimiter = String.valueOf((char) Integer.parseInt(delimiter.substring(2), 16));
        }
        if (delimiter.length() != 1) {
            throw new IOException("invalid LEEF attribute delimiter");
        }
        ObjectNode extensions = event.putObject("extensions");
        String tail = parts.get(parts.size() - 1);
        if (!tail.isEmpty()) {
            for (String field : tail.split(Pattern.quote(delimiter), -1)) {
                int eq = field.indexOf('=');
                if (eq < 1) {
                    throw new IOException("invalid LEEF attribute");
                }
                extensions.put(field.substring(0, eq), field.substring(eq + 1));
            }
        }
        return event;
    }

    private static List<String> splitHeader(String line, String start, int fields)
            throws IOException {
        int startIndex = line.indexOf(start);
        if (startIndex < 0) {
            throw new IOException("expected event log header");
        }
        List<String> parts = new ArrayList<>(fields + 1);
        StringBuilder value = new StringBuilder();
        int cursor = startIndex + start.length();
        for (; cursor < line.length() && parts.size() < fields; cursor++) {
            char ch = line.charAt(cursor);
            if (ch == '\\' && cursor + 1 < line.length()) {
                value.append(line.charAt(++cursor));
            } else if (ch == '|') {
                parts.add(value.toString());
                value.setLength(0);
            } else {
                value.append(ch);
            }
        }
        if (parts.size() != fields) {
            throw new IOException("incomplete event log header");
        }
        parts.add(line.substring(cursor));
        return parts;
    }

    /** Decodes a single log entry before the resulting fields are mapped to columns. */
    private interface Parser {
        ObjectNode parse(String line) throws IOException;
    }

    /** Shares a deterministic character access budget across regex input views. */
    private static final class GuardedInput implements CharSequence {
        private final String input;
        private final int start;
        private final int end;
        private final int[] operations;

        private GuardedInput(String input) {
            this(input, 0, input.length(), new int[] {1000000});
        }

        private GuardedInput(String input, int start, int end, int[] operations) {
            this.input = input;
            this.start = start;
            this.end = end;
            this.operations = operations;
        }

        @Override
        public int length() {
            return end - start;
        }

        @Override
        public char charAt(int index) {
            if (--operations[0] < 0) {
                throw new ArithmeticException("regex operations exceeded");
            }
            return input.charAt(start + index);
        }

        @Override
        public CharSequence subSequence(int offset, int length) {
            return new GuardedInput(input, start + offset, start + length, operations);
        }

        @Override
        public String toString() {
            return input.substring(start, end);
        }
    }
}
