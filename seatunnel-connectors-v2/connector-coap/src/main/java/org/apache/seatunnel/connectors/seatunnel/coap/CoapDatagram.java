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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Decodes a supported inner datagram from a complete CoAP message. */
final class CoapDatagram implements CoapPayload.Decoder {
    private static final Pattern PRI = Pattern.compile("<(\\d{1,3})>");
    private static final Pattern BSD =
            Pattern.compile(
                    "(\\w{3} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}) (\\S+) (.*)", Pattern.DOTALL);
    private static final Pattern BSD_TAG =
            Pattern.compile("([^ :\\[\\]]+)(?:\\[([^\\]]+)\\])?: ?(.*)", Pattern.DOTALL);
    private final CoapOptions.DatagramMode mode;
    private final String separator;
    private final CoapParsedRows rows;
    private final CoapNetflow netflow;
    private final CoapCollectd collectd;

    CoapDatagram(CoapSettings.Listener settings, CatalogTable table) {
        mode = settings.datagramMode;
        separator = settings.options.get(CoapOptions.DATAGRAM_SEPARATOR);
        if (separator != null && (separator.isEmpty() || separator.length() > 32)) {
            throw CoapSettings.invalid("datagram_separator must be 1 to 32 characters");
        }
        rows = new CoapParsedRows(settings, table);
        netflow =
                mode == CoapOptions.DatagramMode.NETFLOW
                        ? new CoapNetflow(
                                settings.netflowTemplates, settings.netflowTemplateTimeout)
                        : null;
        collectd = mode == CoapOptions.DatagramMode.COLLECTD ? new CoapCollectd(settings) : null;
    }

    @Override
    public List<SeaTunnelRow> decode(byte[] bytes, InetSocketAddress peer) throws IOException {
        if (mode == CoapOptions.DatagramMode.RAW_DATA) {
            return raw(bytes);
        }
        List<ObjectNode> decoded;
        switch (mode) {
            case SYSLOG:
                decoded = syslog(bytes);
                break;
            case NETFLOW:
                decoded = netflow.decode(bytes, peer);
                break;
            case COLLECTD:
                decoded = collectd.decode(bytes);
                break;
            default:
                throw new IOException("unsupported inner datagram format");
        }
        List<SeaTunnelRow> result = new ArrayList<>(decoded.size());
        for (ObjectNode record : decoded) {
            result.add(rows.row(record));
        }
        return result;
    }

    private List<SeaTunnelRow> raw(byte[] bytes) throws IOException {
        if (separator == null) {
            return CoapPayload.singleton(new SeaTunnelRow(new Object[] {bytes.clone()}));
        }
        String[] records =
                CoapPayload.decodeText(bytes)
                        .split(Pattern.quote(separator), CoapSettings.MAX_ROWS + 1);
        if (records.length > CoapSettings.MAX_ROWS) {
            throw new IOException("too many raw datagram records");
        }
        List<SeaTunnelRow> result = new ArrayList<>(records.length);
        for (String record : records) {
            result.add(new SeaTunnelRow(new Object[] {record}));
        }
        return result;
    }

    private static List<ObjectNode> syslog(byte[] bytes) throws IOException {
        String text = CoapPayload.decodeText(bytes);
        String[] messages = text.split("\\r?\\n(?=<\\d{1,3}>)", CoapSettings.MAX_ROWS + 1);
        if (messages.length > CoapSettings.MAX_ROWS) {
            throw invalid();
        }
        List<ObjectNode> output = new ArrayList<>(messages.length);
        for (String message : messages) {
            Matcher priority = PRI.matcher(message);
            if (!priority.lookingAt()) {
                throw invalid();
            }
            int pri = Integer.parseInt(priority.group(1));
            String tail = message.substring(priority.end());
            if (pri > 191 || tail.isEmpty()) {
                throw invalid();
            }
            ObjectNode result =
                    CoapPayload.JSON
                            .createObjectNode()
                            .put("priority", pri)
                            .put("facility", pri / 8)
                            .put("severity", pri % 8);
            if (Character.isDigit(tail.charAt(0))) {
                modern(result, tail);
            } else {
                legacy(result, tail);
            }
            output.add(result);
        }
        return output;
    }

    private static void legacy(ObjectNode result, String message) throws IOException {
        Matcher fields = BSD.matcher(message);
        if (!fields.matches()) {
            throw invalid();
        }
        result.put("version", 0).put("timestamp", fields.group(1)).put("hostname", fields.group(2));
        Matcher tag = BSD_TAG.matcher(fields.group(3));
        if (tag.matches()) {
            result.put("app_name", tag.group(1));
            if (tag.group(2) != null) {
                result.put("proc_id", tag.group(2));
            }
            result.put("message", tag.group(3));
        } else {
            result.put("message", fields.group(3));
        }
    }

    private static void modern(ObjectNode result, String message) throws IOException {
        String[] headers = message.split(" ", 7);
        if (headers.length != 7 || !headers[0].matches("[1-9][0-9]{0,2}")) {
            throw invalid();
        }
        result.put("version", Integer.parseInt(headers[0]));
        String[] names = {"timestamp", "hostname", "app_name", "proc_id", "msg_id"};
        for (int i = 0; i < names.length; i++) {
            if (headers[i + 1].isEmpty()) {
                throw invalid();
            }
            if ("-".equals(headers[i + 1])) {
                result.putNull(names[i]);
            } else {
                result.put(names[i], headers[i + 1]);
            }
        }
        String rest = headers[6];
        if (rest.isEmpty()) {
            throw invalid();
        }
        int offset;
        if (rest.charAt(0) == '-') {
            result.putNull("structured_data");
            offset = 1;
        } else {
            offset = structuredData(rest, result.putObject("structured_data"));
        }
        if (offset < rest.length()) {
            if (rest.charAt(offset) != ' ') {
                throw invalid();
            }
            result.put("message", rest.substring(offset + 1));
        }
    }

    private static int structuredData(String text, ObjectNode output) throws IOException {
        int cursor = 0;
        while (cursor < text.length() && text.charAt(cursor) == '[') {
            int begin = ++cursor;
            while (cursor < text.length()
                    && text.charAt(cursor) != ' '
                    && text.charAt(cursor) != ']') {
                cursor++;
            }
            String id = name(text, begin, cursor);
            if (output.has(id)) {
                throw invalid();
            }
            ObjectNode fields = output.putObject(id);
            while (cursor < text.length() && text.charAt(cursor) == ' ') {
                int keyStart = ++cursor;
                int equals = text.indexOf('=', keyStart);
                String key = name(text, keyStart, equals);
                if (equals + 1 >= text.length()
                        || text.charAt(equals + 1) != '"'
                        || fields.has(key)) {
                    throw invalid();
                }
                cursor = equals + 2;
                StringBuilder value = new StringBuilder();
                while (cursor < text.length() && text.charAt(cursor) != '"') {
                    char ch = text.charAt(cursor++);
                    if (ch == '\\' && cursor < text.length()) {
                        char next = text.charAt(cursor);
                        if (next == '\\' || next == '"' || next == ']') {
                            ch = next;
                            cursor++;
                        }
                    } else if (ch == ']') {
                        throw invalid();
                    }
                    value.append(ch);
                }
                if (cursor == text.length()) {
                    throw invalid();
                }
                cursor++;
                fields.put(key, value.toString());
            }
            if (cursor >= text.length() || text.charAt(cursor++) != ']') {
                throw invalid();
            }
        }
        if (cursor == 0) {
            throw invalid();
        }
        return cursor;
    }

    private static String name(String text, int start, int end) throws IOException {
        if (end <= start || end - start > 32) {
            throw invalid();
        }
        String result = text.substring(start, end);
        for (int i = 0; i < result.length(); i++) {
            char ch = result.charAt(i);
            if (ch < 33 || ch > 126 || ch == '"' || ch == '=' || ch == ']') {
                throw invalid();
            }
        }
        return result;
    }

    private static IOException invalid() {
        return new IOException("invalid syslog datagram");
    }
}
