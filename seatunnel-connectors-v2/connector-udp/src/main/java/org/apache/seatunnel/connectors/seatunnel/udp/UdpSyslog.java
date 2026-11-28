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

package org.apache.seatunnel.connectors.seatunnel.udp;

import java.time.DateTimeException;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.udp.UdpPacketFormat.InvalidDatagram;

/**
 * Parses the selected RFC 5424 or common BSD RFC 3164 message layout from strict UTF-8 input.
 *
 * <p>Structured data remains raw text, and legacy timestamps do not acquire an invented year or
 * zone.
 */
final class UdpSyslog {
    private static final Pattern PRIORITY = Pattern.compile("^<([0-9]{1,3})>");
    private static final Pattern VERSION = Pattern.compile("^[1-9][0-9]{0,2} ");
    private static final Pattern TIMESTAMP =
            Pattern.compile(
                    "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})");
    private static final Pattern LEGACY_TIME =
            Pattern.compile(
                    "^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([ 0-9][0-9]) "
                            + "([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9] ");
    private static final Pattern LEGACY_TAG =
            Pattern.compile("^([A-Za-z0-9]{1,32})(?:\\[([0-9]+)\\])?: ?");

    private UdpSyslog() {}

    /**
     * Select the supported header variant after validating the numeric priority. Preserve raw
     *
     * <p>structured data and legacy local timestamps without inventing a timezone, year or remote
     * identity.
     */
    static Object[] decode(String packet) throws InvalidDatagram {
        Matcher prefix = PRIORITY.matcher(packet);
        if (!prefix.find()) {
            throw malformed();
        }
        String rawPriority = prefix.group(1);
        int priority = Integer.parseInt(rawPriority);
        if (priority > 191 || (rawPriority.length() > 1 && rawPriority.charAt(0) == '0')) {
            throw malformed();
        }
        String body = packet.substring(prefix.end());
        return VERSION.matcher(body).find() ? modern(priority, body) : legacy(priority, body);
    }

    private static Object[] modern(int priority, String body) throws InvalidDatagram {
        String[] fields = body.split(" ", 7);
        if (fields.length != 7 || !"1".equals(fields[0])) {
            throw malformed();
        }
        String time = field(fields[1], 32);
        if (time != null) {
            if (!TIMESTAMP.matcher(time).matches()) {
                throw malformed();
            }
            try {
                OffsetDateTime.parse(time);
            } catch (DateTimeException e) {
                throw malformed();
            }
        }
        String host = field(fields[2], 255);
        String app = field(fields[3], 48);
        String process = field(fields[4], 128);
        String messageId = field(fields[5], 32);
        String tail = fields[6];
        int end = structuredDataEnd(tail);
        if (end < tail.length() && tail.charAt(end) != ' ') {
            throw malformed();
        }
        String structured = end == 1 && tail.charAt(0) == '-' ? null : tail.substring(0, end);
        String message = end == tail.length() ? null : tail.substring(end + 1);
        if (message != null && message.startsWith("\uFEFF")) {
            message = message.substring(1); // RFC 5424 marks MSG-UTF8 with an initial BOM.
        }
        return new Object[] {
            priority / 8, priority % 8, 1, time, host, app, process, messageId, structured, message
        };
    }

    private static Object[] legacy(int priority, String body) throws InvalidDatagram {
        Matcher time = LEGACY_TIME.matcher(body);
        if (!time.find() || !validDay(time.group(1), time.group(2))) {
            throw malformed();
        }
        int hostEnd = body.indexOf(' ', time.end());
        if (hostEnd < 0) {
            throw malformed();
        }
        String host = field(body.substring(time.end(), hostEnd), 255);
        String eventTime = body.substring(0, time.end() - 1);
        String message = body.substring(hostEnd + 1);
        Matcher tag = LEGACY_TAG.matcher(message);
        boolean tagged = tag.find();
        return new Object[] {
            priority / 8,
            priority % 8,
            null,
            eventTime,
            host,
            tagged ? tag.group(1) : null,
            tagged ? tag.group(2) : null,
            null,
            null,
            tagged ? message.substring(tag.end()) : message
        };
    }

    private static boolean validDay(String month, String day) {
        String monthNames = "JanFebMarAprMayJunJulAugSepOctNovDec";
        try {
            MonthDay.of(monthNames.indexOf(month) / 3 + 1, Integer.parseInt(day.trim()));
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }

    private static String field(String value, int limit) throws InvalidDatagram {
        if ("-".equals(value)) {
            return null;
        }
        if (value.isEmpty()
                || value.length() > limit
                || value.chars().anyMatch(ch -> ch < 33 || ch > 126)) {
            throw malformed();
        }
        return value;
    }

    /**
     * Finds the boundary by consuming each element and parameter as a unit. A quoted value's
     * escaped closing bracket is not a structured-data terminator.
     */
    private static int structuredDataEnd(String value) throws InvalidDatagram {
        if (value.startsWith("-")) {
            return 1;
        }
        int index = 0;
        while (index < value.length() && value.charAt(index) == '[') {
            index = nameEnd(value, index + 1);
            while (index < value.length() && value.charAt(index) == ' ') {
                index = nameEnd(value, index + 1);
                if (!value.startsWith("=\"", index)) {
                    throw malformed();
                }
                index = parameterEnd(value, index + 2);
            }
            if (index == value.length() || value.charAt(index++) != ']') {
                throw malformed();
            }
        }
        if (index == 0) {
            throw malformed();
        }
        return index;
    }

    private static int nameEnd(String value, int start) throws InvalidDatagram {
        int index = start;
        while (index < value.length()) {
            char character = value.charAt(index);
            if (character < 33
                    || character > 126
                    || character == '='
                    || character == '"'
                    || character == ']') {
                break;
            }
            index++;
        }
        if (index == start || index - start > 32) {
            throw malformed();
        }
        return index;
    }

    private static int parameterEnd(String value, int start) throws InvalidDatagram {
        for (int index = start; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '"') {
                return index + 1;
            }
            if (character == ']') {
                throw malformed();
            }
            if (character == '\\' && index + 1 < value.length()) {
                char next = value.charAt(index + 1);
                if (next == '\\' || next == '"' || next == ']') {
                    index++; // Other sequences retain the literal backslash per RFC 5424.
                }
            }
        }
        throw malformed();
    }

    private static InvalidDatagram malformed() {
        return new InvalidDatagram("malformed UDP syslog message");
    }
}
