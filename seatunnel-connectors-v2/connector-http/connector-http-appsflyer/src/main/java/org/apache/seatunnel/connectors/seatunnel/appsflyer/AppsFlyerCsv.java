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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.connectors.seatunnel.appsflyer.AppsFlyerRecords.InvalidReport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streams an AppsFlyer CSV file with UTF-8, quoted newlines, escaped quotes and fixed record
 * limits. Byte limits are enforced by the HTTP stream before CSV decoding.
 */
final class AppsFlyerCsv {
    private static final int MAX_COLUMNS = 2048;
    private static final int MAX_RECORD_CHARS = 4194304;
    private final PushbackReader input;
    private final int maxField;
    private boolean first = true;

    AppsFlyerCsv(InputStream body, int maxField) {
        this.input =
                new PushbackReader(
                        new BufferedReader(
                                new InputStreamReader(
                                        body,
                                        StandardCharsets.UTF_8
                                                .newDecoder()
                                                .onMalformedInput(CodingErrorAction.REPORT)
                                                .onUnmappableCharacter(CodingErrorAction.REPORT))),
                        1);
        this.maxField = maxField;
    }

    /**
     * Returns one logical CSV record or EOF. Embedded CR/LF are retained within quoted fields. An
     * unexpected quote or unequal byte stream fails before the report is accepted.
     */
    List<String> read() throws IOException {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        boolean closed = false;
        boolean seen = false;
        int length = 0;
        int ch;
        while ((ch = input.read()) != -1) {
            if (first) {
                first = false;
                if (ch == '\ufeff') {
                    continue;
                }
            }
            seen = true;
            if (++length > MAX_RECORD_CHARS) {
                throw new InvalidReport("CSV record exceeds its character limit");
            }
            if (quoted) {
                if (ch == '"') {
                    quoted = false;
                    closed = true;
                } else {
                    append(field, ch);
                }
                continue;
            }
            if (closed && ch == '"') {
                append(field, ch);
                quoted = true;
                closed = false;
                continue;
            }
            if (ch == ',' || ch == '\r' || ch == '\n') {
                add(fields, field);
                if (ch != ',') {
                    if (ch == '\r') {
                        int following = input.read();
                        if (following != '\n' && following != -1) {
                            input.unread(following);
                        }
                    }
                    return fields;
                }
                closed = false;
                continue;
            }
            if (closed) {
                throw new InvalidReport("unexpected content after a closing CSV quote");
            }
            if (ch == '"') {
                if (field.length() != 0) {
                    throw new InvalidReport("unexpected quote in an unquoted CSV field");
                }
                quoted = true;
            } else {
                append(field, ch);
            }
        }
        if (quoted) {
            throw new InvalidReport("unterminated quoted CSV field");
        }
        if (!seen) {
            return null;
        }
        add(fields, field);
        return fields;
    }

    private void append(StringBuilder field, int ch) throws IOException {
        if (field.length() == maxField) {
            throw new InvalidReport("CSV field exceeds max_field_chars");
        }
        field.append((char) ch);
    }

    private static void add(List<String> fields, StringBuilder field) throws IOException {
        if (fields.size() == MAX_COLUMNS) {
            throw new InvalidReport("CSV record exceeds its column limit");
        }
        fields.add(field.toString());
        field.setLength(0);
    }
}
