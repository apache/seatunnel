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

package io.debezium.connector.oracle.logminer.parser;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for decoding an {@code LOB_WRITE} LogMiner REDO_SQL value.
 *
 * <p>Official (Debezium 3.7.1.Final): {@code
 * debezium-connector-oracle/.../parser/LobWriteParser.java} (DBZ-8924). Same {@code SQL_PATTERN}
 * and {@code ''} → {@code '} unescape.
 *
 * <p>SeaTunnel changes: Java 8 nested {@link LobWrite} instead of a {@code record}.
 *
 * @author Chris Cranford
 */
public final class LobWriteParser {

    private static final Pattern SQL_PATTERN =
            Pattern.compile(
                    "(?s).* := ((?:HEXTORAW\\()?'.*'(?:\\))?);\\s*dbms_lob.write\\([^,]+,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,[^,]+\\);.*");

    private LobWriteParser() {}

    /**
     * Parses the {@code LOB_WRITE} redo SQL.
     *
     * @param redoSql the SQL to be parsed
     * @return the parsed details, or {@code null} when the SQL is empty
     */
    public static LobWrite parse(String redoSql) {
        if (Strings.isNullOrEmpty(redoSql)) {
            return null;
        }

        Matcher matcher = SQL_PATTERN.matcher(redoSql.trim());
        if (!matcher.matches()) {
            throw new DebeziumException("Unable to parse unsupported LOB_WRITE SQL: " + redoSql);
        }

        String data = matcher.group(1);
        if (data.startsWith("'")) {
            // string data; drop the quotes
            data = data.substring(1, data.length() - 1);
        }
        int length = Integer.parseInt(matcher.group(2));
        int offset = Integer.parseInt(matcher.group(3)) - 1; // Oracle uses 1-based offsets

        // Double check whether Oracle may have escaped single-quotes in the SQL data.
        // This avoids unintended truncation during the LOB merge phase during the commit
        // logic handled by TransactionCommitConsumer.
        if (data.contains("''")) {
            data = data.replaceAll("''", "'");
        }

        return new LobWrite(offset, length, data);
    }

    public static final class LobWrite {
        public final int offset;
        public final int length;
        public final String data;

        LobWrite(int offset, int length, String data) {
            this.offset = offset;
            this.length = length;
            this.data = data;
        }
    }
}
