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

package io.debezium.connector.oracle.logminer.processor;

import io.debezium.DebeziumException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses the SQL text LogMiner reconstructs for one LOB write. */
final class LogMinerSqlParser {

    private static final Pattern LOB_WRITE_SQL_PATTERN =
            Pattern.compile(
                    "(?s).* := ((?:HEXTORAW\\()?'.*'(?:\\))?);\\s*dbms_lob.write\\([^,]+,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,[^,]+\\);.*");

    private LogMinerSqlParser() {}

    static LobWrite parseLobWrite(String sql) {
        if (sql == null) {
            return null;
        }
        Matcher matcher = LOB_WRITE_SQL_PATTERN.matcher(sql.trim());
        if (!matcher.matches()) {
            throw new DebeziumException("Unable to parse unsupported LOB_WRITE SQL: " + sql);
        }
        String data = matcher.group(1);
        if (data.startsWith("'")) {
            // LogMiner quotes the buffer as a SQL literal. A literal apostrophe is written twice.
            data = unescapeLiteral(data.substring(1, data.length() - 1));
        }
        int length = Integer.parseInt(matcher.group(2));
        int offset = Integer.parseInt(matcher.group(3)) - 1;
        return new LobWrite(offset, length, data);
    }

    /**
     * Collapses the doubled quotes of a SQL string literal. A single quote that LogMiner did not
     * escape is left unchanged.
     */
    static String unescapeLiteral(String data) {
        if (data.indexOf('\'') < 0) {
            return data;
        }
        StringBuilder builder = new StringBuilder(data.length());
        for (int i = 0; i < data.length(); i++) {
            char current = data.charAt(i);
            builder.append(current);
            if (current == '\'' && i + 1 < data.length() && data.charAt(i + 1) == '\'') {
                i++;
            }
        }
        return builder.toString();
    }

    static final class LobWrite {
        final int offset;
        final int length;
        final String data;

        LobWrite(int offset, int length, String data) {
            this.offset = offset;
            this.length = length;
            this.data = data;
        }
    }
}
