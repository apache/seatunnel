/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.sql.splitter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is a simple splitter to split the submitted content into multiple statements.
 */
public class SqlStatementSplitter {

    private static final String COMMENT_MASK = "--.*$";
    private static final String BEGINNING_COMMENT_MASK = "^(\\s)*--.*$";
    private static final String SEMICOLON = ";";
    private static final String LINE_SEPARATOR = "\n";
    private static final String EMPTY_STR = "";

    public static List<String> normalizeStatements(String content) {
        List<String> normalizedStatements = new ArrayList<>();

        for (String stmt : splitContent(content)) {
            stmt = stmt.trim();
            if (stmt.endsWith(SEMICOLON)) {
                stmt = stmt.substring(0, stmt.length() - 1).trim();
            }

            // ignore invalid case, e.g ";\n"
            if (!stmt.trim().isEmpty()) {
                normalizedStatements.add(stmt);
            }
        }
        return normalizedStatements;
    }

    private static List<String> splitContent(String content) {
        List<String> statements = new ArrayList<>();
        List<String> buffer = new ArrayList<>();

        for (String line : content.split(LINE_SEPARATOR)) {
            if (isEndOfStatement(line)) {
                buffer.add(line);
                statements.add(normalizeLine(buffer));
                buffer.clear();
            } else {
                buffer.add(line);
            }
        }
        if (!buffer.isEmpty()) {
            statements.add(normalizeLine(buffer));
        }
        return statements;
    }

    /**
     * Remove comment lines.
     */
    private static String normalizeLine(List<String> buffer) {
        return buffer.stream()
                     .map(statementLine -> statementLine.replaceAll(BEGINNING_COMMENT_MASK, EMPTY_STR))
                     .collect(Collectors.joining(LINE_SEPARATOR));
    }

    private static boolean isEndOfStatement(String line) {
        return line.replaceAll(COMMENT_MASK, EMPTY_STR).trim().endsWith(SEMICOLON);
    }
}
