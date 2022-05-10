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

package org.apache.seatunnel.common.parsing;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class StatementSqlBuilder {

    private final String originalSql;
    private String sql;
    private List<String> parameters;

    public StatementSqlBuilder(String originalSql) {
        this.originalSql = originalSql;
    }

    public String getSql() {
        return sql;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public void parse() {
        ParameterTokenHandler handler = new ParameterTokenHandler();
        GenericTokenParser parser = new GenericTokenParser("#{", "}", handler);
        this.sql = parser.parse(removeExtraWhitespaces(originalSql));
        this.parameters = handler.getParameters();
    }

    public static String removeExtraWhitespaces(String original) {
        StringTokenizer tokenizer = new StringTokenizer(original);
        StringBuilder builder = new StringBuilder();
        boolean hasMoreTokens = tokenizer.hasMoreTokens();
        while (hasMoreTokens) {
            builder.append(tokenizer.nextToken());
            hasMoreTokens = tokenizer.hasMoreTokens();
            if (hasMoreTokens) {
                builder.append(' ');
            }
        }
        return builder.toString();
    }

    private static class ParameterTokenHandler implements TokenHandler {

        private final List<String> parameters = new ArrayList<>();

        public List<String> getParameters() {
            return parameters;
        }

        @Override
        public String handleToken(String content) {
            parameters.add(content);
            return "?";
        }
    }
}
