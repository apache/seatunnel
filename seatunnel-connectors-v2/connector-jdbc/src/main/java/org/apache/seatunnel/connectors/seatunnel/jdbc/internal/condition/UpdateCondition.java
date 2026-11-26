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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.condition;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * The parsed {@code update_condition_sql} option. SQL is rendered from the parsed tree, so the
 * user's text is never passed to the database. The accepted grammar is:
 *
 * <pre>
 * condition  := or
 * or         := and ( OR and )*
 * and        := not ( AND not )*
 * not        := NOT not | primary
 * primary    := '(' condition ')' | operand ( comparison operand | IS [ NOT ] NULL )
 * comparison := '=' | '&lt;&gt;' | '&lt;' | '&lt;=' | '&gt;' | '&gt;='
 * operand    := ':' column | target '.' column
 * column     := identifier | '"' identifier with "" as an escaped quote '"'
 * </pre>
 *
 * <p>{@code :column} is the incoming row and {@code target.column} the stored row. Keywords are
 * case-insensitive, column names are not.
 */
public final class UpdateCondition {

    private static final String OPTION_KEY = "update_condition_sql";

    /** Limits nesting so that a deeply nested condition cannot exhaust the stack. */
    private static final int MAX_NESTING_DEPTH = 32;

    private final Node root;

    private UpdateCondition(Node root) {
        this.root = root;
    }

    /** Parses the condition, which may only reference columns written by the sink. */
    public static UpdateCondition parse(String condition, Collection<String> columnNames) {
        if (condition == null || condition.trim().isEmpty()) {
            throw invalid("the condition must not be blank");
        }
        Parser parser = new Parser(condition, new LinkedHashSet<>(columnNames));
        return new UpdateCondition(parser.parse());
    }

    /** Renders the condition as SQL, quoting columns like the rest of the statement. */
    public String toSql(
            Function<String, String> quoteIdentifier,
            String incomingQualifier,
            String targetQualifier) {
        StringBuilder sql = new StringBuilder();
        root.render(sql, quoteIdentifier, incomingQualifier, targetQualifier);
        return sql.toString();
    }

    private static JdbcConnectorException invalid(String reason) {
        return new JdbcConnectorException(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("Invalid %s: %s", OPTION_KEY, reason));
    }

    private interface Node {
        void render(
                StringBuilder sql,
                Function<String, String> quoteIdentifier,
                String incomingQualifier,
                String targetQualifier);
    }

    private static final class ColumnRef implements Node {
        private final boolean target;
        private final String column;

        private ColumnRef(boolean target, String column) {
            this.target = target;
            this.column = column;
        }

        @Override
        public void render(
                StringBuilder sql,
                Function<String, String> quoteIdentifier,
                String incomingQualifier,
                String targetQualifier) {
            sql.append(target ? targetQualifier : incomingQualifier)
                    .append('.')
                    .append(quoteIdentifier.apply(column));
        }
    }

    private static final class Comparison implements Node {
        private final ColumnRef left;
        private final String operator;
        private final ColumnRef right;

        private Comparison(ColumnRef left, String operator, ColumnRef right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        @Override
        public void render(
                StringBuilder sql,
                Function<String, String> quoteIdentifier,
                String incomingQualifier,
                String targetQualifier) {
            left.render(sql, quoteIdentifier, incomingQualifier, targetQualifier);
            sql.append(' ').append(operator).append(' ');
            right.render(sql, quoteIdentifier, incomingQualifier, targetQualifier);
        }
    }

    private static final class NullCheck implements Node {
        private final ColumnRef operand;
        private final boolean negated;

        private NullCheck(ColumnRef operand, boolean negated) {
            this.operand = operand;
            this.negated = negated;
        }

        @Override
        public void render(
                StringBuilder sql,
                Function<String, String> quoteIdentifier,
                String incomingQualifier,
                String targetQualifier) {
            operand.render(sql, quoteIdentifier, incomingQualifier, targetQualifier);
            sql.append(negated ? " IS NOT NULL" : " IS NULL");
        }
    }

    private static final class Logical implements Node {
        private final String operator;
        private final Node left;
        private final Node right;

        private Logical(String operator, Node left, Node right) {
            this.operator = operator;
            this.left = left;
            this.right = right;
        }

        @Override
        public void render(
                StringBuilder sql,
                Function<String, String> quoteIdentifier,
                String incomingQualifier,
                String targetQualifier) {
            sql.append('(');
            left.render(sql, quoteIdentifier, incomingQualifier, targetQualifier);
            sql.append(' ').append(operator).append(' ');
            right.render(sql, quoteIdentifier, incomingQualifier, targetQualifier);
            sql.append(')');
        }
    }

    private static final class Not implements Node {
        private final Node operand;

        private Not(Node operand) {
            this.operand = operand;
        }

        @Override
        public void render(
                StringBuilder sql,
                Function<String, String> quoteIdentifier,
                String incomingQualifier,
                String targetQualifier) {
            sql.append("(NOT ");
            operand.render(sql, quoteIdentifier, incomingQualifier, targetQualifier);
            sql.append(')');
        }
    }

    private enum TokenType {
        LEFT_PAREN,
        RIGHT_PAREN,
        COMPARISON,
        INCOMING_COLUMN,
        TARGET_COLUMN,
        AND,
        OR,
        NOT,
        IS,
        NULL,
        END
    }

    private static final class Token {
        private final TokenType type;
        private final String text;
        private final int position;

        private Token(TokenType type, String text, int position) {
            this.type = type;
            this.text = text;
            this.position = position;
        }
    }

    private static final class Parser {
        private final String input;
        private final Set<String> columnNames;
        private int position;
        private Token current;
        private int depth;

        private Parser(String input, Set<String> columnNames) {
            this.input = input;
            this.columnNames = columnNames;
        }

        private Node parse() {
            advance();
            Node node = parseOr();
            if (current.type != TokenType.END) {
                throw unexpected("end of the condition");
            }
            return node;
        }

        private Node parseOr() {
            Node node = parseAnd();
            while (current.type == TokenType.OR) {
                advance();
                node = new Logical("OR", node, parseAnd());
            }
            return node;
        }

        private Node parseAnd() {
            Node node = parseNot();
            while (current.type == TokenType.AND) {
                advance();
                node = new Logical("AND", node, parseNot());
            }
            return node;
        }

        private Node parseNot() {
            if (current.type == TokenType.NOT) {
                enterNesting();
                advance();
                Node node = new Not(parseNot());
                depth--;
                return node;
            }
            return parsePrimary();
        }

        private Node parsePrimary() {
            if (current.type == TokenType.LEFT_PAREN) {
                enterNesting();
                advance();
                Node node = parseOr();
                if (current.type != TokenType.RIGHT_PAREN) {
                    throw unexpected("')'");
                }
                advance();
                depth--;
                return node;
            }
            ColumnRef left = parseOperand();
            if (current.type == TokenType.COMPARISON) {
                String operator = current.text;
                advance();
                return new Comparison(left, operator, parseOperand());
            }
            if (current.type == TokenType.IS) {
                advance();
                boolean negated = false;
                if (current.type == TokenType.NOT) {
                    negated = true;
                    advance();
                }
                if (current.type != TokenType.NULL) {
                    throw unexpected("NULL");
                }
                advance();
                return new NullCheck(left, negated);
            }
            throw unexpected("a comparison (=, <>, <, <=, >, >=) or IS [NOT] NULL");
        }

        private ColumnRef parseOperand() {
            if (current.type != TokenType.INCOMING_COLUMN
                    && current.type != TokenType.TARGET_COLUMN) {
                throw unexpected(":column or target.column");
            }
            ColumnRef ref = new ColumnRef(current.type == TokenType.TARGET_COLUMN, current.text);
            advance();
            return ref;
        }

        private void enterNesting() {
            depth++;
            if (depth > MAX_NESTING_DEPTH) {
                throw invalid(
                        "the condition is nested more than " + MAX_NESTING_DEPTH + " levels deep");
            }
        }

        private JdbcConnectorException unexpected(String expected) {
            String found =
                    current.type == TokenType.END
                            ? "the end of the condition"
                            : String.format("'%s' at position %d", current.text, current.position);
            return invalid(String.format("expected %s but found %s", expected, found));
        }

        private void advance() {
            current = nextToken();
        }

        private Token nextToken() {
            while (position < input.length() && Character.isWhitespace(input.charAt(position))) {
                position++;
            }
            if (position >= input.length()) {
                return new Token(TokenType.END, "", position);
            }
            int start = position;
            char c = input.charAt(position);
            switch (c) {
                case '(':
                    position++;
                    return new Token(TokenType.LEFT_PAREN, "(", start);
                case ')':
                    position++;
                    return new Token(TokenType.RIGHT_PAREN, ")", start);
                case '=':
                    position++;
                    return new Token(TokenType.COMPARISON, "=", start);
                case '<':
                    position++;
                    if (peek('=') || peek('>')) {
                        position++;
                        return new Token(
                                TokenType.COMPARISON, input.substring(start, position), start);
                    }
                    return new Token(TokenType.COMPARISON, "<", start);
                case '>':
                    position++;
                    if (peek('=')) {
                        position++;
                        return new Token(TokenType.COMPARISON, ">=", start);
                    }
                    return new Token(TokenType.COMPARISON, ">", start);
                case ':':
                    position++;
                    if (peek(':')) {
                        throw invalid("casts (::) are not supported, at position " + start);
                    }
                    return new Token(TokenType.INCOMING_COLUMN, readColumn(start), start);
                case '"':
                    throw invalid(
                            String.format(
                                    "the quoted identifier at position %d must be written as"
                                            + " :\"column\" or target.\"column\"",
                                    start));
                default:
                    break;
            }
            if (isIdentifierStart(c)) {
                return readWord(start);
            }
            if (Character.isDigit(c) || c == '\'') {
                throw invalid(
                        String.format(
                                "literals are not supported, found '%s' at position %d", c, start));
            }
            throw invalid(String.format("unsupported character '%s' at position %d", c, start));
        }

        private Token readWord(int start) {
            while (position < input.length() && isIdentifierPart(input.charAt(position))) {
                position++;
            }
            String word = input.substring(start, position);
            switch (word.toUpperCase(Locale.ROOT)) {
                case "AND":
                    return new Token(TokenType.AND, word, start);
                case "OR":
                    return new Token(TokenType.OR, word, start);
                case "NOT":
                    return new Token(TokenType.NOT, word, start);
                case "IS":
                    return new Token(TokenType.IS, word, start);
                case "NULL":
                    return new Token(TokenType.NULL, word, start);
                case "TARGET":
                    if (peek('.')) {
                        position++;
                        return new Token(TokenType.TARGET_COLUMN, readColumn(start), start);
                    }
                    break;
                default:
                    break;
            }
            if (nextNonWhitespaceIs('(')) {
                throw invalid(
                        String.format(
                                "functions are not supported, found '%s' at position %d",
                                word, start));
            }
            throw invalid(
                    String.format(
                            "unsupported identifier '%s' at position %d, use :column for the"
                                    + " incoming value and target.column for the stored value",
                            word, start));
        }

        private String readColumn(int operandStart) {
            String column;
            if (peek('"')) {
                column = readQuotedIdentifier(operandStart);
            } else if (position < input.length() && isIdentifierStart(input.charAt(position))) {
                int start = position;
                while (position < input.length() && isIdentifierPart(input.charAt(position))) {
                    position++;
                }
                column = input.substring(start, position);
            } else {
                throw invalid("expected a column name at position " + position);
            }
            if (peek('.')) {
                throw invalid(
                        String.format(
                                "unsupported qualification at position %d, use :column or"
                                        + " target.column",
                                position));
            }
            if (!columnNames.contains(column)) {
                throw invalid(
                        String.format(
                                "unknown column '%s' at position %d, the sink writes the columns %s",
                                column, operandStart, columnNames));
            }
            return column;
        }

        private String readQuotedIdentifier(int operandStart) {
            position++;
            StringBuilder identifier = new StringBuilder();
            while (position < input.length()) {
                char c = input.charAt(position++);
                if (c == '"') {
                    if (peek('"')) {
                        identifier.append('"');
                        position++;
                        continue;
                    }
                    if (identifier.length() == 0) {
                        throw invalid("empty quoted identifier at position " + operandStart);
                    }
                    return identifier.toString();
                }
                identifier.append(c);
            }
            throw invalid("unterminated quoted identifier at position " + operandStart);
        }

        private boolean peek(char expected) {
            return position < input.length() && input.charAt(position) == expected;
        }

        private boolean nextNonWhitespaceIs(char expected) {
            int i = position;
            while (i < input.length() && Character.isWhitespace(input.charAt(i))) {
                i++;
            }
            return i < input.length() && input.charAt(i) == expected;
        }

        private static boolean isIdentifierStart(char c) {
            return Character.isLetter(c) || c == '_';
        }

        private static boolean isIdentifierPart(char c) {
            return Character.isLetterOrDigit(c) || c == '_' || c == '$';
        }
    }
}
