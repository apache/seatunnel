/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.mysql.antlr;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.WritableToken;

import io.debezium.ddl.parser.mysql.generated.MySqlLexer;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/**
 * Lets the Debezium 1.9.8 MySQL grammar parse the MySQL 9.0 {@code VECTOR} column type.
 *
 * <p>That grammar has no {@code VECTOR} token, and the Debezium 3.x parser jar that has one targets
 * Java 17, so it cannot run on the Java 11 baseline. {@code VECTOR} therefore lexes as a plain
 * identifier and the whole statement fails to parse. This lexer retypes the identifier to {@code
 * VARBINARY}, whose grammar rule takes the same optional {@code (n)}. Only the token type changes:
 * the token text stays {@code VECTOR}, so the resolved column keeps its real type name while
 * carrying {@link java.sql.Types#VARBINARY}, which is how the value is delivered.
 *
 * <p>Retyping must not touch identifiers that merely happen to be named {@code vector}, such as a
 * column, table or index. It is limited to CREATE and ALTER statements, before any SELECT, and to a
 * token that follows a column name and is followed by a token that can end a column type.
 *
 * <p>The following token is found with this lexer's own rules, so whitespace, block comments, line
 * comments and executable comments between {@code VECTOR} and its dimension or attributes are
 * skipped exactly as the grammar skips them. Tokens lexed ahead are buffered and returned in order.
 */
public class MySqlVectorTypeLexer extends MySqlLexer {

    private static final String VECTOR = "VECTOR";

    /** Tokens after which {@code vector} is a name being declared or referenced, never a type. */
    private static final Set<Integer> NAME_PREDECESSORS =
            new HashSet<>(
                    Arrays.asList(
                            Token.INVALID_TYPE,
                            LR_BRACKET,
                            COMMA,
                            COLUMN,
                            DROP,
                            TO,
                            AS,
                            TABLE,
                            EXISTS,
                            REFERENCES,
                            ON,
                            INDEX,
                            KEY,
                            UNIQUE,
                            FULLTEXT,
                            SPATIAL,
                            CONSTRAINT,
                            LIKE,
                            MODIFY,
                            CHANGE,
                            AFTER,
                            RENAME));

    /**
     * Tokens that can directly follow a column type, with or without a {@code VECTOR} dimension.
     */
    private static final Set<Integer> COLUMN_TYPE_FOLLOWERS =
            new HashSet<>(
                    Arrays.asList(
                            Token.EOF,
                            LR_BRACKET,
                            RR_BRACKET,
                            COMMA,
                            SEMI,
                            NOT,
                            NULL_LITERAL,
                            NULL_SPEC_LITERAL,
                            DEFAULT,
                            COMMENT,
                            VISIBLE,
                            INVISIBLE,
                            FIRST,
                            AFTER));

    /** Tokens lexed ahead of the parser, of every channel, in source order. */
    private final Deque<Token> lookahead = new ArrayDeque<>();

    /*
     * Statement state, updated only from default-channel tokens as they are handed to the parser.
     * statementFirstTokenType is the first such token after the start or after a SEMI, and selects
     * the statements in which VECTOR may be a type (CREATE, ALTER). selectSeen turns retyping off
     * for the rest of the statement, since a SELECT (view or CREATE ... AS SELECT) only references
     * names. previousTokenType is the last such token and tells a column name (ID, quoted ID or a
     * keyword used as a name) apart from a position where vector can only be a name. A SEMI resets
     * all three, so a multi-statement string is classified per statement.
     */
    private int statementFirstTokenType = Token.INVALID_TYPE;
    private int previousTokenType = Token.INVALID_TYPE;
    private boolean selectSeen;

    public MySqlVectorTypeLexer(CharStream input) {
        super(input);
    }

    @Override
    public Token nextToken() {
        Token token = lookahead.isEmpty() ? super.nextToken() : lookahead.poll();
        if (token.getChannel() != Token.DEFAULT_CHANNEL) {
            return token;
        }
        if (isVectorColumnType(token)) {
            ((WritableToken) token).setType(VARBINARY);
        }
        trackStatement(token);
        return token;
    }

    private boolean isVectorColumnType(Token token) {
        return token.getType() == ID
                && VECTOR.equalsIgnoreCase(token.getText())
                && (statementFirstTokenType == CREATE || statementFirstTokenType == ALTER)
                && !selectSeen
                && !NAME_PREDECESSORS.contains(previousTokenType)
                && COLUMN_TYPE_FOLLOWERS.contains(peekDefaultChannelToken().getType());
    }

    private void trackStatement(Token token) {
        int tokenType = token.getType();
        if (tokenType == SEMI) {
            statementFirstTokenType = Token.INVALID_TYPE;
            previousTokenType = Token.INVALID_TYPE;
            selectSeen = false;
            return;
        }
        if (statementFirstTokenType == Token.INVALID_TYPE) {
            statementFirstTokenType = tokenType;
        }
        selectSeen = selectSeen || tokenType == SELECT;
        previousTokenType = tokenType;
    }

    /**
     * Returns the next token the parser will see, lexing and buffering hidden and comment tokens on
     * the way. The returned token stays buffered and is handed out later by {@link #nextToken()}.
     */
    private Token peekDefaultChannelToken() {
        for (Token buffered : lookahead) {
            if (buffered.getChannel() == Token.DEFAULT_CHANNEL) {
                return buffered;
            }
        }
        while (true) {
            Token next = super.nextToken();
            lookahead.add(next);
            if (next.getChannel() == Token.DEFAULT_CHANNEL || next.getType() == Token.EOF) {
                return next;
            }
        }
    }
}
