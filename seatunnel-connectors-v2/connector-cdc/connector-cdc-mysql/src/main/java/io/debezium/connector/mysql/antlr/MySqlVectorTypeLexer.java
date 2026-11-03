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
 * column, table, index or database, or a column used in an expression. It is therefore positive:
 * {@code vector} is retyped only in CREATE and ALTER statements, before any SELECT, when it stands
 * where a column definition puts its type, and when the next token can end a column type. The
 * positions are {@code ( name VECTOR}, {@code , name VECTOR}, {@code ADD name VECTOR}, {@code
 * [ADD|MODIFY] COLUMN name VECTOR} and {@code CHANGE [COLUMN] old_name new_name VECTOR}.
 *
 * <p>{@link MySqlAntlrDdlParser#parse} only uses this lexer for a statement that fails to parse
 * without it, so the position rules can never change how valid pre-9.0 DDL is parsed.
 *
 * <p>The following token is found with this lexer's own rules, so whitespace, block comments, line
 * comments and executable comments between {@code VECTOR} and its dimension or attributes are
 * skipped exactly as the grammar skips them. Tokens lexed ahead are buffered and returned in order.
 */
public class MySqlVectorTypeLexer extends MySqlLexer {

    private static final String VECTOR = "VECTOR";

    /** Tokens directly before the column name of a column definition, other than CHANGE. */
    private static final Set<Integer> COLUMN_NAME_PREDECESSORS =
            new HashSet<>(Arrays.asList(LR_BRACKET, COMMA, ADD, COLUMN, MODIFY));

    /**
     * Keywords that start an index, key, constraint, partition or alter clause. They are words but
     * never a column name in the positions above (the reserved ones cannot be unquoted names at
     * all), so {@code KEY vector (a)}, {@code , DROP vector} or {@code (LIKE vector)} keep {@code
     * vector} as a name. Any other keyword counts as a name, because MySQL accepts many keywords as
     * unquoted column names, for example {@code data VECTOR(3)}.
     */
    private static final Set<Integer> NON_NAME_KEYWORDS =
            new HashSet<>(
                    Arrays.asList(
                            COLUMN,
                            KEY,
                            INDEX,
                            UNIQUE,
                            PRIMARY,
                            FOREIGN,
                            FULLTEXT,
                            SPATIAL,
                            CONSTRAINT,
                            CHECK,
                            PARTITION,
                            TO,
                            AS,
                            LIKE,
                            ADD,
                            ALTER,
                            CHANGE,
                            DROP,
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
     * names. recentTokenTypes and recentTokenIsName hold the last four such tokens, most recent at
     * index 0, which is enough to recognise the longest position, CHANGE COLUMN old new VECTOR. A
     * SEMI resets everything, so a multi-statement string is classified per statement.
     */
    private static final int RECENT_TOKENS = 4;

    private int statementFirstTokenType = Token.INVALID_TYPE;
    private boolean selectSeen;
    private final int[] recentTokenTypes = new int[RECENT_TOKENS];
    private final boolean[] recentTokenIsName = new boolean[RECENT_TOKENS];

    public MySqlVectorTypeLexer(CharStream input) {
        super(input);
        resetStatement();
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
                && followsColumnName()
                && COLUMN_TYPE_FOLLOWERS.contains(peekDefaultChannelToken().getType());
    }

    /**
     * True when the previous tokens are a column name in one of the column definition positions.
     */
    private boolean followsColumnName() {
        if (!recentTokenIsName[0]) {
            return false;
        }
        if (COLUMN_NAME_PREDECESSORS.contains(recentTokenTypes[1])) {
            return true;
        }
        // CHANGE [COLUMN] old_name new_name VECTOR
        return recentTokenIsName[1]
                && (recentTokenTypes[2] == CHANGE
                        || (recentTokenTypes[2] == COLUMN && recentTokenTypes[3] == CHANGE));
    }

    private void trackStatement(Token token) {
        int tokenType = token.getType();
        if (tokenType == SEMI) {
            resetStatement();
            return;
        }
        if (statementFirstTokenType == Token.INVALID_TYPE) {
            statementFirstTokenType = tokenType;
        }
        selectSeen = selectSeen || tokenType == SELECT;
        System.arraycopy(recentTokenTypes, 0, recentTokenTypes, 1, RECENT_TOKENS - 1);
        System.arraycopy(recentTokenIsName, 0, recentTokenIsName, 1, RECENT_TOKENS - 1);
        recentTokenTypes[0] = tokenType;
        recentTokenIsName[0] = isName(token);
    }

    private void resetStatement() {
        statementFirstTokenType = Token.INVALID_TYPE;
        selectSeen = false;
        Arrays.fill(recentTokenTypes, Token.INVALID_TYPE);
        Arrays.fill(recentTokenIsName, false);
    }

    /**
     * An identifier, a back-quoted identifier, or a keyword that MySQL accepts as a column name.
     */
    private static boolean isName(Token token) {
        int tokenType = token.getType();
        if (tokenType == ID || tokenType == REVERSE_QUOTE_ID) {
            return true;
        }
        if (NON_NAME_KEYWORDS.contains(tokenType)) {
            return false;
        }
        String text = token.getText();
        if (text.isEmpty() || !Character.isLetter(text.charAt(0))) {
            return false;
        }
        for (int i = 1; i < text.length(); i++) {
            char c = text.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                return false;
            }
        }
        return true;
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
