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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
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
 * token that follows a column name and is followed by something that can end a column type.
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

    /** Words that can directly follow a column type when {@code VECTOR} has no dimension. */
    private static final Set<String> COLUMN_ATTRIBUTES =
            new HashSet<>(
                    Arrays.asList(
                            "NOT",
                            "NULL",
                            "DEFAULT",
                            "COMMENT",
                            "VISIBLE",
                            "INVISIBLE",
                            "FIRST",
                            "AFTER"));

    private int statementFirstTokenType = Token.INVALID_TYPE;
    private int previousTokenType = Token.INVALID_TYPE;
    private boolean selectSeen;

    public MySqlVectorTypeLexer(CharStream input) {
        super(input);
    }

    @Override
    public Token nextToken() {
        Token token = super.nextToken();
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
                && isFollowedByColumnTypeEnd();
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

    /** Peeks at the unread input; the token stream position is not moved. */
    private boolean isFollowedByColumnTypeEnd() {
        int offset = 1;
        while (Character.isWhitespace(_input.LA(offset))) {
            offset++;
        }
        int next = _input.LA(offset);
        if (next == CharStream.EOF || next == '(' || next == ')' || next == ',' || next == ';') {
            return true;
        }
        StringBuilder word = new StringBuilder();
        while (Character.isLetter(_input.LA(offset))) {
            word.append((char) _input.LA(offset));
            offset++;
        }
        return COLUMN_ATTRIBUTES.contains(word.toString().toUpperCase(Locale.ROOT));
    }
}
