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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.SchemaChanges;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.CaseChangingCharStream;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.ParsingErrorListener;
import io.debezium.relational.TableEditor;
import io.debezium.relational.ddl.AbstractDdlParser;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.Collection;

@RequiredArgsConstructor
public abstract class AntlrDdlParser<L extends Lexer, P extends Parser> implements Serializable {

    private final String terminator;
    private final boolean throwErrorsFromTreeWalk;
    @Getter private DataTypeResolver dataTypeResolver;
    private AntlrDdlParserListener antlrDdlParserListener;
    private String currentSchema = null;
    @Getter private SchemaChanges schemaChanges = new SchemaChanges();
    @Getter private TableEditor tableEditor;

    public AntlrDdlParser(boolean throwErrorsFromTreeWalk) {
        this(";", throwErrorsFromTreeWalk);
    }

    public void reset() {
        schemaChanges.reset();
        tableEditor = null;
    }

    public void parse(String ddlContent, TableEditor tableEditor) {
        this.tableEditor = tableEditor;

        CodePointCharStream ddlContentCharStream = CharStreams.fromString(ddlContent);
        L lexer = createNewLexerInstance(new CaseChangingCharStream(ddlContentCharStream, true));
        P parser = createNewParserInstance(new CommonTokenStream(lexer));

        dataTypeResolver = initializeDataTypeResolver();

        // remove default console output printing error listener
        parser.removeErrorListener(ConsoleErrorListener.INSTANCE);

        ParsingErrorListener parsingErrorListener =
                new ParsingErrorListener(ddlContent, AbstractDdlParser::accumulateParsingFailure);
        parser.addErrorListener(parsingErrorListener);

        ParseTree parseTree = parseTree(parser);

        if (parsingErrorListener.getErrors().isEmpty()) {
            antlrDdlParserListener = createParseTreeWalkerListener();
            if (antlrDdlParserListener != null) {
                ParseTreeWalker.DEFAULT.walk(antlrDdlParserListener, parseTree);

                if (throwErrorsFromTreeWalk && !antlrDdlParserListener.getErrors().isEmpty()) {
                    throwParsingException(antlrDdlParserListener.getErrors());
                }
            }
        } else {
            throwParsingException(parsingErrorListener.getErrors());
        }
    }

    protected abstract L createNewLexerInstance(CharStream charStream);

    protected abstract P createNewParserInstance(CommonTokenStream commonTokenStream);

    protected abstract AntlrDdlParserListener createParseTreeWalkerListener();

    protected abstract ParseTree parseTree(P parser);

    protected abstract DataTypeResolver initializeDataTypeResolver();

    private void throwParsingException(Collection<ParsingException> errors) {
        if (errors.size() == 1) {
            throw errors.iterator().next();
        } else {
            throw new MultipleParsingExceptions(errors);
        }
    }

    protected static boolean isQuote(char c) {
        return c == '\'' || c == '"' || c == '`';
    }

    protected TablePath resolveTablePath(String schemaName, String tableName) {
        return TablePath.of(schemaName, tableName);
    }

    public void setCurrentDatabase(String databaseName) {
        this.currentSchema = databaseName;
    }

    public void setCurrentSchema(String schemaName) {
        this.currentSchema = schemaName;
    }

    public String currentSchema() {
        return currentSchema;
    }

    protected String withoutQuotes(ParserRuleContext ctx) {
        return withoutQuotes(ctx.getText());
    }

    public static String withoutQuotes(String possiblyQuoted) {
        return isQuoted(possiblyQuoted)
                ? possiblyQuoted.substring(1, possiblyQuoted.length() - 1)
                : possiblyQuoted;
    }

    public static boolean isQuoted(String possiblyQuoted) {
        if (possiblyQuoted.length() < 2) {
            // Too short to be quoted ...
            return false;
        }
        if (possiblyQuoted.startsWith("`") && possiblyQuoted.endsWith("`")) {
            return true;
        }
        if (possiblyQuoted.startsWith("'") && possiblyQuoted.endsWith("'")) {
            return true;
        }
        if (possiblyQuoted.startsWith("\"") && possiblyQuoted.endsWith("\"")) {
            return true;
        }
        return false;
    }

    public static String getText(ParserRuleContext ctx) {
        return getText(ctx, ctx.start.getStartIndex(), ctx.stop.getStopIndex());
    }

    public static String getText(ParserRuleContext ctx, int start, int stop) {
        Interval interval = new Interval(start, stop);
        return ctx.start.getInputStream().getText(interval);
    }
}
