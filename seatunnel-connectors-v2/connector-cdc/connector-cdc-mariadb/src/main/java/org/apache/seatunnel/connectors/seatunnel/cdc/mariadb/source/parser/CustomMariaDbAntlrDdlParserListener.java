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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.parser;

import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.ProxyParseTreeListenerUtil;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.text.ParsingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** MariaDB listener for parsing DDL statements. */
public class CustomMariaDbAntlrDdlParserListener extends MySqlParserBaseListener
        implements AntlrDdlParserListener {

    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();
    private boolean skipNodes;
    private int skippedNodesCount = 0;
    private final Collection<ParsingException> errors = new ArrayList<>();

    public CustomMariaDbAntlrDdlParserListener(
            RelationalDatabaseConnectorConfig dbzConnectorConfig,
            MySqlAntlrDdlParser parser,
            LinkedList<AlterTableEvent> parsedEvents) {
        listeners.add(
                new CustomMariaDbAlterTableParserListener(
                        dbzConnectorConfig, parser, listeners, parsedEvents));
    }

    @Override
    public Collection<ParsingException> getErrors() {
        return errors;
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        if (skipNodes) {
            skippedNodesCount++;
        } else {
            ProxyParseTreeListenerUtil.delegateEnterRule(ctx, listeners, errors);
        }
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (skipNodes) {
            if (skippedNodesCount == 0) {
                skipNodes = false;
            } else {
                skippedNodesCount--;
            }
        } else {
            ProxyParseTreeListenerUtil.delegateExitRule(ctx, listeners, errors);
        }
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        ProxyParseTreeListenerUtil.visitErrorNode(node, listeners, errors);
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        ProxyParseTreeListenerUtil.visitTerminal(node, listeners, errors);
    }

    @Override
    public void enterRoutineBody(MySqlParser.RoutineBodyContext ctx) {
        skipNodes = true;
    }
}
