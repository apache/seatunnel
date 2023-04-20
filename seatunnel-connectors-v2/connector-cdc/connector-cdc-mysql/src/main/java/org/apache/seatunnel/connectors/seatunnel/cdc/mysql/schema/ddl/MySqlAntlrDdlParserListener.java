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

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.listener.AlterTableParserListener;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.ProxyParseTreeListenerUtil;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.text.ParsingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MySqlAntlrDdlParserListener extends MySqlParserBaseListener
        implements AntlrDdlParserListener {
    private final Collection<ParsingException> errors = new ArrayList<>();
    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();
    private boolean skipNodes;
    private int skippedNodesCount = 0;

    public MySqlAntlrDdlParserListener(MySqlAntlrDdlParser parser) {
        // initialize listeners
        listeners.add(new AlterTableParserListener(parser, listeners));
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
                // back in the node where skipping started
                skipNodes = false;
            } else {
                // going up in a tree, means decreasing a number of skipped nodes
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
        // this is a grammar rule for BEGIN ... END part of statements. Skip it.
        skipNodes = true;
    }
}
