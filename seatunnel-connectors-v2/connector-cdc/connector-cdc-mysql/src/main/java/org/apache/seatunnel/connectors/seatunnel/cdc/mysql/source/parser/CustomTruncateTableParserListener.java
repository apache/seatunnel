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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.parser;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.operation.event.TruncateTableEvent;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.TableId;

import java.util.LinkedList;

/** Parses {@code TRUNCATE TABLE} into a {@link TruncateTableEvent}. */
public class CustomTruncateTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;
    private final LinkedList<TruncateTableEvent> parsedTableOperations;

    public CustomTruncateTableParserListener(
            MySqlAntlrDdlParser parser, LinkedList<TruncateTableEvent> parsedTableOperations) {
        this.parser = parser;
        this.parsedTableOperations = parsedTableOperations;
    }

    @Override
    public void enterTruncateTable(MySqlParser.TruncateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        parsedTableOperations.add(
                TruncateTableEvent.of(
                        TableIdentifier.of(
                                "", tableId.catalog(), tableId.schema(), tableId.table())));
        super.enterTruncateTable(ctx);
    }
}
