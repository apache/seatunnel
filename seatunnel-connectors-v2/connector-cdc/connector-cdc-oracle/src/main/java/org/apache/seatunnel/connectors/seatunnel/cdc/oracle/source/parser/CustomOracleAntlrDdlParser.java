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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.parser;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.TableId;

import java.util.LinkedList;
import java.util.List;

/** A ddl parser that will use custom listener. */
public class CustomOracleAntlrDdlParser extends OracleDdlParser {

    private final LinkedList<AlterTableColumnEvent> parsedEvents;

    private final TablePath tablePath;

    public CustomOracleAntlrDdlParser(TablePath tablePath) {
        super();
        this.tablePath = tablePath;
        this.parsedEvents = new LinkedList<>();
    }

    public TableId parseQualifiedTableId() {
        return new TableId(
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new CustomOracleAntlrDdlParserListener(this, parsedEvents);
    }

    public List<AlterTableColumnEvent> getAndClearParsedEvents() {
        List<AlterTableColumnEvent> result = Lists.newArrayList(parsedEvents);
        parsedEvents.clear();
        return result;
    }
}
