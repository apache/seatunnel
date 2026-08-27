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

import io.debezium.connector.mysql.antlr.listener.DefaultValueParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.relational.ColumnEditor;

import java.util.concurrent.atomic.AtomicReference;

public class CustomDefaultValueParserListener extends DefaultValueParserListener {

    private final ColumnEditor columnEditor;

    public CustomDefaultValueParserListener(
            ColumnEditor columnEditor, AtomicReference<Boolean> optionalColumn) {
        super(columnEditor, optionalColumn);
        this.columnEditor = columnEditor;
    }

    @Override
    public void enterDefaultValue(MySqlParser.DefaultValueContext ctx) {
        if (ctx.currentTimestamp() != null && !ctx.currentTimestamp().isEmpty()) {
            if (ctx.currentTimestamp().size() > 1 || (ctx.ON() == null && ctx.UPDATE() == null)) {
                final MySqlParser.CurrentTimestampContext currentTimestamp =
                        ctx.currentTimestamp(0);
                columnEditor.defaultValueExpression(currentTimestamp.getText());
            }
        } else {
            super.enterDefaultValue(ctx);
        }
    }
}
