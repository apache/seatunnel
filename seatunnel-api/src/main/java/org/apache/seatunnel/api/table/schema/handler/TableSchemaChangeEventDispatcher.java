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

package org.apache.seatunnel.api.table.schema.handler;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TableSchemaChangeEventDispatcher implements TableSchemaChangeEventHandler {

    private final Map<Class, TableSchemaChangeEventHandler> handlers;
    private TableSchema schema;

    public TableSchemaChangeEventDispatcher() {
        this.handlers = createHandlers();
    }

    @Override
    public TableSchema get() {
        return schema;
    }

    @Override
    public TableSchemaChangeEventHandler reset(TableSchema schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public TableSchema apply(SchemaChangeEvent event) {
        TableSchemaChangeEventHandler handler = handlers.get(event.getClass());
        if (handler == null) {
            log.warn("Not found handler for event: {}", event.getClass());
            return schema;
        }
        return handler.reset(schema).apply(event);
    }

    private static Map<Class, TableSchemaChangeEventHandler> createHandlers() {
        Map<Class, TableSchemaChangeEventHandler> handlers = new HashMap<>();

        AlterTableSchemaEventHandler alterTableEventHandler = new AlterTableSchemaEventHandler();
        handlers.put(AlterTableEvent.class, alterTableEventHandler);
        handlers.put(AlterTableNameEvent.class, alterTableEventHandler);
        handlers.put(AlterTableColumnsEvent.class, alterTableEventHandler);
        handlers.put(AlterTableAddColumnEvent.class, alterTableEventHandler);
        handlers.put(AlterTableModifyColumnEvent.class, alterTableEventHandler);
        handlers.put(AlterTableDropColumnEvent.class, alterTableEventHandler);
        handlers.put(AlterTableChangeColumnEvent.class, alterTableEventHandler);
        return handlers;
    }
}
