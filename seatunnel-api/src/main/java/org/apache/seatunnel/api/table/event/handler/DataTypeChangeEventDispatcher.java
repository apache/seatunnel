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

package org.apache.seatunnel.api.table.event.handler;

import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DataTypeChangeEventDispatcher implements DataTypeChangeEventHandler {

    private final Map<Class, DataTypeChangeEventHandler> handlers;
    private SeaTunnelRowType dataType;

    public DataTypeChangeEventDispatcher() {
        this.handlers = createHandlers();
    }

    @Override
    public SeaTunnelRowType get() {
        return dataType;
    }

    @Override
    public DataTypeChangeEventHandler reset(SeaTunnelRowType dataType) {
        this.dataType = dataType;
        return this;
    }

    @Override
    public SeaTunnelRowType apply(SchemaChangeEvent event) {
        DataTypeChangeEventHandler handler = handlers.get(event.getClass());
        if (handler == null) {
            log.warn("No DataTypeChangeEventHandler for event: {}", event.getClass());
            return dataType;
        }
        return handler.reset(dataType).apply(event);
    }

    private static Map<Class, DataTypeChangeEventHandler> createHandlers() {
        Map<Class, DataTypeChangeEventHandler> handlers = new HashMap<>();

        AlterTableEventHandler alterTableEventHandler = new AlterTableEventHandler();
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
