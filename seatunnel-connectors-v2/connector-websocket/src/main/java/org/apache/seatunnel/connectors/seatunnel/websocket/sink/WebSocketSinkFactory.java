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

package org.apache.seatunnel.connectors.seatunnel.websocket.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.CLOSE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.FIELD;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.FLUSH_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.MAX_FRAME_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.MAX_QUEUE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkOptions.WRITE_TIMEOUT;

@AutoService(Factory.class)
public final class WebSocketSinkFactory
        implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {
    static final String IDENTIFIER = "WebSocket";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL)
                .optional(
                        FORMAT,
                        FIELD,
                        TOKEN,
                        CONNECT_TIMEOUT,
                        WRITE_TIMEOUT,
                        FLUSH_TIMEOUT,
                        CLOSE_TIMEOUT,
                        MAX_FRAME_BYTES,
                        MAX_QUEUE_BYTES)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, Void, Void, Void> createSink(TableSinkFactoryContext context) {
        return () ->
                new WebSocketSink(
                        context.getOptions(), context.getCatalogTable().getSeaTunnelRowType());
    }
}
