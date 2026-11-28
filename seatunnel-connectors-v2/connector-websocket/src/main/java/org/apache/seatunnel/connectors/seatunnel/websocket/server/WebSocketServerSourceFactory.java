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

package org.apache.seatunnel.connectors.seatunnel.websocket.server;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/**
 * Registers a separate inbound identity in the existing artifact. The source owns its complete
 * produced catalog table; factory creation neither opens a socket nor changes client registration.
 */
@AutoService(Factory.class)
public final class WebSocketServerSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return WebSocketServerSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(WebSocketServerSourceOptions.PORT)
                .optional(
                        WebSocketServerSourceOptions.BIND_HOST,
                        WebSocketServerSourceOptions.PATH,
                        WebSocketServerSourceOptions.EXPECTED_HOST,
                        WebSocketServerSourceOptions.ALLOWED_ORIGINS,
                        WebSocketServerSourceOptions.TOKEN,
                        WebSocketServerSourceOptions.KEYSTORE_PATH,
                        WebSocketServerSourceOptions.KEYSTORE_TYPE,
                        WebSocketServerSourceOptions.PASSWORD,
                        WebSocketServerSourceOptions.FORMAT,
                        WebSocketServerSourceOptions.FIELD_DELIMITER,
                        WebSocketServerSourceOptions.MAX_CONNECTIONS,
                        WebSocketServerSourceOptions.MAX_MESSAGE_BYTES,
                        WebSocketServerSourceOptions.MAX_DECODED_UNITS,
                        WebSocketServerSourceOptions.MAX_QUEUE_MESSAGES,
                        WebSocketServerSourceOptions.MAX_QUEUE_BYTES,
                        WebSocketServerSourceOptions.HANDSHAKE_TIMEOUT_MS,
                        WebSocketServerSourceOptions.IDLE_TIMEOUT_MS,
                        WebSocketServerSourceOptions.MESSAGE_TIMEOUT_MS,
                        ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new WebSocketServerSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return WebSocketServerSource.class;
    }
}
