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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.ALLOW_INSECURE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.ALLOW_LEGACY_SECURITY;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.APPLICATION_URI;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.BROWSE_PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.BROWSE_ROOT;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.CLIENT_KEY_ALIAS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.CLIENT_KEY_STORE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_ARRAY_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_CHUNK_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_CHUNK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_DEPTH;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_MESSAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_NODES;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_RECORDS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MAX_STRING_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.MESSAGE_SECURITY_MODE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.NODES;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.NODE_FETCH_MODE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.NODE_FILE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.QUEUE_CAPACITY;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.READ_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.READ_MODE;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.REFRESH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SECURITY_POLICY;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SERVER_APPLICATION_URI;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SERVER_CERTIFICATE_SHA256;
import static org.apache.seatunnel.connectors.seatunnel.opcua.OpcUaSourceOptions.SESSION_TIMEOUT_MS;

/** Publishes the OpcUa source and all supported options through the Factory SPI. */
@AutoService(Factory.class)
public final class OpcUaSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return OpcUaSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ENDPOINT)
                .optional(
                        READ_MODE,
                        NODE_FETCH_MODE,
                        NODES,
                        NODE_FILE,
                        BROWSE_ROOT,
                        BROWSE_PAGE_SIZE,
                        MAX_NODES,
                        MAX_DEPTH,
                        INTERVAL_MS,
                        REFRESH_INTERVAL_MS,
                        READ_BATCH_SIZE,
                        QUEUE_CAPACITY,
                        MAX_RECORDS,
                        REQUEST_TIMEOUT_MS,
                        SESSION_TIMEOUT_MS,
                        SECURITY_POLICY,
                        MESSAGE_SECURITY_MODE,
                        ALLOW_INSECURE,
                        ALLOW_LEGACY_SECURITY,
                        APPLICATION_URI,
                        SERVER_APPLICATION_URI,
                        SERVER_CERTIFICATE_SHA256,
                        CLIENT_KEY_STORE,
                        CLIENT_KEY_ALIAS,
                        PASSWORD,
                        MAX_CHUNK_SIZE,
                        MAX_CHUNK_COUNT,
                        MAX_MESSAGE_SIZE,
                        MAX_ARRAY_LENGTH,
                        MAX_STRING_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        OpcUaSource source = new OpcUaSource(new OpcUaSettings(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OpcUaSource.class;
    }
}
