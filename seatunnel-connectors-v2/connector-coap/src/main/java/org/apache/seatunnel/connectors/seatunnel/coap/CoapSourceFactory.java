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

package org.apache.seatunnel.connectors.seatunnel.coap;

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

/** Registers the UDP listener and its source-only options with the factory SPI. */
@AutoService(Factory.class)
public final class CoapSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return CoapOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        CoapOptions.BIND_HOST,
                        CoapOptions.PORT,
                        CoapOptions.RESOURCE,
                        CoapOptions.FORMAT,
                        CoapOptions.BODY_SIZE,
                        CoapOptions.QUEUE,
                        CoapOptions.PEERS,
                        CoapOptions.CONCURRENCY,
                        CoapOptions.BLOCK_TIMEOUT,
                        CoapOptions.LIMIT,
                        CoapOptions.CSV_DELIMITER,
                        CoapOptions.AVRO_SCHEMA,
                        CoapOptions.PROTOBUF_SCHEMA,
                        CoapOptions.PROTOBUF_MESSAGE,
                        CoapOptions.XML_PATH,
                        CoapOptions.XML_NAMESPACES,
                        CoapOptions.XML_PRESERVE_ROOT,
                        CoapOptions.RECORD_HEADER_FIELD,
                        CoapOptions.LOG_MODE,
                        CoapOptions.LOG_PATTERN,
                        CoapOptions.LOG_GROUPS,
                        CoapOptions.GROK_DEFINITIONS,
                        CoapOptions.DATAGRAM_MODE,
                        CoapOptions.DATAGRAM_SEPARATOR,
                        CoapOptions.NETFLOW_TEMPLATES,
                        CoapOptions.NETFLOW_TEMPLATE_TIMEOUT,
                        CoapOptions.COLLECTD_USER,
                        CoapOptions.COLLECTD_PASSWORD,
                        CoapOptions.COLLECTD_TYPES,
                        CoapOptions.COLLECTD_CONVERT_TIME,
                        CoapOptions.COLLECTD_OMIT_INTERVAL,
                        ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        CoapSource source = new CoapSource(new CoapSettings.Listener(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CoapSource.class;
    }
}
