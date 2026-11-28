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

package org.apache.seatunnel.connectors.seatunnel.udp;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.BIND_HOST;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.IDLE_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.MAX_DATAGRAMS_PER_POLL;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.MAX_DATAGRAM_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.MAX_RECORDS_PER_DATAGRAM;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.ON_MALFORMED;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.PORTS;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.RECEIVE_BUFFER_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.udp.UdpSourceOptions.SEPARATOR;

/** Creates a UDP listener source and exposes its configuration through the Factory SPI. */
@AutoService(Factory.class)
public final class UdpSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return UdpSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(PORTS)
                .optional(
                        BIND_HOST,
                        FORMAT,
                        SEPARATOR,
                        MAX_DATAGRAM_BYTES,
                        RECEIVE_BUFFER_BYTES,
                        MAX_DATAGRAMS_PER_POLL,
                        MAX_RECORDS_PER_DATAGRAM,
                        IDLE_TIMEOUT_MS,
                        ON_MALFORMED)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        UdpSource source = new UdpSource(new UdpConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return UdpSource.class;
    }
}
