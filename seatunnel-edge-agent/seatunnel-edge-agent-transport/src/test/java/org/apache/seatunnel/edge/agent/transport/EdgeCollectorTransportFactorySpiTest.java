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

package org.apache.seatunnel.edge.agent.transport;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryException;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.edge.agent.transport.config.EdgeOutputOptions;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportOptions;
import org.apache.seatunnel.edge.agent.transport.console.ConsoleCollectorTransport;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketMode;
import org.apache.seatunnel.edge.agent.transport.serialize.PacketPayloadSerializer;
import org.apache.seatunnel.edge.agent.transport.serialize.RawPayloadSerializer;
import org.apache.seatunnel.edge.agent.transport.socket.EdgeTransportClient;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class EdgeCollectorTransportFactorySpiTest {

    private static final ClassLoader CLASS_LOADER = Thread.currentThread().getContextClassLoader();

    @Test
    void discoversTransportAndConsoleFactories() {
        Map<String, Object> transportMap = new HashMap<>();
        transportMap.put(EdgeOutputOptions.TYPE.key(), "transport");
        transportMap.put(EdgeTransportOptions.ENDPOINT.key(), "localhost:1");
        transportMap.put(EdgeTransportOptions.TOKEN.key(), "tok");

        EdgeCollectorTransportFactory factory =
                FactoryUtil.discoverFactory(
                        CLASS_LOADER, EdgeCollectorTransportFactory.class, "transport");
        Assertions.assertInstanceOf(
                EdgeTransportClient.class, factory.create(ReadonlyConfig.fromMap(transportMap)));

        EdgeCollectorTransportFactory consoleFactory =
                FactoryUtil.discoverFactory(
                        CLASS_LOADER, EdgeCollectorTransportFactory.class, "console");
        Map<String, Object> consoleMap = new HashMap<>();
        consoleMap.put(EdgeOutputOptions.TYPE.key(), "console");
        Assertions.assertInstanceOf(
                ConsoleCollectorTransport.class,
                consoleFactory.create(ReadonlyConfig.fromMap(consoleMap)));
    }

    @Test
    void transportFactoryUsesPacketSerializerWhenPacketModeEnabled() {
        Map<String, Object> transportMap = new HashMap<>();
        transportMap.put(EdgeOutputOptions.TYPE.key(), "transport");
        transportMap.put(EdgeTransportOptions.ENDPOINT.key(), "localhost:1");
        transportMap.put(EdgeTransportOptions.TOKEN.key(), "tok");
        transportMap.put(EdgeTransportOptions.PACKET_MODE.key(), EdgePacketMode.PACKET.name());

        EdgeCollectorTransportFactory factory =
                FactoryUtil.discoverFactory(
                        CLASS_LOADER, EdgeCollectorTransportFactory.class, "transport");
        ReadonlyConfig config = ReadonlyConfig.fromMap(transportMap);
        Assertions.assertInstanceOf(
                PacketPayloadSerializer.class, factory.payloadSerializer(config));
    }

    @Test
    void consoleFactoryUsesRawPayloadSerializer() {
        EdgeCollectorTransportFactory consoleFactory =
                FactoryUtil.discoverFactory(
                        CLASS_LOADER, EdgeCollectorTransportFactory.class, "console");
        Map<String, Object> consoleMap = new HashMap<>();
        consoleMap.put(EdgeOutputOptions.TYPE.key(), "console");
        Assertions.assertInstanceOf(
                RawPayloadSerializer.class,
                consoleFactory.payloadSerializer(ReadonlyConfig.fromMap(consoleMap)));
    }

    @Test
    void rejectsUnknownOutputType() {
        FactoryException ex =
                Assertions.assertThrows(
                        FactoryException.class,
                        () ->
                                FactoryUtil.discoverFactory(
                                        CLASS_LOADER,
                                        EdgeCollectorTransportFactory.class,
                                        "unknown"));

        Assertions.assertTrue(
                ex.getMessage().contains("unknown")
                        || ex.getMessage().contains("Could not find any factory"));
    }
}
