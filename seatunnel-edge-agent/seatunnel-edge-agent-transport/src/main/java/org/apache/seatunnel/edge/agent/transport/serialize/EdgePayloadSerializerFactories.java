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

package org.apache.seatunnel.edge.agent.transport.serialize;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportOptions;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketMode;

import java.util.Locale;
import java.util.Objects;

public class EdgePayloadSerializerFactories {

    public static PayloadSerializer create(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        String modeId =
                EdgePacketMode.from(config.get(EdgeTransportOptions.PACKET_MODE))
                        .name()
                        .toLowerCase(Locale.ROOT);
        EdgePayloadSerializerFactory factory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        EdgePayloadSerializerFactory.class,
                        modeId);
        return factory.create(config);
    }
}
