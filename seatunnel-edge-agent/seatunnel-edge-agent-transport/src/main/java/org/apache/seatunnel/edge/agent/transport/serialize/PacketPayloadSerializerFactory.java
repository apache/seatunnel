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
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportOptions;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketCompressionType;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketEncryptionType;

import com.google.auto.service.AutoService;

import java.util.Base64;
import java.util.Objects;

@AutoService(Factory.class)
public class PacketPayloadSerializerFactory implements EdgePayloadSerializerFactory {

    @Override
    public String factoryIdentifier() {
        return "packet";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        EdgeTransportOptions.PACKET_MODE,
                        EdgeTransportOptions.COMPRESSION,
                        EdgeTransportOptions.ENCRYPTION)
                .conditional(
                        EdgeTransportOptions.ENCRYPTION,
                        "aes_gcm",
                        EdgeTransportOptions.AES_SECRET_KEY_BASE64)
                .build();
    }

    @Override
    public PayloadSerializer create(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        EdgePacketCompressionType compression =
                EdgePacketCompressionType.from(config.get(EdgeTransportOptions.COMPRESSION));
        EdgePacketEncryptionType encryption =
                EdgePacketEncryptionType.from(config.get(EdgeTransportOptions.ENCRYPTION));
        byte[] aesKey = null;
        if (encryption == EdgePacketEncryptionType.AES_GCM) {
            String keyBase64 =
                    config.getOptional(EdgeTransportOptions.AES_SECRET_KEY_BASE64).orElse(null);
            if (keyBase64 != null) {
                aesKey = Base64.getDecoder().decode(keyBase64);
            }
        }
        return new PacketPayloadSerializer(compression, encryption, aesKey);
    }
}
