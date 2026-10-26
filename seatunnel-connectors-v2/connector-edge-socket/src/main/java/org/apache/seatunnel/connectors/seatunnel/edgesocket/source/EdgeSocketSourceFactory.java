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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketPacketMode;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Base64;

@AutoService(Factory.class)
public class EdgeSocketSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return EdgeSocketSourceOptions.identifier;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        EdgeSocketSourceOptions.PORT,
                        Conditions.greaterThan(EdgeSocketSourceOptions.PORT, 0))
                .required(
                        EdgeSocketSourceOptions.TOKEN,
                        Conditions.notBlank(EdgeSocketSourceOptions.TOKEN))
                .optional(
                        EdgeSocketSourceOptions.ENDPOINT,
                        Conditions.extension(
                                EdgeSocketSourceOptions.ENDPOINT, new EndpointValidator()))
                .optional(ConnectorCommonOptions.SCHEMA)
                .optional(
                        EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY,
                        Conditions.greaterThan(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY, 0))
                .optional(
                        EdgeSocketSourceOptions.QUEUE_BACKPRESSURE_WATERMARK_RATIO,
                        Conditions.greaterThan(
                                        EdgeSocketSourceOptions.QUEUE_BACKPRESSURE_WATERMARK_RATIO,
                                        0.0)
                                .and(
                                        Conditions.lessOrEqual(
                                                EdgeSocketSourceOptions
                                                        .QUEUE_BACKPRESSURE_WATERMARK_RATIO,
                                                1.0)))
                .optional(
                        EdgeSocketSourceOptions.QUEUE_FULL_RETRY_AFTER_MS,
                        Conditions.greaterThan(
                                EdgeSocketSourceOptions.QUEUE_FULL_RETRY_AFTER_MS, 0))
                .optional(
                        EdgeSocketSourceOptions.SECRET_KEY,
                        Conditions.extension(
                                EdgeSocketSourceOptions.SECRET_KEY, new SecretKeyValidator()))
                .optional(
                        EdgeSocketSourceOptions.PACKET_MODE,
                        EdgeSocketSourceOptions.AUTH_TYPE,
                        EdgeSocketSourceOptions.MAX_RETRIES,
                        EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS,
                        EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new EdgeSocketSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return EdgeSocketSource.class;
    }

    private static class EndpointValidator implements ConditionExtension<String> {

        @Override
        public String description() {
            return "must be blank or in host:port format";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String endpoint) {
            if (endpoint == null || endpoint.trim().isEmpty()) {
                return true;
            }
            int separatorIndex = endpoint.lastIndexOf(':');
            if (separatorIndex <= 0 || separatorIndex >= endpoint.length() - 1) {
                throw new OptionValidationException(
                        "Invalid endpoint: %s, expected format host:port", endpoint);
            }
            String endpointPort = endpoint.substring(separatorIndex + 1);
            try {
                Integer.parseInt(endpointPort);
            } catch (NumberFormatException parseException) {
                throw new OptionValidationException(
                        String.format("Invalid endpoint port in endpoint: %s", endpoint),
                        parseException);
            }
            return true;
        }
    }

    private static class SecretKeyValidator implements ConditionExtension<String> {

        @Override
        public String description() {
            return "must decode to exactly 32 bytes when packet_mode is PACKET";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String secretKey) {
            if (secretKey == null) {
                return true;
            }
            EdgeSocketPacketMode packetMode;
            try {
                packetMode = config.get(EdgeSocketSourceOptions.PACKET_MODE);
            } catch (IllegalArgumentException exception) {
                return true;
            }
            if (packetMode != EdgeSocketPacketMode.PACKET) {
                return true;
            }
            byte[] secretKeyBytes;
            try {
                secretKeyBytes = Base64.getDecoder().decode(secretKey);
            } catch (IllegalArgumentException exception) {
                throw new OptionValidationException("Invalid secret_key: not Base64 encoded");
            }
            if (secretKeyBytes.length != 32) {
                throw new OptionValidationException(
                        "Invalid secret_key: AES-256 requires exactly 32 bytes, "
                                + "but got %d bytes after Base64 decoding",
                        secretKeyBytes.length);
            }
            return true;
        }
    }
}
