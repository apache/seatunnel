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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.ASSET_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.CALENDAR_DATE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.END_DATE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.MAX_REQUESTS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.START_TIME;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerSourceOptions.USERNAME;

/**
 * Registers the AlpacaBroker source against the engine's Factory SPI.
 *
 * <p>Required and optional parameters are exposed through the same rule used by engine
 * configuration.
 */
@AutoService(Factory.class)
public final class AlpacaBrokerSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AlpacaBrokerSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RESOURCE, USERNAME, PASSWORD)
                .optional(
                        ENVIRONMENT,
                        BASE_URL,
                        TRUST_CERTIFICATE,
                        START_TIME,
                        END_TIME,
                        START_DATE,
                        END_DATE,
                        CALENDAR_DATE_TYPE,
                        ASSET_CLASS,
                        PAGE_SIZE,
                        MAX_REQUESTS,
                        REQUEST_TIMEOUT_MS,
                        MAX_ATTEMPTS,
                        MAX_RESPONSE_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        AlpacaBrokerSource source =
                new AlpacaBrokerSource(new AlpacaBrokerSettings(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AlpacaBrokerSource.class;
    }
}
