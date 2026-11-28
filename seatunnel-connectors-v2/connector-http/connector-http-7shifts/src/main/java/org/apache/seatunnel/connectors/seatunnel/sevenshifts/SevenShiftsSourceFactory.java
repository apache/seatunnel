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

package org.apache.seatunnel.connectors.seatunnel.sevenshifts;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.API_VERSION;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.sevenshifts.SevenShiftsSourceOptions.TOKEN;

/** Exposes the 7shifts source and its option contract through the factory SPI. */
@AutoService(Factory.class)
public final class SevenShiftsSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return SevenShiftsSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(TOKEN, RESOURCE)
                .optional(
                        BASE_URL,
                        ALLOW_INSECURE_HTTP,
                        API_VERSION,
                        START_DATE,
                        PAGE_SIZE,
                        REQUEST_TIMEOUT_MS,
                        MAX_ATTEMPTS)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        SevenShiftsSource source =
                new SevenShiftsSource(new SevenShiftsConfig(context.getOptions()));
        return () -> (SeaTunnelSource<T, SplitT, StateT>) source;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SevenShiftsSource.class;
    }
}
