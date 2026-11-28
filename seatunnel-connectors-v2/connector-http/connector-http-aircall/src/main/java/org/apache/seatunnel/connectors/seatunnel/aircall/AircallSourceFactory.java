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
package org.apache.seatunnel.connectors.seatunnel.aircall;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public final class AircallSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AircallSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AircallSourceOptions.USERNAME, AircallSourceOptions.PASSWORD)
                .optional(
                        AircallSourceOptions.BASE_URL,
                        AircallSourceOptions.RESOURCES,
                        AircallSourceOptions.INCLUDE_CONVERSATION_INTELLIGENCE,
                        AircallSourceOptions.START_TIME,
                        AircallSourceOptions.END_TIME,
                        AircallSourceOptions.LOOKBACK_DAYS,
                        AircallSourceOptions.CALL_WINDOW_DAYS,
                        AircallSourceOptions.CONTACT_WINDOW_DAYS,
                        AircallSourceOptions.CONVERSATION_WINDOW_DAYS,
                        AircallSourceOptions.PAGE_SIZE,
                        AircallSourceOptions.SMS_PAGE_SIZE,
                        AircallSourceOptions.MAX_PAGES,
                        AircallSourceOptions.REQUEST_INTERVAL_MS,
                        AircallSourceOptions.MAX_ATTEMPTS,
                        AircallSourceOptions.SOCKET_TIMEOUT_MS,
                        AircallSourceOptions.PAGE_DEADLINE_MS,
                        AircallSourceOptions.MAX_RESPONSE_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new AircallSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AircallSource.class;
    }
}
