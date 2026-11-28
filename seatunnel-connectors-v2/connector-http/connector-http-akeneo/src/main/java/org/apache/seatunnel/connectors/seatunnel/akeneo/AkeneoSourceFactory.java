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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

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
public final class AkeneoSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return AkeneoSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AkeneoSourceOptions.BASE_URL,
                        AkeneoSourceOptions.CLIENT_ID,
                        AkeneoSourceOptions.CLIENT_SECRET,
                        AkeneoSourceOptions.USERNAME,
                        AkeneoSourceOptions.PASSWORD)
                .optional(
                        AkeneoSourceOptions.RESOURCES,
                        AkeneoSourceOptions.MEASUREMENT_API,
                        AkeneoSourceOptions.PAGE_SIZE,
                        AkeneoSourceOptions.MAX_PAGES,
                        AkeneoSourceOptions.REQUEST_INTERVAL_MS,
                        AkeneoSourceOptions.MAX_ATTEMPTS,
                        AkeneoSourceOptions.SOCKET_TIMEOUT_MS,
                        AkeneoSourceOptions.PAGE_DEADLINE_MS,
                        AkeneoSourceOptions.MAX_RESPONSE_BYTES)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new AkeneoSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AkeneoSource.class;
    }
}
