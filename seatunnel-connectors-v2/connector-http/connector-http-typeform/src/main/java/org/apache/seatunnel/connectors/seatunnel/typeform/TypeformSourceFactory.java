/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.typeform;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.typeform.config.TypeformSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

/**
 * Factory that registers the Typeform source under the identifier "Typeform" and declares its
 * options.
 */
@AutoService(Factory.class)
public class TypeformSourceFactory extends HttpSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Typeform";
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new TypeformSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(TypeformSourceOptions.API_KEY, TypeformSourceOptions.FORM_ID)
                .optional(
                        TypeformSourceOptions.BASE_URL,
                        TypeformSourceOptions.PAGE_SIZE,
                        TypeformSourceOptions.SINCE,
                        TypeformSourceOptions.UNTIL,
                        TypeformSourceOptions.RESPONSE_TYPE,
                        TypeformSourceOptions.RETRY,
                        TypeformSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        TypeformSourceOptions.RETRY_BACKOFF_MAX_MS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return TypeformSource.class;
    }
}
