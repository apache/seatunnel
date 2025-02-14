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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class HttpSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Http";
    }

    @Override
    public OptionRule optionRule() {
        return getHttpBuilder().build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new HttpSource(context.getOptions().toConfig());
    }

    public OptionRule.Builder getHttpBuilder() {
        return OptionRule.builder()
                .required(HttpConfig.URL)
                .optional(HttpConfig.METHOD)
                .optional(HttpConfig.HEADERS)
                .optional(HttpConfig.PARAMS)
                .optional(HttpConfig.FORMAT)
                .optional(HttpConfig.BODY)
                .optional(HttpConfig.PAGEING)
                .optional(HttpConfig.JSON_FIELD)
                .optional(HttpConfig.CONTENT_FIELD)
                .conditional(
                        HttpConfig.FORMAT,
                        HttpConfig.ResponseFormat.JSON,
                        ConnectorCommonOptions.SCHEMA)
                .optional(HttpConfig.POLL_INTERVAL_MILLS)
                .optional(HttpConfig.RETRY)
                .optional(HttpConfig.RETRY_BACKOFF_MULTIPLIER_MS)
                .optional(HttpConfig.RETRY_BACKOFF_MAX_MS);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HttpSource.class;
    }
}
