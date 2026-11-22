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

package org.apache.seatunnel.connectors.seatunnel.sonarqube;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;

@AutoService(Factory.class)
public class SonarQubeSourceFactory implements TableSourceFactory {
    static final String IDENTIFIER = "SonarQube";
    static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SonarQube Community Build base URL including its context path, if any.");
    static final Option<String> PROJECT_KEY =
            Options.key("project_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Key of the project whose current main-branch measures are read.");
    static final Option<List<String>> METRIC_KEYS =
            Options.key("metric_keys")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Between 1 and 50 distinct SonarQube metric keys.");
    static final Option<String> TOKEN_ENV =
            Options.key("token_env")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional worker environment variable with a SonarQube user token (HTTPS only).");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SonarQubeSource.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new SonarQubeSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(BASE_URL, PROJECT_KEY, METRIC_KEYS)
                .optional(TOKEN_ENV)
                .build();
    }
}
