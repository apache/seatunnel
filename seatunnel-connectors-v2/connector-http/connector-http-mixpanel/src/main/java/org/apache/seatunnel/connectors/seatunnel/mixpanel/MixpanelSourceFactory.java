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

package org.apache.seatunnel.connectors.seatunnel.mixpanel;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.FIELD_MAPPING;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.FROM_DATE;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.MAX_EVENT_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.MAX_RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.PROJECT_ID;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.TO_DATE;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.USERNAME;

@AutoService(Factory.class)
public class MixpanelSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Mixpanel";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        USERNAME,
                        PASSWORD,
                        PROJECT_ID,
                        FROM_DATE,
                        TO_DATE,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        REGION,
                        BASE_URL,
                        ALLOW_INSECURE_HTTP,
                        CONNECT_TIMEOUT_MS,
                        READ_TIMEOUT_MS,
                        MAX_RETRIES,
                        RETRY_BACKOFF_MS,
                        MAX_RETRY_BACKOFF_MS,
                        MAX_EVENT_BYTES,
                        FIELD_MAPPING)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new MixpanelSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MixpanelSource.class;
    }
}
