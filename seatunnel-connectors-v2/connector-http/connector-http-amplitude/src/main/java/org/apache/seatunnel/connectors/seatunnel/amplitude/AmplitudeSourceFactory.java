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
package org.apache.seatunnel.connectors.seatunnel.amplitude;

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

import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.API_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.FIELD_MAPPING;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_ARCHIVE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_EVENT_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.MAX_UNCOMPRESSED_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.SECRET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amplitude.AmplitudeSourceOptions.START;

@AutoService(Factory.class)
public class AmplitudeSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Amplitude";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(API_KEY, SECRET_KEY, START, END, ConnectorCommonOptions.SCHEMA)
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
                        MAX_ARCHIVE_BYTES,
                        MAX_UNCOMPRESSED_BYTES,
                        FIELD_MAPPING)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new AmplitudeSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AmplitudeSource.class;
    }
}
