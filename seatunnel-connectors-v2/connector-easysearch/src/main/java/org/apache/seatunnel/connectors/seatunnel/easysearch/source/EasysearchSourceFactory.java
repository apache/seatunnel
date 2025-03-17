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

package org.apache.seatunnel.connectors.seatunnel.easysearch.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.HOSTS;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.TLS_KEY_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.TLS_KEY_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.TLS_TRUST_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.TLS_TRUST_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.TLS_VERIFY_HOSTNAME;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.EzsClusterConnectionConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig.INDEX;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig.SCROLL_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig.SCROLL_TIME;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig.SOURCE;

@AutoService(Factory.class)
public class EasysearchSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Easysearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOSTS, INDEX)
                .optional(
                        USERNAME,
                        PASSWORD,
                        SCROLL_TIME,
                        SCROLL_SIZE,
                        QUERY,
                        TLS_VERIFY_CERTIFICATE,
                        TLS_VERIFY_HOSTNAME,
                        TLS_KEY_STORE_PATH,
                        TLS_KEY_STORE_PASSWORD,
                        TLS_TRUST_STORE_PATH,
                        TLS_TRUST_STORE_PASSWORD)
                .exclusive(SOURCE, ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return EasysearchSource.class;
    }
}
