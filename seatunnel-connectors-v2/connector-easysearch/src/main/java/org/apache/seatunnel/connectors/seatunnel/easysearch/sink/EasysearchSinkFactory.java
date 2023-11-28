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

package org.apache.seatunnel.connectors.seatunnel.easysearch.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

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
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SinkConfig.KEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SinkConfig.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SinkConfig.MAX_RETRY_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SinkConfig.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.easysearch.config.SourceConfig.INDEX;

@AutoService(Factory.class)
public class EasysearchSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Easysearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOSTS, INDEX)
                .optional(
                        PRIMARY_KEYS,
                        KEY_DELIMITER,
                        USERNAME,
                        PASSWORD,
                        MAX_RETRY_COUNT,
                        MAX_BATCH_SIZE,
                        TLS_VERIFY_CERTIFICATE,
                        TLS_VERIFY_HOSTNAME,
                        TLS_KEY_STORE_PATH,
                        TLS_KEY_STORE_PASSWORD,
                        TLS_TRUST_STORE_PATH,
                        TLS_TRUST_STORE_PASSWORD)
                .build();
    }
}
