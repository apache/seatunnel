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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/** PostgreSQL snapshot configuration used alongside the GaussDB-specific WAL reader. */
public final class GaussDBSourceConfigFactory extends PostgresSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public JdbcSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        Map<String, Object> debeziumConfig = new LinkedHashMap<>(config.getSourceMap());
        config.getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                .ifPresent(
                        properties -> {
                            Map<String, String> filteredProperties =
                                    new LinkedHashMap<>(properties);
                            filteredProperties.remove("plugin.name");
                            filteredProperties.remove("slot.name");
                            debeziumConfig.put(
                                    SourceOptions.DEBEZIUM_PROPERTIES.key(), filteredProperties);
                        });
        // Debezium initializes snapshot metadata but never consumes the mppdb stream. Its closed
        // decoder enum still requires a known PostgreSQL plugin name. Slot ownership remains with
        // the GaussDB options so both the snapshot offset and mppdb stream reference the same slot.
        debeziumConfig.put(PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME.key(), "pgoutput");
        super.fromReadonlyConfig(ReadonlyConfig.fromMap(debeziumConfig));
        return this;
    }
}
