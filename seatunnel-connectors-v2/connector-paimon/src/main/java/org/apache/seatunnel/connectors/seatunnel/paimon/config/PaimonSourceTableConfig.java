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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;

import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class PaimonSourceTableConfig implements Serializable {

    private final String database;
    private final String table;
    private final String query;

    private PaimonSourceTableConfig(String database, String table, String query) {
        this.database = database;
        this.table = table;
        this.query = query;
    }

    public static PaimonSourceTableConfig parsePaimonSourceConfig(ReadonlyConfig config) {
        String database = config.get(PaimonBaseOptions.DATABASE);
        String table = config.get(PaimonBaseOptions.TABLE);
        String query = config.getOptional(PaimonSourceOptions.QUERY_SQL).orElse(null);
        return new PaimonSourceTableConfig(database, table, query);
    }

    public static List<PaimonSourceTableConfig> of(ReadonlyConfig config) {
        if (config.getOptional(PaimonSourceOptions.TABLE_LIST).isPresent()) {
            List<Map<String, Object>> maps = config.get(PaimonSourceOptions.TABLE_LIST);
            return maps.stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(PaimonSourceTableConfig::parsePaimonSourceConfig)
                    .collect(Collectors.toList());
        }
        return Lists.newArrayList(parsePaimonSourceConfig(config));
    }

    public TablePath getTablePath() {
        return TablePath.of(database, table);
    }
}
