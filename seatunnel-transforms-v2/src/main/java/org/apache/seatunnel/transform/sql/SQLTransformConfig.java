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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

import static org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType.ZETA;

@Getter
@Setter
public class SQLTransformConfig {

    public static final Option<String> KEY_QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("The query SQL");

    public static final Option<String> KEY_ENGINE =
            Options.key("engine")
                    .stringType()
                    .defaultValue(ZETA.name())
                    .withDescription("The SQL engine type");

    public static final Option<List<TableTransforms>> MULTI_TABLES =
            Options.key("table_transform")
                    .listType(TableTransforms.class)
                    .noDefaultValue()
                    .withDescription("");

    @Data
    public static class TableTransforms implements Serializable {
        @JsonAlias("table_path")
        private String tablePath;

        @JsonAlias("query")
        private String query;

        @JsonAlias("engine")
        private String engine;
    }

    private String query;
    private SQLEngineFactory.EngineType engineType;

    public static SQLTransformConfig of(ReadonlyConfig config) {
        SQLTransformConfig sqlTransformConfig = new SQLTransformConfig();
        sqlTransformConfig.setQuery(config.get(KEY_QUERY));
        sqlTransformConfig.setEngineType(
                SQLEngineFactory.EngineType.valueOf(config.get(KEY_ENGINE)));
        return sqlTransformConfig;
    }

    public static SQLTransformConfig of(ReadonlyConfig config, CatalogTable catalogTable) {
        String tablePath = catalogTable.getTableId().toTablePath().getFullName();
        if (null != config.get(MULTI_TABLES)) {
            return config.get(MULTI_TABLES).stream()
                    .filter(tableTransforms -> tableTransforms.getTablePath().equals(tablePath))
                    .findFirst()
                    .map(
                            tableTransforms -> {
                                SQLTransformConfig sqlTransformConfig = new SQLTransformConfig();
                                sqlTransformConfig.setQuery(tableTransforms.getQuery());
                                sqlTransformConfig.setEngineType(
                                        tableTransforms.getEngine() != null
                                                ? SQLEngineFactory.EngineType.valueOf(
                                                        tableTransforms.getEngine())
                                                : ZETA);
                                return sqlTransformConfig;
                            })
                    .orElseGet(() -> of(config));
        }
        return of(config);
    }
}
