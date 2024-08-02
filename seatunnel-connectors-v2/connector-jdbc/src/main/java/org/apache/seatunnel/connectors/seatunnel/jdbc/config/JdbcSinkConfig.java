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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.ENABLE_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.IS_PRIMARY_KEY_UPDATED;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.SUPPORT_UPSERT_BY_INSERT_ONLY;

@Data
@Builder
public class JdbcSinkConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private boolean isExactlyOnce;
    private String simpleSql;
    private String database;
    private String table;
    private List<String> primaryKeys;
    private boolean enableUpsert;
    @Builder.Default private boolean isPrimaryKeyUpdated = true;
    private boolean supportUpsertByInsertOnly;
    private boolean useCopyStatement;
    @Builder.Default private boolean createIndex = true;

    public static JdbcSinkConfig of(ReadonlyConfig config) {
        JdbcSinkConfigBuilder builder = JdbcSinkConfig.builder();
        builder.jdbcConnectionConfig(JdbcConnectionConfig.of(config));
        builder.isExactlyOnce(config.get(JdbcOptions.IS_EXACTLY_ONCE));
        config.getOptional(JdbcOptions.PRIMARY_KEYS).ifPresent(builder::primaryKeys);
        config.getOptional(JdbcOptions.DATABASE).ifPresent(builder::database);
        config.getOptional(JdbcOptions.TABLE).ifPresent(builder::table);
        builder.enableUpsert(config.get(ENABLE_UPSERT));
        builder.isPrimaryKeyUpdated(config.get(IS_PRIMARY_KEY_UPDATED));
        builder.supportUpsertByInsertOnly(config.get(SUPPORT_UPSERT_BY_INSERT_ONLY));
        builder.simpleSql(config.get(JdbcOptions.QUERY));
        builder.useCopyStatement(config.get(JdbcOptions.USE_COPY_STATEMENT));
        builder.createIndex(config.get(JdbcCatalogOptions.CREATE_INDEX));
        return builder.build();
    }
}
