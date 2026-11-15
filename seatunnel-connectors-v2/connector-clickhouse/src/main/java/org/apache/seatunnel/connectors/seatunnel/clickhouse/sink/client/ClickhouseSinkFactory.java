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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.CLICKHOUSE_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.BULK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SCHEMA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.TABLE;

@AutoService(Factory.class)
public class ClickhouseSinkFactory implements TableSinkFactory, SupportSinkDryRunValidation {
    private static final int DRY_RUN_TIMEOUT_MILLIS = 10_000;

    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new ClickhouseSink(catalogTable, readonlyConfig);
    }

    /** Checks the runtime's initial server using system metadata, without invoking save modes. */
    @Override
    public void validateConnectionForDryRun(TableSinkFactoryContext context) {
        if (Thread.currentThread().isInterrupted()) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.DRY_RUN_VALIDATION_FAILED,
                    "Clickhouse sink dry-run was interrupted before connecting.");
        }
        ReadonlyConfig config = context.getOptions();
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        // createNodes only installs credentials when both fields are nonempty. Do not report
        // validation of a named account when the runtime would fall back to the default account.
        if (username.isEmpty() || (!"default".equals(username) && password.isEmpty())) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.DRY_RUN_VALIDATION_FAILED,
                    "Clickhouse sink dry-run requires a nonempty username and a nonempty password"
                            + " for non-default users; the sink cannot resolve these credentials.");
        }
        Map<String, String> options = new HashMap<>(config.get(CLICKHOUSE_CONFIG));
        if (!options.getOrDefault(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), "").isEmpty()) {
            // HTTP parameters can supply an additional query, bypassing our metadata-only query.
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.DRY_RUN_VALIDATION_FAILED,
                    "Clickhouse sink dry-run does not support clickhouse.config.custom_http_params.");
        }
        if (ClickHouseUtils.getKeyValuePairs(
                        options.getOrDefault(ClickHouseHttpOption.CUSTOM_HEADERS.getKey(), ""))
                .keySet().stream()
                .anyMatch("X-ClickHouse-Query-Id"::equalsIgnoreCase)) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.DRY_RUN_VALIDATION_FAILED,
                    "Clickhouse sink dry-run does not support X-ClickHouse-Query-Id in custom_http_headers.");
        }
        boolean tableExists;
        try {
            capDryRunTimeout(
                    options, ClickHouseClientOption.CONNECTION_TIMEOUT, DRY_RUN_TIMEOUT_MILLIS);
            capDryRunTimeout(
                    options, ClickHouseClientOption.SOCKET_TIMEOUT, DRY_RUN_TIMEOUT_MILLIS);
            capDryRunTimeout(options, ClickHouseClientOption.MAX_EXECUTION_TIME, 10);
            options.put(ClickHouseClientOption.RETRY.getKey(), "0");
            options.put(ClickHouseClientOption.FAILOVER.getKey(), "0");
            options.put(ClickHouseClientOption.REPEAT_ON_SESSION_LOCK.getKey(), "false");
            options.put(ClickHouseClientOption.SESSION_ID.getKey(), "");
            options.put(ClickHouseClientOption.ASYNC.getKey(), "false");
            // Do not select the target database: a create-schema mode can create it later.
            options.put(ClickHouseClientOption.DATABASE.getKey(), "");
            ClickHouseNode node =
                    ClickhouseUtil.createNodes(
                                    config.get(HOST),
                                    "",
                                    config.get(SERVER_TIME_ZONE),
                                    username,
                                    password,
                                    options)
                            .get(0);
            try (ClickHouseClient client = ClickHouseClient.newInstance(node.getProtocol());
                    ClickHouseResponse response =
                            client.connect(node)
                                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                                    .query(
                                            "SELECT count() FROM system.tables"
                                                    + " WHERE database = :database AND name = :table")
                                    // Object[] binds literal values; the String[] overload accepts
                                    // SQL.
                                    .params(new Object[] {config.get(DATABASE), config.get(TABLE)})
                                    .executeAndWait()) {
                tableExists = response.firstRecord().getValue(0).asInteger() > 0;
            }
        } catch (Exception e) {
            // Driver messages and suppressed close failures may include credentials or HTTP
            // options.
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.DRY_RUN_VALIDATION_FAILED,
                    "Clickhouse sink dry-run failed. Check host, username, password, TLS settings,"
                            + " timeouts, and access to system.tables.");
        }
        if (!tableExists
                && config.get(SCHEMA_SAVE_MODE) == SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.SINK_TABLE_NOT_EXIST,
                    "Clickhouse sink table is missing or not visible in system.tables and"
                            + " schema_save_mode is ERROR_WHEN_SCHEMA_NOT_EXIST."
                            + " Check the database and table options.");
        }
    }

    private static void capDryRunTimeout(
            Map<String, String> options, ClickHouseClientOption option, int maximum) {
        int configured =
                Integer.parseInt(
                        options.getOrDefault(option.getKey(), option.getDefaultValue().toString()));
        options.put(
                option.getKey(),
                Integer.toString(configured > 0 ? Math.min(configured, maximum) : maximum));
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, DATABASE, TABLE, USERNAME, PASSWORD)
                .optional(
                        SERVER_TIME_ZONE,
                        CLICKHOUSE_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE,
                        SCHEMA_SAVE_MODE,
                        DATA_SAVE_MODE,
                        SAVE_MODE_CREATE_TEMPLATE,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(DATA_SAVE_MODE, DataSaveMode.CUSTOM_PROCESSING, CUSTOM_SQL)
                .build();
    }
}
