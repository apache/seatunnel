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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;

import org.tikv.common.ConfigUtils;
import org.tikv.common.TiConfiguration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/** TiDB source options */
public class TiDBSourceOptions implements Serializable {

    public static final Option<String> DATABASE_NAME =
            Options.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the TiDB server to monitor.");

    public static final Option<String> TABLE_NAME =
            Options.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the database to monitor.");

    public static final Option<List<String>> TABLE_NAMES =
            Options.key("table-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names to capture changes from, each entry must be in the"
                                    + " format database_name.table_name, for example:"
                                    + " [\"db1.table1\", \"db2.table2\"]. Mutually exclusive with"
                                    + " database-name/table-name.");

    public static final Option<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class,
                            Arrays.asList(
                                    StartupMode.INITIAL, StartupMode.EARLIEST, StartupMode.LATEST))
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "Optional startup mode for CDC source, valid enumerations are "
                                    + "\"initial\", \"earliest\", \"latest\"");

    public static final Option<String> PD_ADDRESSES =
            Options.key("pd-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TiKV cluster's PD address");

    public static final Option<Integer> BATCH_SIZE_PER_SCAN =
            Options.key("batch-size-per-scan")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Size per scan");

    public static final Option<Long> TIKV_GRPC_TIMEOUT =
            Options.key(ConfigUtils.TIKV_GRPC_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC timeout in ms");

    public static final Option<Long> TIKV_GRPC_SCAN_TIMEOUT =
            Options.key(ConfigUtils.TIKV_GRPC_SCAN_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC scan timeout in ms");

    public static final Option<Integer> TIKV_BATCH_GET_CONCURRENCY =
            Options.key(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch get concurrency");

    public static final Option<Integer> TIKV_BATCH_SCAN_CONCURRENCY =
            Options.key(ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch scan concurrency");

    public static TiConfiguration getTiConfiguration(final ReadonlyConfig configuration) {
        final String pdAddrsStr = configuration.get(PD_ADDRESSES);
        final TiConfiguration tiConf = TiConfiguration.createDefault(pdAddrsStr);
        configuration.getOptional(TIKV_GRPC_TIMEOUT).ifPresent(tiConf::setTimeout);
        configuration.getOptional(TIKV_GRPC_SCAN_TIMEOUT).ifPresent(tiConf::setScanTimeout);
        configuration
                .getOptional(TIKV_BATCH_GET_CONCURRENCY)
                .ifPresent(tiConf::setBatchGetConcurrency);

        configuration
                .getOptional(TIKV_BATCH_SCAN_CONCURRENCY)
                .ifPresent(tiConf::setBatchScanConcurrency);
        return tiConf;
    }

    /**
     * Resolves the captured table list in the unified {@code database_name.table_name} format.
     * Prefers the multi-table option {@code table-names}; falls back to the legacy single-table
     * options {@code database-name}/{@code table-name} for backward compatibility.
     *
     * @param configuration the readonly config of the TiDB-CDC source
     * @return de-duplicated list of table full names, never null or empty
     */
    public static List<String> getTableFullNames(final ReadonlyConfig configuration) {
        List<String> tableNames = configuration.getOptional(TABLE_NAMES).orElse(null);
        if (tableNames != null && !tableNames.isEmpty()) {
            List<String> fullNames = new ArrayList<>(new LinkedHashSet<>(tableNames));
            fullNames.forEach(TiDBSourceOptions::validateTableFullName);
            return fullNames;
        }
        String databaseName = configuration.get(DATABASE_NAME);
        String tableName = configuration.get(TABLE_NAME);
        if (databaseName == null || tableName == null) {
            throw new IllegalArgumentException(
                    "TiDB-CDC source must configure either 'table-names' (e.g."
                            + " [\"database_name.table_name\"]) or both 'database-name' and"
                            + " 'table-name'.");
        }
        return Collections.singletonList(databaseName + "." + tableName);
    }

    /**
     * Parses the database part of a {@code database_name.table_name} full name. The first dot is
     * the separator, so table names containing dots are supported.
     *
     * @param tableFullName table full name in {@code database_name.table_name} format
     * @return the database name
     */
    public static String parseDatabaseName(final String tableFullName) {
        validateTableFullName(tableFullName);
        return tableFullName.substring(0, tableFullName.indexOf('.'));
    }

    /**
     * Parses the table part of a {@code database_name.table_name} full name.
     *
     * @param tableFullName table full name in {@code database_name.table_name} format
     * @return the table name
     */
    public static String parseTableName(final String tableFullName) {
        validateTableFullName(tableFullName);
        return tableFullName.substring(tableFullName.indexOf('.') + 1);
    }

    /**
     * Builds a {@code database_name.table_name} full name, the inverse of {@link
     * #parseDatabaseName} and {@link #parseTableName}.
     *
     * @param databaseName database name
     * @param tableName table name
     * @return table full name in {@code database_name.table_name} format
     */
    public static String tableFullName(final String databaseName, final String tableName) {
        return databaseName + "." + tableName;
    }

    private static void validateTableFullName(final String tableFullName) {
        if (tableFullName == null) {
            throw new IllegalArgumentException(
                    "Table name must be in database_name.table_name format, but got: null");
        }
        int separatorIndex = tableFullName.indexOf('.');
        if (separatorIndex <= 0 || separatorIndex == tableFullName.length() - 1) {
            throw new IllegalArgumentException(
                    "Table name must be in database_name.table_name format, but got: "
                            + tableFullName);
        }
    }
}
