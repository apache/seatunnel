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
import java.util.Arrays;

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
            Options.key("batch-size-per-sca ")
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
}
