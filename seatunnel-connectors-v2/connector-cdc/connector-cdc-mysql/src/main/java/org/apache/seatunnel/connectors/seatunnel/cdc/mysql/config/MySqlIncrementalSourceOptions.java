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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.options.table.CatalogOptions;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.Arrays;

public class MySqlIncrementalSourceOptions extends JdbcSourceOptions implements CatalogOptions {

    public static final Option<Boolean> INT_TYPE_NARROWING =
            Options.key("int_type_narrowing")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now.");

    public static final Option<String> STARTUP_SPECIFIC_OFFSET_GTID_SET =
            Options.key("startup.specific-offset.gtid-set")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional GTID set used with file and position in case of \"specific\" startup mode.");

    public static final Option<Long> STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS =
            Options.key("startup.specific-offset.skip-events")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional number of binlog events to skip after the specific startup offset.");

    public static final Option<Long> STARTUP_SPECIFIC_OFFSET_SKIP_ROWS =
            Options.key("startup.specific-offset.skip-rows")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional number of rows to skip after the specific startup offset.");

    /**
     * Controls whether MySQL CDC should snapshot tables that newly match the table pattern after a
     * checkpoint or savepoint restore.
     */
    public static final Option<Boolean> SCAN_NEWLY_ADDED_TABLE_ENABLED =
            Options.key("scan.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to scan newly added tables when the job is restored from checkpoint or savepoint.");

    /**
     * Controls whether MySQL CDC should register newly created table schemas while consuming
     * binlog. This mode only starts from the CREATE TABLE binlog position and never backfills
     * historical rows.
     */
    public static final Option<Boolean> SCAN_BINLOG_NEWLY_ADDED_TABLE_ENABLED =
            Options.key("scan.binlog.newly-added-table.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read schema and data records for newly added tables during the binlog reading phase. This option does not snapshot historical data for those tables.");

    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class,
                            Arrays.asList(
                                    StartupMode.INITIAL,
                                    StartupMode.EARLIEST,
                                    StartupMode.LATEST,
                                    StartupMode.SPECIFIC,
                                    StartupMode.TIMESTAMP))
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "Optional startup mode for CDC source, valid enumerations are "
                                    + "\"initial\", \"earliest\", \"latest\" , \"specific\" or \"timestamp\"");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .singleChoice(
                            StopMode.class,
                            Arrays.asList(StopMode.LATEST, StopMode.SPECIFIC, StopMode.NEVER))
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "Optional stop mode for CDC source, valid enumerations are "
                                    + "\"never\", \"latest\" or \"specific\"");
}
