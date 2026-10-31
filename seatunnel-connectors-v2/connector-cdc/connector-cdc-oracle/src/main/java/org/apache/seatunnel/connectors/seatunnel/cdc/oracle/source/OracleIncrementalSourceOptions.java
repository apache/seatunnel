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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.Arrays;
import java.util.List;

public class OracleIncrementalSourceOptions extends JdbcSourceOptions {
    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            (SingleChoiceOption)
                    Options.key(SourceOptions.STARTUP_MODE_KEY)
                            .singleChoice(
                                    StartupMode.class,
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.LATEST,
                                            StartupMode.SPECIFIC,
                                            StartupMode.TIMESTAMP))
                            .defaultValue(StartupMode.INITIAL)
                            .withDescription(
                                    "Optional startup mode for CDC source, valid enumerations are "
                                            + "\"initial\", \"latest\", \"specific\" or \"timestamp\"");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            (SingleChoiceOption)
                    Options.key(SourceOptions.STOP_MODE_KEY)
                            .singleChoice(StopMode.class, Arrays.asList(StopMode.NEVER))
                            .defaultValue(StopMode.NEVER)
                            .withDescription(
                                    "Optional stop mode for CDC source, valid enumerations are "
                                            + "\"never\"");

    public static final Option<List<String>> SCHEMA_NAMES =
            Options.key("schema-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Schema name of the database to monitor.");

    public static final Option<Long> STARTUP_SPECIFIC_OFFSET_SCN =
            Options.key("startup.specific-offset.scn")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional SCN used in case of \"specific\" startup mode.");

    public static final Option<Boolean> USE_SELECT_COUNT =
            Options.key("use_select_count")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Use select count for table count in full stage");

    public static final Option<Boolean> SKIP_ANALYZE =
            Options.key("skip_analyze")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip the analysis of table count in full stage");

    public static final Option<Boolean> LOB_ENABLED =
            Options.key("lob.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Pass through to Debezium lob.enabled. true mines CLOB, NCLOB, BLOB, and"
                                    + " XMLTYPE redo. When omitted, debezium.lob.enabled is left unchanged."
                                    + " An explicit value overrides debezium.lob.enabled.");

    public static final Option<Boolean> LOB_RESELECT_ENABLED =
            Options.key("lob.reselect.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "With lob.enabled true and this option false, a LOB column an UPDATE did not"
                                    + " change, and LOB columns on DELETE and UPDATE_BEFORE, are the"
                                    + " unavailable-value placeholder. A sink that writes the whole row"
                                    + " replaces the stored value with that placeholder. true re-selects those"
                                    + " CLOB, NCLOB, BLOB, and XMLTYPE columns on INSERT and UPDATE_AFTER by"
                                    + " primary key, binding commit_scn as AS OF SCN ?, and DELETE and"
                                    + " UPDATE_BEFORE placeholders become null. Rejected unless lob.enabled"
                                    + " is true. A re-select failure follows lob.unavailable-value.handling.");

    public static final Option<OracleLobUnavailableValueHandling> LOB_UNAVAILABLE_VALUE_HANDLING =
            Options.key("lob.unavailable-value.handling")
                    .enumType(OracleLobUnavailableValueHandling.class)
                    .defaultValue(OracleLobUnavailableValueHandling.WARN_AND_KEEP)
                    .withDescription(
                            "The placeholder remains when re-select is off, the table has no primary key,"
                                    + " or the lookup fails, and a full-row sink write would store it."
                                    + " warn_and_keep logs once and keeps it. null replaces it with null so"
                                    + " the sink does not store the placeholder. fail stops the task. null"
                                    + " and fail are rejected unless lob.enabled is true.");
}
