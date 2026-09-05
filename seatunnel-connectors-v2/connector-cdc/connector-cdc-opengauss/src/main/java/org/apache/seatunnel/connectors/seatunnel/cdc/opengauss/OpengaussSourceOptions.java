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

package org.apache.seatunnel.connectors.seatunnel.cdc.opengauss;

import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;

import java.util.Arrays;

/**
 * Startup-mode contract owned by the OpenGauss CDC connector.
 *
 * <p>OpenGauss reuses the PostgreSQL runtime through the PG base, but it must not inherit
 * PostgreSQL's startup-mode surface: {@code snapshot-only} and {@code committed-offset} are backed
 * by PostgreSQL-specific behavior (the latter reads {@code confirmed_flush_lsn} and {@code
 * active_pid} from {@code pg_replication_slots}), which OpenGauss is not verified to serve. Owning
 * the option here keeps OpenGauss pinned to the three modes it has always accepted, so a later
 * addition on the PostgreSQL side cannot silently widen this connector again.
 *
 * <p>{@code stop.mode} is deliberately still taken from the PostgreSQL options: it has a single
 * legal value ({@code never}) with no dialect-specific behavior behind it.
 */
public class OpengaussSourceOptions {

    /** Startup modes OpenGauss CDC accepts; intentionally narrower than PostgreSQL CDC. */
    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            (SingleChoiceOption)
                    Options.key(SourceOptions.STARTUP_MODE_KEY)
                            .singleChoice(
                                    StartupMode.class,
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.EARLIEST,
                                            StartupMode.LATEST))
                            .defaultValue(StartupMode.INITIAL)
                            .withDescription(
                                    "Optional startup mode for Opengauss CDC source, valid enumerations are "
                                            + "\"initial\", \"earliest\", \"latest\"");
}
