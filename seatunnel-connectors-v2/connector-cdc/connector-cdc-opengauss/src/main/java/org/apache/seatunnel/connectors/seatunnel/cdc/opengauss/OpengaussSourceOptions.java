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
 * PostgreSQL's startup-mode surface wholesale. {@code committed-offset} is excluded: it resolves
 * its start position through {@code LsnOffsetFactory#committedOffset()}, which reads {@code
 * confirmed_flush_lsn} and {@code active_pid} from {@code pg_replication_slots}, columns OpenGauss
 * is not verified to expose. {@code snapshot-only} is kept: it is served entirely by the
 * dialect-agnostic incremental framework ({@code SnapshotOnlySplitAssigner}, gated only on {@code
 * StartupMode} in {@code IncrementalSource}), touches nothing PostgreSQL-specific, and was already
 * accepted by this connector through the shared PostgreSQL option. Owning the option here means a
 * later PostgreSQL-only addition cannot silently widen this connector again.
 *
 * <p>{@code stop.mode} is deliberately still taken from the PostgreSQL options: it has a single
 * legal value ({@code never}) with no dialect-specific behavior behind it.
 */
public class OpengaussSourceOptions {

    /**
     * Startup modes OpenGauss CDC accepts: everything PostgreSQL CDC accepts except {@code
     * committed-offset}. No cast is needed here because the single-choice builder already returns a
     * {@link SingleChoiceOption} of the chosen enum type.
     */
    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class,
                            Arrays.asList(
                                    StartupMode.INITIAL,
                                    StartupMode.SNAPSHOT_ONLY,
                                    StartupMode.EARLIEST,
                                    StartupMode.LATEST))
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "Optional startup mode for Opengauss CDC source, valid enumerations are "
                                    + "\"initial\", \"snapshot-only\", \"earliest\", \"latest\"");
}
