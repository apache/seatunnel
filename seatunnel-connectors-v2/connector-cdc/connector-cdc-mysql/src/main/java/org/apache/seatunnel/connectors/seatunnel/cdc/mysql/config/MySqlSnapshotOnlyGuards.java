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
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

/**
 * Reusable {@link ConditionExtension} guards for the MySQL CDC {@code snapshot-only} startup mode.
 *
 * <p>A {@code snapshot-only} job reads the snapshot, may perform a bounded backfill and then
 * finishes; it never enters continuous binlog streaming. Options that only make sense for binlog
 * streaming are therefore rejected rather than silently ignored. The guards are meant to be wired
 * as conditional value constraints that only apply when {@code startup.mode} is {@code
 * snapshot-only}, so they do not check the startup mode themselves.
 *
 * <p>The {@code startup.specific-offset.*} options are not covered here: {@link
 * MySqlSpecificOffsetGuards} already rejects them for every startup mode other than {@code
 * specific}.
 */
public final class MySqlSnapshotOnlyGuards {

    private MySqlSnapshotOnlyGuards() {}

    public static ConditionExtension<StopMode> stopModeGuard(Option<StopMode> stopModeOption) {
        return new StopModeGuard(stopModeOption.key());
    }

    public static ConditionExtension<Long> startupTimestampGuard(Option<Long> timestampOption) {
        return new StartupTimestampGuard(timestampOption.key());
    }

    /** Only {@code stop.mode = never} is allowed, because the job never streams the binlog. */
    static final class StopModeGuard implements ConditionExtension<StopMode> {
        private final String guardedKey;

        StopModeGuard(String guardedKey) {
            this.guardedKey = guardedKey;
        }

        @Override
        public String description() {
            return "must be 'never' when the startup mode is '" + StartupMode.SNAPSHOT_ONLY + "'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, StopMode value) {
            if (value != null && value != StopMode.NEVER) {
                throw new OptionValidationException(
                        String.format(
                                "'%s' startup mode is a bounded snapshot-only job and does not"
                                        + " enter continuous change log streaming, so it is"
                                        + " incompatible with '%s = %s'. Remove the stop mode"
                                        + " option.",
                                StartupMode.SNAPSHOT_ONLY, guardedKey, value.name().toLowerCase()));
            }
            return true;
        }
    }

    /** The snapshot phase derives the backfill offsets, so a startup timestamp is not used. */
    static final class StartupTimestampGuard implements ConditionExtension<Long> {
        private final String guardedKey;

        StartupTimestampGuard(String guardedKey) {
            this.guardedKey = guardedKey;
        }

        @Override
        public String description() {
            return "can not be used when the startup mode is '" + StartupMode.SNAPSHOT_ONLY + "'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Long value) {
            if (value == null) {
                return true;
            }
            throw new OptionValidationException(
                    String.format(
                            "'%s' startup mode derives bounded backfill offsets from the snapshot"
                                    + " phase and does not use startup change log offsets, so"
                                    + " startup offset option '%s' is not allowed.",
                            StartupMode.SNAPSHOT_ONLY, guardedKey));
        }
    }
}
