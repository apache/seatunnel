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

/**
 * Reusable {@link ConditionExtension} guards for the MySQL CDC specific startup options.
 *
 * <p>Each guard is attached to exactly one {@code startup.specific-offset.*} option of the source
 * option rule, so a configuration that uses specific startup offsets is validated with the same
 * rules the runtime source applies: the offsets are only accepted when the {@code startup.mode} is
 * (or defaults to) {@code specific}, the binlog file and position must be configured together, and
 * the value domains (non-blank / non-negative) match the runtime checks. A valid anchor with
 * optional GTID / skip metadata, and a configuration without any specific offset, remain accepted.
 *
 * <p>Runtime behavior is unchanged; the guards only surface the same failures before the job is
 * started. The errors reuse the wording of the runtime error messages so that operators see the
 * same message either way.
 */
public final class MySqlSpecificOffsetGuards {

    private MySqlSpecificOffsetGuards() {}

    public static ConditionExtension<String> specificOffsetFileGuard(
            Option<StartupMode> startupModeOption,
            Option<String> fileOption,
            Option<Long> posOption) {
        return new SpecificOffsetFileGuard(startupModeOption, fileOption.key(), posOption);
    }

    public static ConditionExtension<Long> specificOffsetPosGuard(
            Option<StartupMode> startupModeOption,
            Option<Long> posOption,
            Option<String> fileOption) {
        return new SpecificOffsetPosGuard(startupModeOption, posOption.key(), fileOption);
    }

    public static ConditionExtension<String> specificOffsetGtidSetGuard(
            Option<StartupMode> startupModeOption, Option<String> gtidSetOption) {
        return new SpecificOffsetGtidSetGuard(startupModeOption, gtidSetOption.key());
    }

    public static ConditionExtension<Long> specificOffsetSkipGuard(
            Option<StartupMode> startupModeOption, Option<Long> skipOption) {
        return new SpecificOffsetSkipGuard(startupModeOption, skipOption.key());
    }

    protected abstract static class ModeSpecificGuard<T> implements ConditionExtension<T> {
        protected final Option<StartupMode> startupModeOption;
        protected final String guardedKey;

        protected ModeSpecificGuard(Option<StartupMode> startupModeOption, String guardedKey) {
            this.startupModeOption = startupModeOption;
            this.guardedKey = guardedKey;
        }

        protected void checkStartupMode(ReadonlyConfig config) {
            StartupMode mode =
                    config.getOptional(startupModeOption)
                            .orElseGet(startupModeOption::defaultValue);
            if (mode != StartupMode.SPECIFIC) {
                throw new OptionValidationException(
                        String.format(
                                "'startup.specific-offset.*' options can only be used when '%s' is"
                                        + " 'specific', but current mode is '%s'.",
                                startupModeOption.key(), mode.name()));
            }
        }

        protected void checkPairedOption(
                ReadonlyConfig config, Option<?> otherOption, String otherKey) {
            if (!config.getOptional(otherOption).isPresent()) {
                throw new OptionValidationException(
                        String.format(
                                "'%s' and '%s' must be configured together when '%s' is"
                                        + " 'specific'.",
                                guardedKey, otherKey, startupModeOption.key()));
            }
        }
    }

    static final class SpecificOffsetFileGuard extends ModeSpecificGuard<String> {
        private final Option<Long> posOption;

        SpecificOffsetFileGuard(
                Option<StartupMode> startupModeOption, String guardedKey, Option<Long> posOption) {
            super(startupModeOption, guardedKey);
            this.posOption = posOption;
        }

        @Override
        public String description() {
            return guardedKey
                    + "' can only be used when '"
                    + startupModeOption.key()
                    + "' is 'specific', must not be blank and must be paired with '"
                    + posOption.key()
                    + "'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String value) {
            checkStartupMode(config);
            if (value.trim().isEmpty()) {
                throw new OptionValidationException(
                        String.format("'%s' must not be blank.", guardedKey));
            }
            checkPairedOption(config, posOption, posOption.key());
            return true;
        }
    }

    static final class SpecificOffsetPosGuard extends ModeSpecificGuard<Long> {
        private final Option<String> fileOption;

        SpecificOffsetPosGuard(
                Option<StartupMode> startupModeOption,
                String guardedKey,
                Option<String> fileOption) {
            super(startupModeOption, guardedKey);
            this.fileOption = fileOption;
        }

        @Override
        public String description() {
            return guardedKey
                    + "' can only be used when '"
                    + startupModeOption.key()
                    + "' is 'specific' and must be paired with '"
                    + fileOption.key()
                    + "'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Long value) {
            checkStartupMode(config);
            checkPairedOption(config, fileOption, fileOption.key());
            return true;
        }
    }

    static final class SpecificOffsetGtidSetGuard extends ModeSpecificGuard<String> {
        SpecificOffsetGtidSetGuard(Option<StartupMode> startupModeOption, String guardedKey) {
            super(startupModeOption, guardedKey);
        }

        @Override
        public String description() {
            return guardedKey
                    + "' can only be used when '"
                    + startupModeOption.key()
                    + "' is 'specific' and must not be blank";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String value) {
            checkStartupMode(config);
            if (value.trim().isEmpty()) {
                throw new OptionValidationException(
                        String.format("'%s' must not be blank.", guardedKey));
            }
            return true;
        }
    }

    static final class SpecificOffsetSkipGuard extends ModeSpecificGuard<Long> {
        SpecificOffsetSkipGuard(Option<StartupMode> startupModeOption, String guardedKey) {
            super(startupModeOption, guardedKey);
        }

        @Override
        public String description() {
            return guardedKey
                    + "' can only be used when '"
                    + startupModeOption.key()
                    + "' is 'specific' and must be greater than or equal to 0";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Long value) {
            checkStartupMode(config);
            if (value < 0L) {
                throw new OptionValidationException(
                        String.format("'%s' must be greater than or equal to 0.", guardedKey));
            }
            return true;
        }
    }
}
