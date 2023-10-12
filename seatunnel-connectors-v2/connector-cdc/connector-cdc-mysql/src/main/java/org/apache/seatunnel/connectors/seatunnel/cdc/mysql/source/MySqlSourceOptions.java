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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.Arrays;

public class MySqlSourceOptions {
    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            (SingleChoiceOption)
                    Options.key(SourceOptions.STARTUP_MODE_KEY)
                            .singleChoice(
                                    StartupMode.class,
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.EARLIEST,
                                            StartupMode.LATEST,
                                            StartupMode.SPECIFIC))
                            .defaultValue(StartupMode.INITIAL)
                            .withDescription(
                                    "Optional startup mode for CDC source, valid enumerations are "
                                            + "\"initial\", \"earliest\", \"latest\" or \"specific\"");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            (SingleChoiceOption)
                    Options.key(SourceOptions.STOP_MODE_KEY)
                            .singleChoice(
                                    StopMode.class,
                                    Arrays.asList(
                                            StopMode.LATEST, StopMode.SPECIFIC, StopMode.NEVER))
                            .defaultValue(StopMode.NEVER)
                            .withDescription(
                                    "Optional stop mode for CDC source, valid enumerations are "
                                            + "\"never\", \"latest\" or \"specific\"");
}
