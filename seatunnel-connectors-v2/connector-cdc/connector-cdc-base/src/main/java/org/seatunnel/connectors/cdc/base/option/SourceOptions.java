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

package org.seatunnel.connectors.cdc.base.option;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

@SuppressWarnings("MagicNumber")
public class SourceOptions {

    public static final Option<Integer> SNAPSHOT_SPLIT_SIZE = Options.key("snapshot.split.size")
            .intType()
            .defaultValue(8096)
            .withDescription("The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.");

    public static final Option<Integer> SNAPSHOT_FETCH_SIZE = Options.key("snapshot.fetch.size")
            .intType()
            .defaultValue(1024)
            .withDescription("The maximum fetch size for per poll when read table snapshot.");

    public static final Option<StartupMode> STARTUP_MODE = Options.key("startup.mode")
            .enumType(StartupMode.class)
            .defaultValue(StartupMode.INITIAL)
            .withDescription("Optional startup mode for CDC source, valid enumerations are "
                    + "\"initial\", \"earliest\", \"latest\", \"timestamp\"\n or \"specific\"");

    public static final Option<StopMode> STOP_MODE = Options.key("stop.mode")
            .enumType(StopMode.class)
            .defaultValue(StopMode.NEVER)
            .withDescription("Optional stop mode for CDC source, valid enumerations are "
                    + "\"never\", \"latest\", \"timestamp\"\n or \"specific\"");

    public static final Option<Long> STARTUP_TIMESTAMP = Options.key("startup.timestamp")
            .longType()
            .noDefaultValue()
            .withDescription("Optional timestamp(mills) used in case of \"timestamp\" startup mode");

    public static final Option<Long> STOP_TIMESTAMP = Options.key("stop.timestamp")
            .longType()
            .noDefaultValue()
            .withDescription("Optional timestamp(mills) used in case of \"timestamp\" stop mode");
}
