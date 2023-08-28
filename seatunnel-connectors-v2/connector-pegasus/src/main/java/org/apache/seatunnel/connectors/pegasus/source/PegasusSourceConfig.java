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

package org.apache.seatunnel.connectors.pegasus.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.apache.pegasus.client.FilterType;

public class PegasusSourceConfig {

    public static final Option<String> META_SERVER =
            Options.key("meta_server")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The meta server hosts in format 'ip:port,ip:port,...'.");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table to be read.");

    public static final Option<ReadMode> READ_MODE =
            Options.key("read_mode")
                    .enumType(ReadMode.class)
                    .defaultValue(ReadMode.unorderedScanner)
                    .withDescription("The read operation type, including: unorderedScanner.");

    enum ReadMode {
        unorderedScanner
    }

    public static final Option<Integer> SCAN_OPTION_TIMEOUT_MILLIS =
            Options.key("scan_option.timeoutMillis")
                    .intType()
                    .noDefaultValue()
                    .withDescription("scan options 'timeoutMillis' for scanner, unorderedScanner.");

    public static final Option<Integer> SCAN_OPTION_BATCH_SIZE =
            Options.key("scan_option.batchSize")
                    .intType()
                    .noDefaultValue()
                    .withDescription("scan options 'batchSize' for scanner, unorderedScanner.");

    public static final Option<Boolean> SCAN_OPTION_START_INCLUDE =
            Options.key("scan_option.startInclusive")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "scan options 'startInclusive' for scanner, unorderedScanner.");

    public static final Option<Boolean> SCAN_OPTION_STOP_INCLUDE =
            Options.key("scan_option.stopInclusive")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("scan options 'stopInclusive' for scanner, unorderedScanner.");

    public static final Option<FilterType> SCAN_OPTION_HASH_KEY_FILTER_TYPE =
            Options.key("scan_option.hashKeyFilterType")
                    .enumType(FilterType.class)
                    .noDefaultValue()
                    .withDescription(
                            "scan options 'hashKeyFilterType' for scanner, unorderedScanner.");

    public static final Option<String> SCAN_OPTION_HASH_KEY_FILTER_PATTERN =
            Options.key("scan_option.hashKeyFilterPattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "scan options 'hashKeyFilterPattern' for scanner, unorderedScanner.");

    public static final Option<FilterType> SCAN_OPTION_SORT_KEY_FILTER_TYPE =
            Options.key("scan_option.sortKeyFilterType")
                    .enumType(FilterType.class)
                    .noDefaultValue()
                    .withDescription(
                            "scan options 'sortKeyFilterType' for scanner, unorderedScanner.");

    public static final Option<String> SCAN_OPTION_SORT_KEY_FILTER_PATTERN =
            Options.key("scan_option.sortKeyFilterPattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "scan options 'sortKeyFilterPattern' for scanner, unorderedScanner.");

    public static final Option<Boolean> SCAN_OPTION_NO_VALUE =
            Options.key("scan_option.noValue")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("scan options 'noValue' for scanner, unorderedScanner.");
}
