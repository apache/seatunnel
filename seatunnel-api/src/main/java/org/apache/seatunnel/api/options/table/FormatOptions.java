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

package org.apache.seatunnel.api.options.table;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;

public interface FormatOptions {
    Option<DateUtils.Formatter> DATE_FORMAT_LEGACY =
            Options.key("date_format")
                    .enumType(DateUtils.Formatter.class)
                    .defaultValue(DateUtils.Formatter.YYYY_MM_DD)
                    .withDescription("Date format");

    Option<DateTimeUtils.Formatter> DATETIME_FORMAT_LEGACY =
            Options.key("datetime_format")
                    .enumType(DateTimeUtils.Formatter.class)
                    .defaultValue(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)
                    .withDescription("Datetime format");

    Option<TimeUtils.Formatter> TIME_FORMAT_LEGACY =
            Options.key("time_format")
                    .enumType(TimeUtils.Formatter.class)
                    .defaultValue(TimeUtils.Formatter.HH_MM_SS)
                    .withDescription("Time format");

    // Not used yet. Reserved for future use to support custom date/time format strings.
    Option<String> DATE_FORMAT =
            Options.key("date_format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd")
                    .withDescription(
                            "Date format string (e.g. 'yyyy-MM-dd'). "
                                    + "Must match one of the predefined values in the Formatter enum.");

    Option<String> DATETIME_FORMAT =
            Options.key("datetime_format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd HH:mm:ss")
                    .withDescription(
                            "Datetime format string (e.g. 'yyyy-MM-dd HH:mm:ss'). "
                                    + "Must match one of the predefined values in the Formatter enum.");

    // Not used yet. Reserved for future use to support custom date/time format strings.
    Option<String> TIME_FORMAT =
            Options.key("time_format")
                    .stringType()
                    .defaultValue("HH:mm:ss")
                    .withDescription(
                            "Time format string (e.g. 'HH:mm:ss'). "
                                    + "Must match one of the predefined values in the Formatter enum.");
}
