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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.util;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.DateTimeUtils;

import java.time.LocalDateTime;

public class FormatterContext {
    private final DateTimeUtils.Formatter localDateTimeFormat;

    public FormatterContext(String localDateTimeFormat) {
        this.localDateTimeFormat = DateTimeUtils.Formatter.parse(localDateTimeFormat);
    }

    public boolean isDateTimeType(Object field) {
        return field instanceof LocalDateTime;
    }

    public String formatDateTime(Object field) {
        if (field instanceof LocalDateTime) {
            return this.format(((LocalDateTime) field));
        }
        throw CommonError.illegalArgument(
                field.getClass().getName(),
                "Cannot format the given value: not a LocalDateTime instance.");
    }

    private String format(LocalDateTime localDateTime) {
        return DateTimeUtils.toString(localDateTime, localDateTimeFormat);
    }
}
