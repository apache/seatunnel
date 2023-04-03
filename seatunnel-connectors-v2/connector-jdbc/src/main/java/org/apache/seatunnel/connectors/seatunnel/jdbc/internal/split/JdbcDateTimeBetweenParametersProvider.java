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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

import static com.google.common.base.Preconditions.checkArgument;

public class JdbcDateTimeBetweenParametersProvider extends JdbcNumericBetweenParametersProvider {
    private final java.util.Date minVal;
    private final java.util.Date maxVal;

    public JdbcDateTimeBetweenParametersProvider(java.util.Date minVal, java.util.Date maxVal) {
        super(toLong(minVal), toLong(maxVal));
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    public JdbcDateTimeBetweenParametersProvider(
            long fetchSize, java.util.Date minVal, java.util.Date maxVal) {
        super(fetchSize, toLong(minVal), toLong(maxVal));
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    private static long toLong(java.util.Date date) {
        checkArgument(
                (date instanceof Date) || (date instanceof Time) || (date instanceof Timestamp),
                "Unsupported arg type: " + date.getClass().getName());
        if (date instanceof Date) {
            return ((Date) date).toLocalDate().toEpochDay();
        }
        if (date instanceof Time) {
            return date.getTime() / 1000L;
        } else {
            return date.getTime();
        }
    }

    @Override
    public Serializable[][] getParameterValues() {
        Serializable[][] paramValues = super.getParameterValues();
        int len = paramValues.length;
        Serializable[][] dateRangeValues = new Serializable[len][2];
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < 2; j++) {
                long value = (long) paramValues[i][j];
                if (minVal instanceof Date) {
                    dateRangeValues[i][j] = Date.valueOf(LocalDate.ofEpochDay(value));
                } else if (minVal instanceof Time) {
                    dateRangeValues[i][j] = new Time(value * 1000L);
                } else {
                    dateRangeValues[i][j] = new Timestamp(value);
                }
            }
        }
        return dateRangeValues;
    }
}
