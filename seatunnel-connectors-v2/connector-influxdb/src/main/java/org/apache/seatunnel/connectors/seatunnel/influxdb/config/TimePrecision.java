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

package org.apache.seatunnel.connectors.seatunnel.influxdb.config;

import java.util.concurrent.TimeUnit;

public enum TimePrecision {
    NS("NS", TimeUnit.NANOSECONDS),
    U("U", TimeUnit.MICROSECONDS),
    MS("MS", TimeUnit.MILLISECONDS),
    S("S", TimeUnit.SECONDS),
    M("M", TimeUnit.MINUTES),
    H("H", TimeUnit.HOURS);
    private String desc;
    private TimeUnit precision;

    TimePrecision(String desc, TimeUnit precision) {
        this.desc = desc;
        this.precision = precision;
    }

    public TimeUnit getTimeUnit() {
        return this.precision;
    }

    public static TimePrecision getPrecision(String desc) {
        for (TimePrecision timePrecision : TimePrecision.values()) {
            if (desc.equals(timePrecision.desc)) {
                return timePrecision;
            }
        }
        return TimePrecision.NS;
    }
}
