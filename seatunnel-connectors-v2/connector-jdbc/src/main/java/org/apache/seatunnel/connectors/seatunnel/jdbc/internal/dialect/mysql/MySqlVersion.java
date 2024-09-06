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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

public enum MySqlVersion {
    V_5_5,
    V_5_6,
    V_5_7,
    V_8,
    V_8_4;

    public static MySqlVersion parse(String version) {
        if (version != null) {
            if (version.startsWith("5.5")) {
                return V_5_5;
            }
            if (version.startsWith("5.6")) {
                return V_5_6;
            }
            if (version.startsWith("5.7")) {
                return V_5_7;
            }
            if (version.startsWith("8.0")) {
                return V_8;
            }
            if (version.startsWith("8.4")) {
                return V_8_4;
            }
        }
        throw new UnsupportedOperationException("Unsupported MySQL version: " + version);
    }

    public boolean isBefore(MySqlVersion version) {
        return this.compareTo(version) < 0;
    }

    public boolean isAtOrBefore(MySqlVersion version) {
        return this.compareTo(version) <= 0;
    }

    public boolean isAfter(MySqlVersion version) {
        return this.compareTo(version) > 0;
    }

    public boolean isAtOrAfter(MySqlVersion version) {
        return this.compareTo(version) >= 0;
    }
}
